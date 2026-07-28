
import logging
import os
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ChatJoinRequest,
)
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------- Настройки ----------
# Все секреты и настройки берутся из файла .env (см. .env.example) и НИКОГДА не хранятся в коде.
load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# ADMIN_IDS в .env через запятую: ADMIN_IDS=1872389147,7206938314
ADMIN_IDS = {
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()
}

# Числовой id канала (см. инструкцию в начале файла). НЕ ссылка, НЕ юзернейм.
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")

CHANNEL_INVITE_LINK = os.environ.get("CHANNEL_INVITE_LINK", "")

SOCIAL_LINKS = {
    "TikTok": os.environ.get("TIKTOK_LINK", ""),
    "Instagram": os.environ.get("INSTAGRAM_LINK", ""),
}
# Пустые ссылки в соц.сетях не показываем кнопкой
SOCIAL_LINKS = {name: url for name, url in SOCIAL_LINKS.items() if url}

if not BOT_TOKEN:
    raise SystemExit(
        "BOT_TOKEN не найден. Создай файл .env рядом со скриптом (см. .env.example) "
        "и укажи в нём BOT_TOKEN=..."
    )
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS пуст — никто не будет получать уведомления. Проверь .env")

MEMBER_STATUSES = {
    ChatMemberStatus.MEMBER,
    ChatMemberStatus.ADMINISTRATOR,
    ChatMemberStatus.OWNER,
}

# ---------- База данных (PostgreSQL — постоянное хранилище, переживает деплои) ----------
# На Render (и любом другом хостинге с эфемерной файловой системой) SQLite-файл стирается
# при каждом деплое/рестарте. Поэтому база — внешняя Postgres (бесплатно и без ограничения
# по времени, например на neon.tech), строка подключения берётся из .env / переменных
# окружения хостинга как DATABASE_URL.
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise SystemExit(
        "DATABASE_URL не найден. Создай бесплатную Postgres-базу (например, на https://neon.tech), "
        "скопируй строку подключения и укажи её в .env / переменных окружения как DATABASE_URL=... "
        "Без этого данные будут стираться при каждом деплое."
    )

_pg_conn = psycopg2.connect(DATABASE_URL, sslmode="require")
_pg_conn.autocommit = True


class _ConnWrapper:
    """Даёт интерфейс, похожий на sqlite3.Connection (execute/executescript/commit),
    поверх psycopg2, чтобы не переписывать все запросы в коде бота."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, query: str, params=()):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query.replace("?", "%s"), params)
        return cur

    def executescript(self, script: str) -> None:
        cur = self._conn.cursor()
        cur.execute(script)
        cur.close()

    def commit(self) -> None:
        self._conn.commit()  # no-op при autocommit=True, оставлено для совместимости вызовов


_conn = _ConnWrapper(_pg_conn)


def _init_db() -> None:
    _conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            name TEXT,
            username TEXT,
            first_seen TEXT,
            last_seen TEXT
        );
        CREATE TABLE IF NOT EXISTS blocked (
            user_id BIGINT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS requests (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            chat_title TEXT,
            user_id BIGINT,
            user_name TEXT,
            username TEXT,
            date TEXT,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            user_name TEXT,
            username TEXT,
            date TEXT,
            text TEXT
        );
        CREATE TABLE IF NOT EXISTS memberships (
            user_id BIGINT PRIMARY KEY,
            status TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS message_replies (
            admin_message_id BIGINT PRIMARY KEY,
            user_id BIGINT,
            created_at TEXT
        );
        """
    )


_init_db()

# Привязка message_id пересланного админу сообщения -> user_id отправителя,
# хранится в БД (переживает рестарт бота на Render), используется, чтобы Reply от админа
# доставлялся нужному пользователю.
def _remember_reply_target(admin_message_id: int, user_id: int) -> None:
    _conn.execute(
        """
        INSERT INTO message_replies (admin_message_id, user_id, created_at) VALUES (?, ?, ?)
        ON CONFLICT (admin_message_id) DO UPDATE SET user_id=excluded.user_id
        """,
        (admin_message_id, user_id, datetime.now().isoformat(timespec="seconds")),
    )
    _conn.commit()


def _get_reply_target(admin_message_id: int) -> int | None:
    row = _conn.execute(
        "SELECT user_id FROM message_replies WHERE admin_message_id=?", (admin_message_id,)
    ).fetchone()
    return row["user_id"] if row else None


# user_id -> True, пока пользователь в режиме "пишет сообщение владельцу"
# (выставляется нажатием кнопки "💬 Написать владельцу", иначе случайные сообщения
# в чат с ботом не пересылаются админам). Только в памяти — это просто UX-состояние,
# при рестарте бота пользователь просто нажмёт кнопку ещё раз, ничего страшного.
awaiting_contact: dict[int, bool] = {}

# ---------- Меню ----------
ADMIN_MENU = ReplyKeyboardMarkup(
    [
        ["📋 Все заявки", "🔄 Обновить заявки"],
        ["💬 Все сообщения", "📊 Статистика"],
        ["✅ Принять все заявки", "🚫 Блок / разблок"],
        ["📈 Статус подписок"],
    ],
    resize_keyboard=True,
)

USER_MENU = ReplyKeyboardMarkup(
    [["💬 Написать владельцу"], ["🔗 Другие соц. сети"]],
    resize_keyboard=True,
)

CONTACT_MODE_MENU = ReplyKeyboardMarkup(
    [["🔚 Закончить диалог"]],
    resize_keyboard=True,
)


def _fmt_user(user) -> str:
    name = user.full_name
    username = f"@{user.username}" if user.username else "без username"
    return f"{name} ({username}, id={user.id})"


def _is_admin(chat_id: int) -> bool:
    return chat_id in ADMIN_IDS


def _remember_user(user) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    _conn.execute(
        """
        INSERT INTO users (user_id, name, username, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            name=excluded.name, username=excluded.username, last_seen=excluded.last_seen
        """,
        (user.id, user.full_name, user.username, now, now),
    )
    _conn.commit()


def _is_blocked(user_id: int) -> bool:
    row = _conn.execute("SELECT 1 FROM blocked WHERE user_id=?", (user_id,)).fetchone()
    return row is not None


def _block_user(user_id: int) -> None:
    _conn.execute("INSERT INTO blocked (user_id) VALUES (?) ON CONFLICT (user_id) DO NOTHING", (user_id,))
    _conn.commit()


def _unblock_user(user_id: int) -> None:
    _conn.execute("DELETE FROM blocked WHERE user_id=?", (user_id,))
    _conn.commit()


def _set_membership(user_id: int, status: str) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    _conn.execute(
        """
        INSERT INTO memberships (user_id, status, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at
        """,
        (user_id, status, now),
    )
    _conn.commit()


def _get_cached_membership(user_id: int) -> str | None:
    row = _conn.execute("SELECT status FROM memberships WHERE user_id=?", (user_id,)).fetchone()
    return row["status"] if row else None


async def _notify_admins(context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    sent = {}
    for admin_id in ADMIN_IDS:
        try:
            msg = await context.bot.send_message(chat_id=admin_id, text=text, reply_markup=reply_markup)
            sent[admin_id] = msg.message_id
        except TelegramError:
            logger.exception("Не удалось отправить сообщение админу %s", admin_id)
    return sent


def _channel_configured() -> bool:
    return bool(CHANNEL_ID) and "ВСТАВЬ" not in str(CHANNEL_ID)


async def _check_membership_live(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """Проверяет подписку прямо сейчас через Telegram API и обновляет кэш в БД.
    Если API недоступен — откатывается на последнее известное значение из БД."""
    if not _channel_configured():
        logger.warning("CHANNEL_ID не настроен — не могу проверить подписку")
        return False
    try:
        member = await context.bot.get_chat_member(chat_id=int(CHANNEL_ID), user_id=user_id)
        _set_membership(user_id, member.status)
        return member.status in MEMBER_STATUSES
    except TelegramError as e:
        logger.warning("get_chat_member не сработал для %s: %s — использую кэш", user_id, e)
        cached = _get_cached_membership(user_id)
        return cached in {s.value if hasattr(s, "value") else s for s in MEMBER_STATUSES} if cached else False


# ---------- Заявки на вступление ----------
async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Заявка автоматически принимается, админам приходит уведомление о том, кто подписался."""
    req: ChatJoinRequest = update.chat_join_request
    chat = req.chat
    user = req.from_user

    row = _conn.execute(
        """INSERT INTO requests (chat_id, chat_title, user_id, user_name, username, date, status)
           VALUES (?, ?, ?, ?, ?, ?, 'pending') RETURNING id""",
        (chat.id, chat.title, user.id, user.full_name, user.username, req.date.strftime("%Y-%m-%d %H:%M")),
    ).fetchone()
    req_row_id = row["id"]
    _conn.commit()

    try:
        await context.bot.approve_chat_join_request(chat_id=chat.id, user_id=user.id)
        _conn.execute("UPDATE requests SET status='approved' WHERE id=?", (req_row_id,))
        _conn.commit()
        text = (
            f"✅ Новая заявка автоматически ПРИНЯТА в канал «{chat.title}»\n\n"
            f"Пользователь: {_fmt_user(user)}\n"
            f"Время: {req.date.strftime('%Y-%m-%d %H:%M')}"
        )
    except Exception as e:
        logger.exception("Не удалось автоматически одобрить заявку")
        text = (
            f"⚠️ Не удалось автоматически принять заявку в канал «{chat.title}»\n\n"
            f"Пользователь: {_fmt_user(user)}\n"
            f"Ошибка: {e}"
        )

    await _notify_admins(context, text)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action, chat_id_str, user_id_str = query.data.split(":")
    chat_id, user_id = int(chat_id_str), int(user_id_str)

    row = _conn.execute(
        "SELECT id FROM requests WHERE chat_id=? AND user_id=? AND status='pending'",
        (chat_id, user_id),
    ).fetchone()
    if row is None:
        await query.edit_message_text(query.message.text + "\n\n⚠️ Заявка уже обработана ранее.")
        return

    try:
        if action == "approve":
            await context.bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            new_status = "approved"
            result_line = "✅ Заявка ПРИНЯТА"
        else:
            await context.bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
            new_status = "declined"
            result_line = "❌ Заявка ОТКЛОНЕНА"
        _conn.execute("UPDATE requests SET status=? WHERE id=?", (new_status, row["id"]))
        _conn.commit()
    except Exception as e:
        logger.exception("Ошибка при обработке заявки")
        result_line = f"⚠️ Ошибка: {e}"

    await query.edit_message_text(
        query.message.text + f"\n\n{result_line} ({datetime.now().strftime('%H:%M:%S')})"
    )


async def cmd_approve_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return

    pending = _conn.execute("SELECT * FROM requests WHERE status='pending'").fetchall()
    if not pending:
        await update.message.reply_text("Нет ожидающих заявок.", reply_markup=ADMIN_MENU)
        return

    approved, failed = 0, 0
    for r in pending:
        try:
            await context.bot.approve_chat_join_request(chat_id=r["chat_id"], user_id=r["user_id"])
            _conn.execute("UPDATE requests SET status='approved' WHERE id=?", (r["id"],))
            approved += 1
        except Exception:
            logger.exception("Не удалось одобрить заявку %s", dict(r))
            failed += 1
    _conn.commit()

    text = f"✅ Одобрено: {approved} из {len(pending)}."
    if failed:
        text += f"\n⚠️ Не удалось одобрить: {failed}"
    await update.message.reply_text(text, reply_markup=ADMIN_MENU)


async def cmd_sync_requests(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Проверяет все pending-заявки через API и подтягивает те, что одобрили
    вручную мимо бота (напрямую в Telegram). Отклонённые вручную так не находятся —
    Telegram не даёт это отличить от "ещё не решена", используй /decline_id."""
    if not _is_admin(update.effective_chat.id):
        return
    if not _channel_configured():
        await update.message.reply_text("⚠️ CHANNEL_ID не настроен в коде бота.", reply_markup=ADMIN_MENU)
        return

    pending = _conn.execute("SELECT * FROM requests WHERE status='pending'").fetchall()
    if not pending:
        await update.message.reply_text("Нет заявок в статусе «ожидает» — обновлять нечего.", reply_markup=ADMIN_MENU)
        return

    checked, synced_approved = 0, 0
    for r in pending:
        try:
            member = await context.bot.get_chat_member(chat_id=r["chat_id"], user_id=r["user_id"])
            checked += 1
            _set_membership(r["user_id"], member.status)
            if member.status in MEMBER_STATUSES:
                _conn.execute("UPDATE requests SET status='approved' WHERE id=?", (r["id"],))
                synced_approved += 1
        except TelegramError:
            logger.warning("Не удалось проверить статус для заявки id=%s", r["id"])
    _conn.commit()

    text = (
        f"🔄 Проверено заявок: {checked} из {len(pending)}\n"
        f"Досинхронизировано как «принята» (одобрено вручную вне бота): {synced_approved}\n\n"
        "Если ты отклонял(а) заявку вручную в Telegram — она всё ещё будет висеть "
        "как «ожидает», т.к. это API не показывает. Помечай такие командой:\n"
        "/decline_id <id заявки> (id заявки виден в списке «Все заявки»)"
    )
    await update.message.reply_text(text, reply_markup=ADMIN_MENU)


async def cmd_decline_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Вручную пометить заявку отклонённой (если решил её мимо бота): /decline_id <id>"""
    if not _is_admin(update.effective_chat.id):
        return
    if not context.args:
        await update.message.reply_text("Использование: /decline_id <id заявки>\nid смотри в списке «Все заявки».")
        return
    try:
        req_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом.")
        return
    row = _conn.execute("SELECT * FROM requests WHERE id=?", (req_id,)).fetchone()
    if row is None:
        await update.message.reply_text(f"Заявка с id={req_id} не найдена.")
        return
    _conn.execute("UPDATE requests SET status='declined' WHERE id=?", (req_id,))
    _conn.commit()
    await update.message.reply_text(
        f"❌ Заявка id={req_id} ({row['user_name']}) помечена как отклонённая.", reply_markup=ADMIN_MENU
    )


async def cmd_view_requests(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return
    rows = _conn.execute("SELECT * FROM requests ORDER BY id DESC LIMIT 30").fetchall()
    total = _conn.execute("SELECT COUNT(*) c FROM requests").fetchone()["c"]
    if not rows:
        await update.message.reply_text("Заявок пока не было.", reply_markup=ADMIN_MENU)
        return

    status_emoji = {"pending": "🕒", "approved": "✅", "declined": "❌"}
    lines = [f"📋 Заявки (последние {len(rows)} из {total}):\n"]
    for r in rows:
        member_status = _get_cached_membership(r["user_id"])
        sub_mark = "✅ в канале" if member_status in ("member", "administrator", "creator") else (
            "❌ не в канале" if member_status else "❓ неизвестно"
        )
        lines.append(
            f"#{r['id']} {status_emoji.get(r['status'], '?')} {r['user_name']} "
            f"(@{r['username'] or '—'}) — {r['date']} · {sub_mark}"
        )
    text = "\n".join(lines)
    for i in range(0, len(text), 3500):
        await update.message.reply_text(text[i:i + 3500])
    await update.message.reply_text("⬆️ Список выше", reply_markup=ADMIN_MENU)


async def cmd_view_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return
    rows = _conn.execute("SELECT * FROM messages ORDER BY id DESC LIMIT 30").fetchall()
    total = _conn.execute("SELECT COUNT(*) c FROM messages").fetchone()["c"]
    if not rows:
        await update.message.reply_text("Сообщений пока не было.", reply_markup=ADMIN_MENU)
        return

    lines = [f"💬 Сообщения (последние {len(rows)} из {total}):\n"]
    for m in rows:
        preview = (m["text"][:80] + "…") if len(m["text"]) > 80 else m["text"]
        lines.append(
            f"— {m['user_name']} (@{m['username'] or '—'}, id={m['user_id']}) [{m['date']}]:\n  {preview}"
        )
    text = "\n".join(lines)
    for i in range(0, len(text), 3500):
        await update.message.reply_text(text[i:i + 3500])
    await update.message.reply_text("⬆️ Список выше", reply_markup=ADMIN_MENU)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return

    now = datetime.now()
    today_prefix = now.strftime("%Y-%m-%d")
    week_ago = (now - timedelta(days=7)).isoformat(timespec="seconds")
    month_ago = (now - timedelta(days=30)).isoformat(timespec="seconds")

    users_total = _conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    users_today = _conn.execute(
        "SELECT COUNT(*) c FROM users WHERE first_seen LIKE ?", (today_prefix + "%",)
    ).fetchone()["c"]
    users_week = _conn.execute(
        "SELECT COUNT(*) c FROM users WHERE first_seen >= ?", (week_ago,)
    ).fetchone()["c"]
    users_month = _conn.execute(
        "SELECT COUNT(*) c FROM users WHERE first_seen >= ?", (month_ago,)
    ).fetchone()["c"]
    active_senders = _conn.execute("SELECT COUNT(DISTINCT user_id) c FROM messages").fetchone()["c"]
    subscribed_known = _conn.execute(
        "SELECT COUNT(*) c FROM memberships WHERE status IN ('member','administrator','creator')"
    ).fetchone()["c"]
    blocked_count = _conn.execute("SELECT COUNT(*) c FROM blocked").fetchone()["c"]

    requests_total = _conn.execute("SELECT COUNT(*) c FROM requests").fetchone()["c"]
    approved = _conn.execute("SELECT COUNT(*) c FROM requests WHERE status='approved'").fetchone()["c"]
    declined = _conn.execute("SELECT COUNT(*) c FROM requests WHERE status='declined'").fetchone()["c"]
    pending = _conn.execute("SELECT COUNT(*) c FROM requests WHERE status='pending'").fetchone()["c"]
    messages_count = _conn.execute("SELECT COUNT(*) c FROM messages").fetchone()["c"]

    channel_line = ""
    if _channel_configured():
        try:
            real_count = await context.bot.get_chat_member_count(chat_id=int(CHANNEL_ID))
            channel_line = f"\nУчастников в канале (по данным Telegram): {real_count}"
        except TelegramError:
            channel_line = "\n⚠️ Не удалось получить число участников канала"

    text = (
        "📊 Статистика бота\n\n"
        "👥 Пользователи бота\n"
        f"Всего писали/запускали бота: {users_total}\n"
        f"Новых сегодня: {users_today}\n"
        f"Новых за 7 дней: {users_week}\n"
        f"Новых за 30 дней: {users_month}\n"
        f"Писали в форму связи: {active_senders}\n"
        f"Подписаны на канал (известно боту): {subscribed_known}\n"
        f"Заблокировано: {blocked_count}"
        f"{channel_line}\n\n"
        "📥 Заявки на канал\n"
        f"Всего: {requests_total}\n"
        f"Принято: {approved}\n"
        f"Отклонено: {declined}\n"
        f"Ожидает: {pending}\n\n"
        f"💬 Сообщений через форму связи: {messages_count}"
    )
    await update.message.reply_text(text, reply_markup=ADMIN_MENU)


async def cmd_subscription_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает, сколько известных пользователей бота подписаны на канал (по кэшу в БД)."""
    if not _is_admin(update.effective_chat.id):
        return
    subscribed = _conn.execute(
        "SELECT COUNT(*) c FROM memberships WHERE status IN ('member','administrator','creator')"
    ).fetchone()["c"]
    not_subscribed = _conn.execute(
        "SELECT COUNT(*) c FROM memberships WHERE status NOT IN ('member','administrator','creator')"
    ).fetchone()["c"]
    users_count = _conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    known = subscribed + not_subscribed
    text = (
        "📈 Статус подписок (по данным, известным боту)\n\n"
        f"Подписаны на канал: {subscribed}\n"
        f"Не подписаны / вышли: {not_subscribed}\n"
        f"Ещё неизвестно: {max(users_count - known, 0)}\n\n"
        "Данные обновляются автоматически, когда кто-то подписывается/отписывается, "
        "а также при нажатии пользователем кнопки «Другие соц. сети»."
    )
    if not _channel_configured():
        text += "\n\n⚠️ CHANNEL_ID ещё не настроен в коде бота."
    await update.message.reply_text(text, reply_markup=ADMIN_MENU)


# ---------- Отслеживание подписки в реальном времени ----------
async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cmu = update.chat_member
    if cmu is None:
        return
    if not _channel_configured() or cmu.chat.id != int(CHANNEL_ID):
        return
    user_id = cmu.new_chat_member.user.id
    status = cmu.new_chat_member.status
    _set_membership(user_id, status)

    # Если человек стал участником канала (даже если заявку одобрили вручную,
    # мимо кнопок бота) — синхронизируем висящую заявку.
    if status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        _conn.execute(
            "UPDATE requests SET status='approved' WHERE chat_id=? AND user_id=? AND status='pending'",
            (cmu.chat.id, user_id),
        )
        _conn.commit()


# ---------- Форма связи ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = update.effective_chat.id
    _remember_user(user)

    if _is_admin(chat_id):
        await update.message.reply_text(f"Привет, админ! Твой chat_id: {chat_id}", reply_markup=ADMIN_MENU)
        return

    await update.message.reply_text(
        "Привет! 👋\n\n"
        f'Наш канал: <a href="{CHANNEL_INVITE_LINK}">перейти в канал</a>\n\n'
        "Если хочешь связаться со мной — нажми «💬 Написать владельцу».\n"
        "Если ты уже подписан(а) на канал — нажми «🔗 Другие соц. сети», покажу дополнительные ссылки.",
        reply_markup=USER_MENU,
        parse_mode=ParseMode.HTML,
    )


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await update.message.reply_text("Меню:", reply_markup=ADMIN_MENU if _is_admin(chat_id) else USER_MENU)


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return
    target_id = None
    if update.message.reply_to_message:
        target_id = _get_reply_target(update.message.reply_to_message.message_id)
    elif context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            pass
    if target_id is None:
        await update.message.reply_text(
            "Укажи id: /block 123456789\nили Reply на пересланное сообщение пользователя с командой /block"
        )
        return
    _block_user(target_id)
    await update.message.reply_text(f"🚫 Пользователь {target_id} заблокирован.", reply_markup=ADMIN_MENU)


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return
    target_id = None
    if update.message.reply_to_message:
        target_id = _get_reply_target(update.message.reply_to_message.message_id)
    elif context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            pass
    if target_id is None:
        await update.message.reply_text("Укажи id: /unblock 123456789")
        return
    _unblock_user(target_id)
    await update.message.reply_text(f"✅ Пользователь {target_id} разблокирован.", reply_markup=ADMIN_MENU)


async def cmd_block_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_chat.id):
        return
    blocked_count = _conn.execute("SELECT COUNT(*) c FROM blocked").fetchone()["c"]
    await update.message.reply_text(
        "Чтобы заблокировать: Reply на пересланное сообщение пользователя + команда /block\n"
        "Или вручную: /block <user_id>\n\n"
        "Разблокировать: /unblock <user_id>\n\n"
        f"Сейчас заблокировано: {blocked_count}",
        reply_markup=ADMIN_MENU,
    )


async def on_forwarded_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Служебная функция: перешли боту сообщение ИЗ канала — покажет числовой id канала."""
    if update.message is None or not _is_admin(update.effective_chat.id):
        return
    fwd_chat = update.message.forward_from_chat
    if fwd_chat:
        await update.message.reply_text(
            f"id канала «{fwd_chat.title}»: {fwd_chat.id}\nСкопируй это число в CHANNEL_ID в коде бота."
        )


async def on_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    message = update.message
    if message is None:
        return
    text = message.text or ""

    # ---- Админ-чаты ----
    if _is_admin(chat_id):
        if text == "📋 Все заявки":
            return await cmd_view_requests(update, context)
        if text == "🔄 Обновить заявки":
            return await cmd_sync_requests(update, context)
        if text == "💬 Все сообщения":
            return await cmd_view_messages(update, context)
        if text == "📊 Статистика":
            return await cmd_stats(update, context)
        if text == "✅ Принять все заявки":
            return await cmd_approve_all(update, context)
        if text == "🚫 Блок / разблок":
            return await cmd_block_help(update, context)
        if text == "📈 Статус подписок":
            return await cmd_subscription_status(update, context)

        if message.reply_to_message:
            target_user_id = _get_reply_target(message.reply_to_message.message_id)
            if target_user_id:
                try:
                    await context.bot.send_message(chat_id=target_user_id, text=f"✉️ Ответ:\n\n{text}")
                    await message.reply_text("Отправлено.")
                except Exception as e:
                    await message.reply_text(f"⚠️ Не удалось отправить: {e}")
            return
        return

    # ---- Обычные пользователи ----
    user = update.effective_user
    _remember_user(user)

    if _is_blocked(user.id):
        await message.reply_text("Извини, ты заблокирован(а) и не можешь отправлять сообщения.")
        return

    if text == "💬 Написать владельцу":
        awaiting_contact[user.id] = True
        await message.reply_text(
            "Напиши своё сообщение — я его передам 👇\n"
            "Когда закончишь — нажми «🔚 Закончить диалог».",
            reply_markup=CONTACT_MODE_MENU,
        )
        return

    if text == "🔚 Закончить диалог":
        awaiting_contact[user.id] = False
        await message.reply_text("Диалог завершён. Если понадоблюсь — жми «💬 Написать владельцу».", reply_markup=USER_MENU)
        return

    if text == "🔗 Другие соц. сети":
        is_subscribed = await _check_membership_live(context, user.id)
        if is_subscribed:
            social_keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton(name, url=url)] for name, url in SOCIAL_LINKS.items()]
            )
            await message.reply_text(
                "Ты подписан(а) на канал 🙌\nВот мои дополнительные соц. сети 👇",
                reply_markup=social_keyboard,
            )
            await message.reply_text("Меню:", reply_markup=USER_MENU)
        else:
            await message.reply_text(
                "Похоже, ты ещё не подписан(а) на канал.\n"
                f'Подпишись здесь: <a href="{CHANNEL_INVITE_LINK}">перейти в канал</a>\n'
                "И потом снова нажми эту кнопку — покажу дополнительные ссылки 🙂",
                reply_markup=USER_MENU,
                parse_mode=ParseMode.HTML,
            )
        return

    # Обычное сообщение -> форма связи, только если пользователь нажал "Написать владельцу"
    if not awaiting_contact.get(user.id):
        await message.reply_text(
            "Чтобы отправить сообщение мне — сначала нажми «💬 Написать владельцу» 👇",
            reply_markup=USER_MENU,
        )
        return

    _conn.execute(
        "INSERT INTO messages (user_id, user_name, username, date, text) VALUES (?, ?, ?, ?, ?)",
        (user.id, user.full_name, user.username, datetime.now().strftime("%Y-%m-%d %H:%M"), text),
    )
    _conn.commit()

    header = (
        f"✉️ Новое сообщение от {user.full_name} (@{user.username or 'без username'}, id={user.id})\n"
        f"Написать ему напрямую: tg://user?id={user.id}\n\n"
    )
    sent_map = await _notify_admins(context, header + text)
    for admin_id, msg_id in sent_map.items():
        _remember_reply_target(msg_id, user.id)

    await message.reply_text("Сообщение отправлено, скоро с тобой свяжутся.\nЕщё что-то? Пиши сюда же 👇", reply_markup=CONTACT_MODE_MENU)


async def on_photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    message = update.message
    if message is None:
        return
    caption = message.caption or ""
    photo_file_id = message.photo[-1].file_id  # самое большое доступное разрешение

    # ---- Админ отвечает фото ----
    if _is_admin(chat_id):
        if message.reply_to_message:
            target_user_id = _get_reply_target(message.reply_to_message.message_id)
            if target_user_id:
                try:
                    await context.bot.send_photo(
                        chat_id=target_user_id,
                        photo=photo_file_id,
                        caption=f"✉️ Ответ:\n\n{caption}" if caption else "✉️ Ответ",
                    )
                    await message.reply_text("Отправлено.")
                except Exception as e:
                    await message.reply_text(f"⚠️ Не удалось отправить: {e}")
        return

    # ---- Обычные пользователи ----
    user = update.effective_user
    _remember_user(user)

    if _is_blocked(user.id):
        await message.reply_text("Извини, ты заблокирован(а) и не можешь отправлять сообщения.")
        return

    if not awaiting_contact.get(user.id):
        await message.reply_text(
            "Чтобы отправить фото мне — сначала нажми «💬 Написать владельцу» 👇",
            reply_markup=USER_MENU,
        )
        return

    _conn.execute(
        "INSERT INTO messages (user_id, user_name, username, date, text) VALUES (?, ?, ?, ?, ?)",
        (user.id, user.full_name, user.username, datetime.now().strftime("%Y-%m-%d %H:%M"), f"[фото] {caption}".strip()),
    )
    _conn.commit()

    header = (
        f"📷 Новое фото от {user.full_name} (@{user.username or 'без username'}, id={user.id})\n"
        f"Написать ему напрямую: tg://user?id={user.id}\n\n{caption}"
    ).strip()

    sent_map: dict[int, int] = {}
    for admin_id in ADMIN_IDS:
        try:
            msg = await context.bot.send_photo(chat_id=admin_id, photo=photo_file_id, caption=header)
            sent_map[admin_id] = msg.message_id
        except TelegramError:
            logger.exception("Не удалось отправить фото админу %s", admin_id)

    for admin_id, msg_id in sent_map.items():
        _remember_reply_target(msg_id, user.id)

    await message.reply_text(
        "Фото отправлено, скоро с тобой свяжутся.\nЕщё что-то? Пиши сюда же 👇", reply_markup=CONTACT_MODE_MENU
    )


# ---------- Health-check сервер (нужен для Render Free: он ждёт открытый порт) ----------
def _run_health_server() -> None:
    port = int(os.environ.get("PORT", "10000"))

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format, *args):
            pass  # не засорять логи каждым health-чеком/пингом

    HTTPServer(("0.0.0.0", port), _Handler).serve_forever()


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Необработанная ошибка при обработке апдейта %s", update, exc_info=context.error)


def main() -> None:
    threading.Thread(target=_run_health_server, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("approve_all", cmd_approve_all))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("unblock", cmd_unblock))
    app.add_handler(CommandHandler("decline_id", cmd_decline_id))
    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(ChatMemberHandler(on_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.FORWARDED & filters.UpdateType.MESSAGE, on_forwarded_channel_post))
    app.add_handler(MessageHandler(filters.PHOTO & filters.UpdateType.MESSAGE, on_photo_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.UpdateType.MESSAGE, on_private_message))

    logger.info("Бот запущен...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()