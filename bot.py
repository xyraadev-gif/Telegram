"""
Telegram-бот ежедневных напоминаний: Премиум-подписка (RUB / Telegram Stars),
промокоды и секретная админ-панель.

Стек: Python 3.10+, aiogram 3.x, aiosqlite, APScheduler 3.x.
Запуск: python bot.py   (настройки — в .env, см. .env.example)
"""
import asyncio
import hmac
import html
import logging
import math
import os
import re
import secrets
import string
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

# ════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ════════════════════════════════════════════════════════════════════════════
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN", "")  # пусто -> оплата в рублях скрыта
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "")
DB_PATH = os.getenv("DB_PATH", "bot.db")
DEFAULT_TZ = os.getenv("DEFAULT_TIMEZONE", "Europe/Moscow")

FREE_LIMIT = 10          # лимит активных напоминаний для бесплатных
PAGE_SIZE = 8            # напоминаний на страницу в списке
LATE_WINDOW_SEC = 600    # если планировщик опоздал — досылаем в течение 10 минут
MAX_TEXT_LEN = 200
MAX_NOTE_LEN = 500

# Тарифы: дни, цена в рублях, цена в звёздах
PLANS = {
    "1m": {"title": "1 месяц", "days": 30, "rub": 65, "stars": 100},
    "3m": {"title": "3 месяца", "days": 90, "rub": 195, "stars": 290},
    "1y": {"title": "1 год", "days": 365, "rub": 700, "stars": 1000},
}

# Стили уведомлений (Премиум): ключ -> (название кнопки, заголовок, без звука?)
STYLES = {
    "normal": ("🔔 Обычный", "🔔 <b>Напоминание</b>", False),
    "urgent": ("⚡️ Срочный", "⚡️🚨 <b>СРОЧНО!</b> 🚨⚡️", False),
    "calm": ("🧘 Спокойный", "🧘 <i>Спокойное напоминание</i>", True),  # тихая доставка
}
# Необязательные стикеры (file_id) для стилей: STICKER_NORMAL / STICKER_URGENT / STICKER_CALM
STICKERS = {k: os.getenv(f"STICKER_{k.upper()}", "") for k in STYLES}

BTN_ADD = "➕ Добавить напоминание"
BTN_LIST = "📋 Мои напоминания"
BTN_PREMIUM = "👑 Премиум подписка"
BTN_PROMO = "🎟 Ввести промокод"
BTN_HELP = "ℹ️ Помощь / FAQ"
MENU_TEXTS = {BTN_ADD, BTN_LIST, BTN_PREMIUM, BTN_PROMO, BTN_HELP}

log = logging.getLogger("reminder_bot")


# ════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ════════════════════════════════════════════════════════════════════════════
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id          INTEGER PRIMARY KEY,
    username         TEXT,
    first_name       TEXT,
    tz               TEXT    NOT NULL,
    premium_until    INTEGER NOT NULL DEFAULT 0,   -- unix timestamp (UTC)
    discount_percent INTEGER NOT NULL DEFAULT 0,   -- скидка на ближайшую покупку
    blocked          INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

CREATE TABLE IF NOT EXISTS reminders (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id        INTEGER NOT NULL,
    remind_time    TEXT    NOT NULL,               -- 'HH:MM' в часовом поясе пользователя
    text           TEXT    NOT NULL,
    note           TEXT,                           -- Премиум: заметка
    style          TEXT    NOT NULL DEFAULT 'normal',
    active         INTEGER NOT NULL DEFAULT 1,
    last_sent_date TEXT,                           -- 'YYYY-MM-DD' (локальная дата)
    created_at     INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_rem_user ON reminders(user_id, active);

CREATE TABLE IF NOT EXISTS promocodes (
    code            TEXT PRIMARY KEY,
    type            TEXT    NOT NULL,              -- 'days' | 'discount'
    value           INTEGER NOT NULL,              -- дни или проценты
    max_activations INTEGER NOT NULL,
    used_count      INTEGER NOT NULL DEFAULT 0,
    active          INTEGER NOT NULL DEFAULT 1,
    created_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS promo_usages (
    code    TEXT    NOT NULL,
    user_id INTEGER NOT NULL,
    used_at INTEGER NOT NULL,
    PRIMARY KEY (code, user_id)
);

CREATE TABLE IF NOT EXISTS payments (
    charge_id  TEXT PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    plan       TEXT    NOT NULL,
    currency   TEXT    NOT NULL,
    amount     INTEGER NOT NULL,
    days       INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY
);
"""


def now_ts() -> int:
    return int(time.time())


def is_premium(premium_until: int) -> bool:
    return premium_until > now_ts()


class Database:
    """Тонкая async-обёртка над одним соединением SQLite (+ lock для атомарных операций)."""

    def __init__(self, path: str):
        self.path = path
        self.conn: aiosqlite.Connection
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA foreign_keys=ON")
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        await self.conn.close()

    async def _one(self, sql: str, args: tuple = ()):
        cur = await self.conn.execute(sql, args)
        return await cur.fetchone()

    async def _all(self, sql: str, args: tuple = ()):
        cur = await self.conn.execute(sql, args)
        return await cur.fetchall()

    # ── пользователи ────────────────────────────────────────────────────────
    async def upsert_user(self, user) -> None:
        async with self._lock:
            await self.conn.execute(
                """INSERT INTO users(user_id, username, first_name, tz, created_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                     username=excluded.username, first_name=excluded.first_name, blocked=0""",
                (user.id, (user.username or "").lower() or None, user.first_name,
                 DEFAULT_TZ, now_ts()),
            )
            await self.conn.commit()

    async def get_user(self, user_id: int):
        return await self._one("SELECT * FROM users WHERE user_id=?", (user_id,))

    async def find_user(self, ident: str):
        ident = ident.strip()
        if ident.lstrip("-").isdigit():
            return await self.get_user(int(ident))
        return await self._one(
            "SELECT * FROM users WHERE username=?", (ident.lstrip("@").lower(),)
        )

    async def set_tz(self, user_id: int, tz: str) -> None:
        async with self._lock:
            await self.conn.execute("UPDATE users SET tz=? WHERE user_id=?", (tz, user_id))
            await self.conn.commit()

    async def mark_blocked(self, user_id: int) -> None:
        async with self._lock:
            await self.conn.execute("UPDATE users SET blocked=1 WHERE user_id=?", (user_id,))
            await self.conn.commit()

    async def broadcast_ids(self) -> list[int]:
        rows = await self._all("SELECT user_id FROM users WHERE blocked=0")
        return [r["user_id"] for r in rows]

    # ── премиум ─────────────────────────────────────────────────────────────
    async def _extend(self, user_id: int, days: int) -> int:
        """Прибавляет дни к premium_until (от текущего конца подписки или от «сейчас»)."""
        row = await self._one("SELECT premium_until FROM users WHERE user_id=?", (user_id,))
        base = max(row["premium_until"], now_ts())
        new_until = base + days * 86400
        await self.conn.execute(
            "UPDATE users SET premium_until=? WHERE user_id=?", (new_until, user_id)
        )
        return new_until

    async def add_premium_days(self, user_id: int, days: int) -> int:
        async with self._lock:
            until = await self._extend(user_id, days)
            await self.conn.commit()
            return until

    async def revoke_premium(self, user_id: int, days: int) -> int:
        """days=0 — забрать полностью; иначе вычесть дни (не ниже «сейчас»)."""
        async with self._lock:
            row = await self._one("SELECT premium_until FROM users WHERE user_id=?", (user_id,))
            until = 0 if days == 0 else max(0, row["premium_until"] - days * 86400)
            if until <= now_ts():
                until = 0
            await self.conn.execute(
                "UPDATE users SET premium_until=? WHERE user_id=?", (until, user_id)
            )
            await self.conn.commit()
            return until

    # ── напоминания ─────────────────────────────────────────────────────────
    async def count_active(self, user_id: int) -> int:
        row = await self._one(
            "SELECT COUNT(*) c FROM reminders WHERE user_id=? AND active=1", (user_id,)
        )
        return row["c"]

    async def add_reminder(self, user_id, remind_time, text, note, style) -> int:
        async with self._lock:
            cur = await self.conn.execute(
                """INSERT INTO reminders(user_id, remind_time, text, note, style, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (user_id, remind_time, text, note, style, now_ts()),
            )
            await self.conn.commit()
            return cur.lastrowid

    async def list_reminders(self, user_id: int, limit: int, offset: int):
        return await self._all(
            """SELECT * FROM reminders WHERE user_id=? AND active=1
               ORDER BY remind_time, id LIMIT ? OFFSET ?""",
            (user_id, limit, offset),
        )

    async def delete_reminder(self, rem_id: int, user_id: int) -> None:
        async with self._lock:
            await self.conn.execute(
                "DELETE FROM reminders WHERE id=? AND user_id=?", (rem_id, user_id)
            )
            await self.conn.commit()

    async def reminder_candidates(self):
        """Все активные напоминания активных пользователей (+ tz и статус премиума)."""
        return await self._all(
            """SELECT r.*, u.tz, u.premium_until
               FROM reminders r JOIN users u ON u.user_id = r.user_id
               WHERE r.active=1 AND u.blocked=0"""
        )

    async def mark_sent(self, rem_id: int, local_date: str) -> None:
        async with self._lock:
            await self.conn.execute(
                "UPDATE reminders SET last_sent_date=? WHERE id=?", (local_date, rem_id)
            )
            await self.conn.commit()

    # ── промокоды ───────────────────────────────────────────────────────────
    async def create_promo(self, code: str, ptype: str, value: int, max_act: int) -> bool:
        async with self._lock:
            try:
                await self.conn.execute(
                    """INSERT INTO promocodes(code, type, value, max_activations, created_at)
                       VALUES(?,?,?,?,?)""",
                    (code, ptype, value, max_act, now_ts()),
                )
                await self.conn.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def redeem_promo(self, user_id: int, code: str):
        """Возвращает (статус, строка_промокода). Статусы: ok / invalid / exhausted / used."""
        code = code.strip().upper()
        async with self._lock:
            p = await self._one("SELECT * FROM promocodes WHERE code=?", (code,))
            if not p or not p["active"]:
                return "invalid", None
            if p["used_count"] >= p["max_activations"]:
                return "exhausted", None
            if await self._one(
                "SELECT 1 FROM promo_usages WHERE code=? AND user_id=?", (code, user_id)
            ):
                return "used", None
            await self.conn.execute(
                "INSERT INTO promo_usages(code, user_id, used_at) VALUES(?,?,?)",
                (code, user_id, now_ts()),
            )
            await self.conn.execute(
                "UPDATE promocodes SET used_count=used_count+1 WHERE code=?", (code,)
            )
            if p["type"] == "days":
                await self._extend(user_id, p["value"])
            else:  # скидка — берём максимальную из имеющейся и новой
                await self.conn.execute(
                    "UPDATE users SET discount_percent=MAX(discount_percent, ?) WHERE user_id=?",
                    (p["value"], user_id),
                )
            await self.conn.commit()
            return "ok", p

    # ── платежи ─────────────────────────────────────────────────────────────
    async def apply_payment(self, user_id, charge_id, plan, currency, amount, days, used_discount):
        """Идемпотентно: повторный charge_id не продлевает подписку второй раз."""
        async with self._lock:
            cur = await self.conn.execute(
                "INSERT OR IGNORE INTO payments VALUES(?,?,?,?,?,?,?)",
                (charge_id, user_id, plan, currency, amount, days, now_ts()),
            )
            if cur.rowcount == 0:
                await self.conn.commit()
                return None
            until = await self._extend(user_id, days)
            if used_discount:
                await self.conn.execute(
                    "UPDATE users SET discount_percent=0 WHERE user_id=?", (user_id,)
                )
            await self.conn.commit()
            return until

    # ── админы ──────────────────────────────────────────────────────────────
    async def is_admin(self, user_id: int) -> bool:
        return await self._one("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) is not None

    async def add_admin(self, user_id: int) -> None:
        async with self._lock:
            await self.conn.execute("INSERT OR IGNORE INTO admins VALUES(?)", (user_id,))
            await self.conn.commit()

    async def remove_admin(self, user_id: int) -> None:
        async with self._lock:
            await self.conn.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
            await self.conn.commit()

    async def stats(self) -> dict:
        day_ago = now_ts() - 86400
        return {
            "users": (await self._one("SELECT COUNT(*) c FROM users"))["c"],
            "new_24h": (await self._one(
                "SELECT COUNT(*) c FROM users WHERE created_at>?", (day_ago,)))["c"],
            "premium": (await self._one(
                "SELECT COUNT(*) c FROM users WHERE premium_until>?", (now_ts(),)))["c"],
            "reminders_total": (await self._one("SELECT COUNT(*) c FROM reminders"))["c"],
            "payments": (await self._one("SELECT COUNT(*) c FROM payments"))["c"],
        }


db = Database(DB_PATH)


# ════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНОЕ
# ════════════════════════════════════════════════════════════════════════════
TIME_RE = re.compile(r"^\s*(\d{1,2})[:.](\d{2})\s*$")
_TZ_CACHE: dict[str, ZoneInfo] = {}


def get_zone(name: str) -> ZoneInfo:
    if name not in _TZ_CACHE:
        try:
            _TZ_CACHE[name] = ZoneInfo(name)
        except ZoneInfoNotFoundError:
            _TZ_CACHE[name] = ZoneInfo("UTC")
    return _TZ_CACHE[name]


def parse_time(raw: str) -> Optional[str]:
    m = TIME_RE.match(raw)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h > 23 or mi > 59:
        return None
    return f"{h:02d}:{mi:02d}"


def fmt_date(ts: int, tz: str) -> str:
    return datetime.fromtimestamp(ts, get_zone(tz)).strftime("%d.%m.%Y %H:%M")


def esc(s: str) -> str:
    return html.escape(s or "")


def rub_amount(plan: str, discount: int) -> int:
    """Сумма в копейках: рубли * (100 - скидка%) копеек."""
    return PLANS[plan]["rub"] * (100 - discount)


def stars_amount(plan: str, discount: int) -> int:
    return max(1, (PLANS[plan]["stars"] * (100 - discount) + 50) // 100)


def fmt_rub(kopecks: int) -> str:
    return f"{kopecks / 100:g} ₽"


async def tg_retry(func, *args, **kwargs):
    """Вызов Telegram API с повтором при flood-контроле."""
    for _ in range(3):
        try:
            return await func(*args, **kwargs)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
    raise RuntimeError("Слишком много повторов из-за flood control")


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADD), KeyboardButton(text=BTN_LIST)],
            [KeyboardButton(text=BTN_PREMIUM), KeyboardButton(text=BTN_PROMO)],
            [KeyboardButton(text=BTN_HELP)],
        ],
        resize_keyboard=True,
    )


def cancel_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✖️ Отмена", callback_data="cancel")
    return b.as_markup()


# ════════════════════════════════════════════════════════════════════════════
# FSM-СОСТОЯНИЯ
# ════════════════════════════════════════════════════════════════════════════
class AddReminder(StatesGroup):
    time = State()
    text = State()
    note = State()
    style = State()


class PromoInput(StatesGroup):
    code = State()


class AdminLogin(StatesGroup):
    password = State()


class AdminFlow(StatesGroup):
    target = State()
    days = State()
    promo_value = State()
    promo_max = State()
    promo_code = State()
    broadcast = State()
    broadcast_confirm = State()


# ════════════════════════════════════════════════════════════════════════════
# MIDDLEWARE: регистрируем/обновляем пользователя при любом апдейте
# ════════════════════════════════════════════════════════════════════════════
class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user and not user.is_bot:
            await db.upsert_user(user)
        return await handler(event, data)


# ════════════════════════════════════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЙ РОУТЕР
# ════════════════════════════════════════════════════════════════════════════
user_router = Router(name="user")

HELP_TEXT = (
    "ℹ️ <b>Помощь / FAQ</b>\n\n"
    "<b>Как это работает?</b>\n"
    "Вы создаёте напоминание с временем ЧЧ:ММ — бот присылает его <b>каждый день</b> "
    "в это время по вашему часовому поясу.\n\n"
    "<b>Часовой пояс</b>\n"
    "Команда <code>/timezone Europe/Berlin</code> (список: «IANA time zones»). "
    f"По умолчанию: <code>{DEFAULT_TZ}</code>.\n\n"
    f"<b>Бесплатно:</b> до {FREE_LIMIT} активных напоминаний, стандартный текст.\n"
    "<b>👑 Премиум:</b> безлимит, заметки к напоминаниям, стили уведомлений "
    "(🔔 обычный, ⚡️ срочный, 🧘 спокойный — без звука), приоритетная доставка.\n\n"
    "<b>Оплата:</b> рубли (карта) или Telegram Stars ⭐️. Дни прибавляются к текущей подписке.\n"
    "<b>Промокоды</b> дают бесплатные дни Премиума или скидку на покупку.\n\n"
    "Отмена любого действия — /cancel"
)


@user_router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(
        f"👋 Привет, {esc(m.from_user.first_name)}!\n\n"
        "Я напоминаю о важном каждый день в нужное время. Выберите действие в меню ниже.",
        reply_markup=main_menu(),
    )


@user_router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Действие отменено.", reply_markup=main_menu())


@user_router.callback_query(F.data == "cancel")
async def cb_cancel(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Действие отменено.")
    await c.answer()


@user_router.message(Command("timezone"))
async def cmd_timezone(m: Message, command: CommandObject):
    user = await db.get_user(m.from_user.id)
    if not command.args:
        await m.answer(
            f"Ваш часовой пояс: <code>{esc(user['tz'])}</code>\n"
            "Изменить: <code>/timezone Europe/Berlin</code>"
        )
        return
    name = command.args.strip()
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        await m.answer("❌ Неизвестный часовой пояс. Пример: <code>Europe/Moscow</code>, "
                       "<code>Asia/Almaty</code>, <code>Europe/Berlin</code>.")
        return
    await db.set_tz(m.from_user.id, name)
    await m.answer(f"✅ Часовой пояс: <code>{esc(name)}</code>")


# ── Главное меню (сбрасывает любое FSM-состояние) ───────────────────────────
@user_router.message(F.text == BTN_HELP)
async def menu_help(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(HELP_TEXT)


@user_router.message(F.text == BTN_ADD)
async def menu_add(m: Message, state: FSMContext):
    await state.clear()
    user = await db.get_user(m.from_user.id)
    if not is_premium(user["premium_until"]):
        if await db.count_active(m.from_user.id) >= FREE_LIMIT:
            b = InlineKeyboardBuilder()
            b.button(text="👑 Получить Премиум", callback_data="buy:back")
            await m.answer(
                f"⚠️ Достигнут лимит бесплатного тарифа: {FREE_LIMIT} напоминаний.\n"
                "Удалите ненужные или оформите Премиум — безлимит и заметки.",
                reply_markup=b.as_markup(),
            )
            return
    await state.set_state(AddReminder.time)
    await m.answer(
        "⏰ Введите время в формате <b>ЧЧ:ММ</b> (например, <code>08:30</code>).\n"
        f"Часовой пояс: <code>{esc(user['tz'])}</code> (изменить: /timezone)",
        reply_markup=cancel_kb(),
    )


@user_router.message(AddReminder.time)
async def add_time(m: Message, state: FSMContext):
    t = parse_time(m.text or "")
    if not t:
        await m.answer("❌ Неверный формат. Введите время как <code>ЧЧ:ММ</code>, например <code>21:05</code>.")
        return
    await state.update_data(time=t)
    await state.set_state(AddReminder.text)
    await m.answer(f"Время: <b>{t}</b>\n\n✏️ Теперь введите текст напоминания:", reply_markup=cancel_kb())


@user_router.message(AddReminder.text)
async def add_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if not text or len(text) > MAX_TEXT_LEN:
        await m.answer(f"❌ Текст должен быть от 1 до {MAX_TEXT_LEN} символов.")
        return
    await state.update_data(text=text)
    user = await db.get_user(m.from_user.id)
    if is_premium(user["premium_until"]):
        await state.set_state(AddReminder.note)
        b = InlineKeyboardBuilder()
        b.button(text="⏭ Пропустить", callback_data="note:skip")
        b.button(text="✖️ Отмена", callback_data="cancel")
        b.adjust(2)
        await m.answer(
            "📝 <b>Премиум:</b> добавьте заметку/описание (например, «2 таблетки после еды») "
            "или нажмите «Пропустить».",
            reply_markup=b.as_markup(),
        )
    else:
        await _save_reminder(m, state, note=None, style="normal")


@user_router.message(AddReminder.note)
async def add_note(m: Message, state: FSMContext):
    note = (m.text or "").strip()
    if not note or len(note) > MAX_NOTE_LEN:
        await m.answer(f"❌ Заметка должна быть от 1 до {MAX_NOTE_LEN} символов.")
        return
    await state.update_data(note=note)
    await _ask_style(m, state)


@user_router.callback_query(AddReminder.note, F.data == "note:skip")
async def add_note_skip(c: CallbackQuery, state: FSMContext):
    await state.update_data(note=None)
    await c.answer()
    await _ask_style(c.message, state)


async def _ask_style(msg: Message, state: FSMContext):
    await state.set_state(AddReminder.style)
    b = InlineKeyboardBuilder()
    for key, (label, _, _) in STYLES.items():
        b.button(text=label, callback_data=f"style:{key}")
    b.adjust(1)
    await msg.answer("🎨 Выберите стиль уведомления:", reply_markup=b.as_markup())


@user_router.callback_query(AddReminder.style, F.data.startswith("style:"))
async def add_style(c: CallbackQuery, state: FSMContext):
    style = c.data.split(":")[1]
    if style not in STYLES:
        await c.answer("Неизвестный стиль", show_alert=True)
        return
    data = await state.get_data()
    await c.answer()
    await _save_reminder(c.message, state, note=data.get("note"), style=style, user_id=c.from_user.id)


async def _save_reminder(msg: Message, state: FSMContext, note, style, user_id: Optional[int] = None):
    uid = user_id or msg.from_user.id
    data = await state.get_data()
    await state.clear()
    # Повторная проверка лимита (защита от гонок)
    user = await db.get_user(uid)
    if not is_premium(user["premium_until"]) and await db.count_active(uid) >= FREE_LIMIT:
        await msg.answer("⚠️ Лимит бесплатного тарифа исчерпан.", reply_markup=main_menu())
        return
    await db.add_reminder(uid, data["time"], data["text"], note, style)
    extra = f"\n📝 {esc(note)}" if note else ""
    await msg.answer(
        f"✅ Напоминание создано!\n⏰ Ежедневно в <b>{data['time']}</b>\n💬 {esc(data['text'])}{extra}",
        reply_markup=main_menu(),
    )


# ── Список напоминаний ──────────────────────────────────────────────────────
async def render_list(uid: int, page: int):
    user = await db.get_user(uid)
    premium = is_premium(user["premium_until"])
    total = await db.count_active(uid)
    if total == 0:
        return "📋 У вас пока нет напоминаний. Нажмите «➕ Добавить напоминание».", None
    pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(max(page, 0), pages - 1)
    rows = await db.list_reminders(uid, PAGE_SIZE, page * PAGE_SIZE)
    limit = "∞" if premium else str(FREE_LIMIT)
    text = (f"📋 <b>Ваши напоминания</b> ({total}/{limit})\n"
            "Нажмите на напоминание, чтобы удалить его.")
    b = InlineKeyboardBuilder()
    for r in rows:
        mark = (STYLES[r["style"]][0].split()[0] if premium and r["style"] in STYLES else "⏰")
        note = " 📝" if r["note"] else ""
        b.button(text=f"🗑 {mark} {r['remind_time']} — {r['text'][:28]}{note}",
                 callback_data=f"rem:del:{r['id']}:{page}")
    layout = [1] * len(rows)
    if pages > 1:
        nav = 0
        if page > 0:
            b.button(text="⬅️", callback_data=f"rem:list:{page - 1}")
            nav += 1
        b.button(text=f"{page + 1}/{pages}", callback_data="rem:noop")
        nav += 1
        if page < pages - 1:
            b.button(text="➡️", callback_data=f"rem:list:{page + 1}")
            nav += 1
        layout.append(nav)
    b.adjust(*layout)
    return text, b.as_markup()


@user_router.message(F.text == BTN_LIST)
async def menu_list(m: Message, state: FSMContext):
    await state.clear()
    text, kb = await render_list(m.from_user.id, 0)
    await m.answer(text, reply_markup=kb)


@user_router.callback_query(F.data.startswith("rem:list:"))
async def cb_list(c: CallbackQuery):
    text, kb = await render_list(c.from_user.id, int(c.data.split(":")[2]))
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass
    await c.answer()


@user_router.callback_query(F.data == "rem:noop")
async def cb_noop(c: CallbackQuery):
    await c.answer()


@user_router.callback_query(F.data.startswith("rem:del:"))
async def cb_delete(c: CallbackQuery):
    _, _, rid, page = c.data.split(":")
    await db.delete_reminder(int(rid), c.from_user.id)
    text, kb = await render_list(c.from_user.id, int(page))
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass
    await c.answer("🗑 Удалено")


# ── Промокоды ───────────────────────────────────────────────────────────────
@user_router.message(F.text == BTN_PROMO)
async def menu_promo(m: Message, state: FSMContext):
    await state.clear()
    await state.set_state(PromoInput.code)
    await m.answer("🎟 Введите промокод:", reply_markup=cancel_kb())


@user_router.message(PromoInput.code)
async def promo_enter(m: Message, state: FSMContext):
    status, p = await db.redeem_promo(m.from_user.id, m.text or "")
    if status == "ok":
        await state.clear()
        user = await db.get_user(m.from_user.id)
        if p["type"] == "days":
            await m.answer(
                f"🎉 Промокод активирован! <b>+{p['value']} дн.</b> Премиума.\n"
                f"Подписка действует до <b>{fmt_date(user['premium_until'], user['tz'])}</b>.",
                reply_markup=main_menu(),
            )
        else:
            await m.answer(
                f"🎉 Промокод активирован! Скидка <b>{p['value']}%</b> применится "
                "к вашей следующей покупке Премиума.",
                reply_markup=main_menu(),
            )
        return
    errors = {
        "invalid": "❌ Такого промокода нет или он отключён.",
        "exhausted": "😕 Лимит активаций этого промокода исчерпан.",
        "used": "ℹ️ Вы уже использовали этот промокод.",
    }
    await m.answer(errors[status] + "\nПопробуйте другой код или нажмите «Отмена».",
                   reply_markup=cancel_kb())


# ── Премиум и оплата ────────────────────────────────────────────────────────
async def premium_text(uid: int) -> str:
    user = await db.get_user(uid)
    if is_premium(user["premium_until"]):
        status = f"✅ Активен до <b>{fmt_date(user['premium_until'], user['tz'])}</b>"
    else:
        status = "❌ Не активен"
    disc = (f"\n🎟 У вас скидка <b>{user['discount_percent']}%</b> на покупку!"
            if user["discount_percent"] else "")
    return (
        "👑 <b>Премиум подписка</b>\n\n"
        f"Статус: {status}{disc}\n\n"
        "<b>Что даёт Премиум:</b>\n"
        "• ♾ Безлимитные напоминания\n"
        "• 📝 Заметки и описания к напоминаниям\n"
        "• 🎨 Стили: 🔔 обычный, ⚡️ срочный, 🧘 спокойный (без звука) + стикеры\n"
        "• 🚀 Приоритетная доставка\n\n"
        "Выберите способ оплаты:"
    )


def currency_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if PROVIDER_TOKEN:
        b.button(text="💳 Оплатить в рублях (₽)", callback_data="buy:cur:rub")
    b.button(text="⭐️ Оплатить Telegram Stars", callback_data="buy:cur:xtr")
    b.adjust(1)
    return b.as_markup()


@user_router.message(F.text == BTN_PREMIUM)
async def menu_premium(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(await premium_text(m.from_user.id), reply_markup=currency_kb())


@user_router.callback_query(F.data == "buy:back")
async def cb_buy_back(c: CallbackQuery):
    await c.message.answer(await premium_text(c.from_user.id), reply_markup=currency_kb())
    await c.answer()


@user_router.callback_query(F.data.startswith("buy:cur:"))
async def cb_choose_currency(c: CallbackQuery):
    cur = c.data.split(":")[2]
    if cur == "rub" and not PROVIDER_TOKEN:
        await c.answer("Оплата в рублях временно недоступна", show_alert=True)
        return
    user = await db.get_user(c.from_user.id)
    d = user["discount_percent"]
    b = InlineKeyboardBuilder()
    for key, p in PLANS.items():
        if cur == "rub":
            price = fmt_rub(rub_amount(key, d))
        else:
            price = f"{stars_amount(key, d)} ⭐️"
        suffix = f" (−{d}%)" if d else ""
        b.button(text=f"{p['title']} — {price}{suffix}", callback_data=f"buy:{cur}:{key}")
    b.button(text="⬅️ Назад", callback_data="buy:menu")
    b.adjust(1)
    await c.message.edit_text(
        "Выберите тариф:" + ("\n💳 Оплата картой в рублях" if cur == "rub" else "\n⭐️ Оплата Telegram Stars"),
        reply_markup=b.as_markup(),
    )
    await c.answer()


@user_router.callback_query(F.data == "buy:menu")
async def cb_buy_menu(c: CallbackQuery):
    await c.message.edit_text(await premium_text(c.from_user.id), reply_markup=currency_kb())
    await c.answer()


@user_router.callback_query(F.data.regexp(r"^buy:(rub|xtr):(1m|3m|1y)$"))
async def cb_buy_plan(c: CallbackQuery):
    _, cur, plan_key = c.data.split(":")
    plan = PLANS[plan_key]
    user = await db.get_user(c.from_user.id)
    d = user["discount_percent"]
    if cur == "rub":
        if not PROVIDER_TOKEN:
            await c.answer("Оплата в рублях недоступна", show_alert=True)
            return
        amount, currency, token = rub_amount(plan_key, d), "RUB", PROVIDER_TOKEN
    else:
        amount, currency, token = stars_amount(plan_key, d), "XTR", ""
    await c.message.answer_invoice(
        title=f"Премиум — {plan['title']}",
        description=f"Премиум-подписка на {plan['days']} дн.: безлимит, заметки, стили, приоритет.",
        payload=f"sub|{plan_key}|{cur}|{d}",
        provider_token=token,
        currency=currency,
        prices=[LabeledPrice(label=f"Премиум {plan['title']}", amount=amount)],
    )
    await c.answer()


def parse_payload(payload: str) -> Optional[tuple[str, str, int]]:
    try:
        tag, plan, cur, d = payload.split("|")
        if tag != "sub" or plan not in PLANS or cur not in ("rub", "xtr"):
            return None
        return plan, cur, int(d)
    except ValueError:
        return None


@user_router.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    parsed = parse_payload(q.invoice_payload)
    if not parsed:
        await q.answer(ok=False, error_message="Некорректный счёт. Создайте счёт заново.")
        return
    plan, cur, d = parsed
    user = await db.get_user(q.from_user.id)
    expected = rub_amount(plan, d) if cur == "rub" else stars_amount(plan, d)
    expected_cur = "RUB" if cur == "rub" else "XTR"
    if (q.total_amount != expected or q.currency != expected_cur
            or (user and user["discount_percent"] != d)):
        await q.answer(ok=False, error_message="Цена или скидка изменились. Создайте счёт заново.")
        return
    await q.answer(ok=True)


@user_router.message(F.successful_payment)
async def successful_payment(m: Message):
    pay = m.successful_payment
    parsed = parse_payload(pay.invoice_payload)
    if not parsed:
        log.error("Платёж с неизвестным payload: %s", pay.invoice_payload)
        return
    plan_key, cur, d = parsed
    days = PLANS[plan_key]["days"]
    until = await db.apply_payment(
        m.from_user.id, pay.telegram_payment_charge_id, plan_key,
        pay.currency, pay.total_amount, days, used_discount=d > 0,
    )
    if until is None:
        return  # дубликат уведомления
    user = await db.get_user(m.from_user.id)
    log.info("Оплата: user=%s plan=%s %s %s", m.from_user.id, plan_key, pay.total_amount, pay.currency)
    await m.answer(
        f"🎉 Спасибо за оплату! Добавлено <b>{days} дн.</b> Премиума.\n"
        f"Подписка активна до <b>{fmt_date(until, user['tz'])}</b>.",
        reply_markup=main_menu(),
    )


# ── Вход в админку (секретный код) ──────────────────────────────────────────
_login_fails: dict[int, tuple[int, float]] = {}  # user_id -> (попыток, заблокирован_до)


def _check_secret(candidate: str) -> bool:
    return bool(ADMIN_SECRET_KEY) and hmac.compare_digest(
        candidate.strip().encode(), ADMIN_SECRET_KEY.encode()
    )


async def _try_login(m: Message, candidate: str) -> bool:
    uid = m.from_user.id
    fails, until = _login_fails.get(uid, (0, 0.0))
    if until > time.time():
        await m.answer("⛔️ Слишком много попыток. Попробуйте позже.")
        return False
    if _check_secret(candidate):
        _login_fails.pop(uid, None)
        await db.add_admin(uid)
        return True
    fails += 1
    _login_fails[uid] = (fails, time.time() + 900 if fails >= 5 else 0.0)
    await m.answer("❌ Неверный код.")
    return False


@user_router.message(Command("admin"))
async def cmd_admin(m: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    if await db.is_admin(m.from_user.id):
        await m.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())
        return
    if command.args:
        # Сообщение с паролем лучше сразу удалить
        try:
            await m.delete()
        except TelegramBadRequest:
            pass
        if await _try_login(m, command.args):
            await m.answer("🔓 Доступ разрешён.\n\n🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())
    else:
        await state.set_state(AdminLogin.password)
        await m.answer("🔐 Введите секретный код:")


@user_router.message(AdminLogin.password)
async def admin_password(m: Message, state: FSMContext):
    try:
        await m.delete()
    except TelegramBadRequest:
        pass
    if await _try_login(m, m.text or ""):
        await state.clear()
        await m.answer("🔓 Доступ разрешён.\n\n🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())


# ════════════════════════════════════════════════════════════════════════════
# АДМИН-РОУТЕР
# ════════════════════════════════════════════════════════════════════════════
class AdminFilter(BaseFilter):
    async def __call__(self, event) -> bool:
        user = getattr(event, "from_user", None)
        return bool(user) and await db.is_admin(user.id)


admin_router = Router(name="admin")
admin_router.message.filter(AdminFilter())
admin_router.callback_query.filter(AdminFilter())


def admin_menu_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📊 Статистика", callback_data="adm:stats")
    b.button(text="➕ Выдать Премиум", callback_data="adm:grant")
    b.button(text="➖ Забрать Премиум", callback_data="adm:revoke")
    b.button(text="🎟 Создать промокод", callback_data="adm:promo")
    b.button(text="📢 Рассылка", callback_data="adm:bcast")
    b.button(text="🚪 Выйти из админки", callback_data="adm:logout")
    b.adjust(1, 2, 1, 1, 1)
    return b.as_markup()


def back_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ В админ-панель", callback_data="adm:home")
    return b.as_markup()


@admin_router.callback_query(F.data == "adm:home")
async def adm_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())
    await c.answer()


@admin_router.callback_query(F.data == "adm:logout")
async def adm_logout(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await db.remove_admin(c.from_user.id)
    await c.message.edit_text("🔒 Вы вышли из админ-панели.")
    await c.answer()


@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(c: CallbackQuery):
    s = await db.stats()
    await c.message.edit_text(
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Пользователей: <b>{s['users']}</b> (+{s['new_24h']} за 24ч)\n"
        f"👑 Активных Премиум: <b>{s['premium']}</b>\n"
        f"⏰ Создано напоминаний: <b>{s['reminders_total']}</b>\n"
        f"💰 Оплат: <b>{s['payments']}</b>",
        reply_markup=back_kb(),
    )
    await c.answer()


# ── Выдать / забрать Премиум ────────────────────────────────────────────────
@admin_router.callback_query(F.data.in_({"adm:grant", "adm:revoke"}))
async def adm_grant_start(c: CallbackQuery, state: FSMContext):
    mode = c.data.split(":")[1]
    await state.set_state(AdminFlow.target)
    await state.update_data(mode=mode)
    await c.message.edit_text(
        f"{'➕ Выдача' if mode == 'grant' else '➖ Снятие'} Премиума.\n"
        "Отправьте <b>user_id</b> или <b>@username</b> пользователя "
        "(он должен хотя бы раз запустить бота):",
        reply_markup=back_kb(),
    )
    await c.answer()


@admin_router.message(AdminFlow.target)
async def adm_grant_target(m: Message, state: FSMContext):
    user = await db.find_user(m.text or "")
    if not user:
        await m.answer("❌ Пользователь не найден. Проверьте ID/@username:", reply_markup=back_kb())
        return
    await state.update_data(target=user["user_id"])
    await state.set_state(AdminFlow.days)
    data = await state.get_data()
    hint = ("Сколько дней выдать?" if data["mode"] == "grant"
            else "Сколько дней забрать? (<code>0</code> — забрать весь Премиум)")
    name = f"@{user['username']}" if user["username"] else str(user["user_id"])
    await m.answer(f"Пользователь: <b>{esc(name)}</b>\n{hint}", reply_markup=back_kb())


@admin_router.message(AdminFlow.days)
async def adm_grant_days(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    mode, uid = data["mode"], data["target"]
    raw = (m.text or "").strip()
    if not raw.isdigit() or int(raw) > 36500 or (mode == "grant" and int(raw) == 0):
        await m.answer("❌ Введите целое число дней (для выдачи — больше 0).", reply_markup=back_kb())
        return
    days = int(raw)
    await state.clear()
    if mode == "grant":
        until = await db.add_premium_days(uid, days)
        user = await db.get_user(uid)
        await m.answer(f"✅ Выдано +{days} дн. Премиум до {fmt_date(until, user['tz'])}.",
                       reply_markup=admin_menu_kb())
        notify = f"🎁 Администратор подарил вам <b>{days} дн.</b> Премиума!"
    else:
        await db.revoke_premium(uid, days)
        await m.answer("✅ Премиум обновлён." , reply_markup=admin_menu_kb())
        notify = "ℹ️ Ваша Премиум-подписка была изменена администратором."
    try:
        await bot.send_message(uid, notify)
    except (TelegramForbiddenError, TelegramBadRequest):
        pass


# ── Генерация промокодов ────────────────────────────────────────────────────
@admin_router.callback_query(F.data == "adm:promo")
async def adm_promo_start(c: CallbackQuery, state: FSMContext):
    await state.clear()
    b = InlineKeyboardBuilder()
    b.button(text="📅 Бесплатные дни Премиума", callback_data="adm:ptype:days")
    b.button(text="💸 Скидка на покупку (%)", callback_data="adm:ptype:discount")
    b.button(text="⬅️ Назад", callback_data="adm:home")
    b.adjust(1)
    await c.message.edit_text("🎟 Выберите тип промокода:", reply_markup=b.as_markup())
    await c.answer()


@admin_router.callback_query(F.data.startswith("adm:ptype:"))
async def adm_promo_type(c: CallbackQuery, state: FSMContext):
    ptype = c.data.split(":")[2]
    await state.set_state(AdminFlow.promo_value)
    await state.update_data(ptype=ptype)
    q = "Сколько дней Премиума давать? (например, 7)" if ptype == "days" \
        else "Размер скидки в процентах (1–99):"
    await c.message.edit_text(q, reply_markup=back_kb())
    await c.answer()


@admin_router.message(AdminFlow.promo_value)
async def adm_promo_value(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = (m.text or "").strip()
    hi = 3650 if data["ptype"] == "days" else 99
    if not raw.isdigit() or not (1 <= int(raw) <= hi):
        await m.answer(f"❌ Введите число от 1 до {hi}.", reply_markup=back_kb())
        return
    await state.update_data(value=int(raw))
    await state.set_state(AdminFlow.promo_max)
    await m.answer("Сколько раз можно активировать промокод? (например, 50)", reply_markup=back_kb())


@admin_router.message(AdminFlow.promo_max)
async def adm_promo_max(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    if not raw.isdigit() or not (1 <= int(raw) <= 1_000_000):
        await m.answer("❌ Введите положительное целое число.", reply_markup=back_kb())
        return
    await state.update_data(max_act=int(raw))
    await state.set_state(AdminFlow.promo_code)
    await m.answer("Отправьте свой код (A–Z, 0–9, 3–32 символа) или <code>-</code> для случайного:",
                   reply_markup=back_kb())


@admin_router.message(AdminFlow.promo_code)
async def adm_promo_code(m: Message, state: FSMContext):
    raw = (m.text or "").strip().upper()
    if raw == "-":
        alphabet = string.ascii_uppercase + string.digits
        raw = "".join(secrets.choice(alphabet) for _ in range(8))
    elif not re.fullmatch(r"[A-Z0-9_-]{3,32}", raw):
        await m.answer("❌ Допустимы A–Z, 0–9, «_», «-», длина 3–32.", reply_markup=back_kb())
        return
    data = await state.get_data()
    if not await db.create_promo(raw, data["ptype"], data["value"], data["max_act"]):
        await m.answer("❌ Такой код уже существует. Введите другой или «-»:", reply_markup=back_kb())
        return
    await state.clear()
    what = f"+{data['value']} дн. Премиума" if data["ptype"] == "days" else f"скидка {data['value']}%"
    await m.answer(
        f"✅ Промокод создан:\n\n<code>{raw}</code>\n\n"
        f"Тип: {what}\nАктиваций: {data['max_act']}",
        reply_markup=admin_menu_kb(),
    )


# ── Рассылка ────────────────────────────────────────────────────────────────
_bg_tasks: set[asyncio.Task] = set()


@admin_router.callback_query(F.data == "adm:bcast")
async def adm_bcast_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminFlow.broadcast)
    await c.message.edit_text(
        "📢 Отправьте сообщение для рассылки (текст, фото, видео — как есть, форматирование сохранится):",
        reply_markup=back_kb(),
    )
    await c.answer()


@admin_router.message(AdminFlow.broadcast)
async def adm_bcast_preview(m: Message, state: FSMContext):
    await state.update_data(src_chat=m.chat.id, src_msg=m.message_id)
    await state.set_state(AdminFlow.broadcast_confirm)
    count = len(await db.broadcast_ids())
    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Отправить ({count})", callback_data="adm:bgo")
    b.button(text="✖️ Отмена", callback_data="adm:home")
    b.adjust(1)
    await m.answer(f"Разослать это сообщение <b>{count}</b> пользователям?", reply_markup=b.as_markup())


@admin_router.callback_query(AdminFlow.broadcast_confirm, F.data == "adm:bgo")
async def adm_bcast_go(c: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    await c.message.edit_text("🚀 Рассылка запущена. Пришлю отчёт по завершении.")
    task = asyncio.create_task(run_broadcast(bot, c.from_user.id, data["src_chat"], data["src_msg"]))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    await c.answer()


async def run_broadcast(bot: Bot, admin_id: int, src_chat: int, src_msg: int) -> None:
    ids = await db.broadcast_ids()
    ok = fail = 0
    for uid in ids:
        try:
            await tg_retry(bot.copy_message, uid, src_chat, src_msg)
            ok += 1
        except TelegramForbiddenError:
            await db.mark_blocked(uid)
            fail += 1
        except Exception as e:  # noqa: BLE001
            log.warning("Рассылка: ошибка для %s: %s", uid, e)
            fail += 1
        await asyncio.sleep(0.05)  # ~20 сообщений/сек, в пределах лимитов Telegram
    await bot.send_message(admin_id, f"📢 Рассылка завершена.\n✅ Доставлено: {ok}\n❌ Ошибок: {fail}",
                           reply_markup=admin_menu_kb())


# ════════════════════════════════════════════════════════════════════════════
# ПЛАНИРОВЩИК НАПОМИНАНИЙ
# ════════════════════════════════════════════════════════════════════════════
async def deliver(bot: Bot, r) -> None:
    """Отправляет одно напоминание. Премиум-оформление применяется только действующим Премиум."""
    premium = is_premium(r["premium_until"])
    style = r["style"] if premium and r["style"] in STYLES else "normal"
    if premium:
        _, header, silent = STYLES[style]
    else:
        header, silent = "⏰ <b>Напоминание</b>", False
    body = f"{header}\n\n{esc(r['text'])}"
    if premium and r["note"]:
        body += f"\n\n📝 <i>{esc(r['note'])}</i>"
    if premium and STICKERS.get(style):
        try:
            await tg_retry(bot.send_sticker, r["user_id"], STICKERS[style], disable_notification=silent)
        except TelegramForbiddenError:
            raise
        except Exception as e:  # noqa: BLE001 — стикер не критичен
            log.warning("Стикер не отправлен: %s", e)
    await tg_retry(bot.send_message, r["user_id"], body, disable_notification=silent)


async def check_reminders(bot: Bot) -> None:
    """Запускается каждую минуту: находит напоминания, время которых наступило."""
    utc_now = datetime.now(timezone.utc)
    due_premium, due_free = [], []
    for r in await db.reminder_candidates():
        local = utc_now.astimezone(get_zone(r["tz"]))
        today = local.strftime("%Y-%m-%d")
        if r["last_sent_date"] == today:
            continue
        h, mi = map(int, r["remind_time"].split(":"))
        due_at = local.replace(hour=h, minute=mi, second=0, microsecond=0)
        delta = (local - due_at).total_seconds()
        if 0 <= delta <= LATE_WINDOW_SEC:
            (due_premium if is_premium(r["premium_until"]) else due_free).append((r, today))

    async def send_one(item, sem: asyncio.Semaphore, delay: float):
        r, today = item
        async with sem:
            try:
                await deliver(bot, r)
                await db.mark_sent(r["id"], today)
            except TelegramForbiddenError:
                await db.mark_blocked(r["user_id"])
            except Exception as e:  # noqa: BLE001
                log.warning("Не удалось отправить напоминание %s: %s", r["id"], e)
            await asyncio.sleep(delay)

    # Приоритетная доставка: Премиум — сначала и с высокой параллельностью
    if due_premium:
        sem = asyncio.Semaphore(20)
        await asyncio.gather(*(send_one(i, sem, 0.0) for i in due_premium))
    if due_free:
        sem = asyncio.Semaphore(5)
        await asyncio.gather(*(send_one(i, sem, 0.05) for i in due_free))
    if due_premium or due_free:
        log.info("Отправлено напоминаний: premium=%d free=%d", len(due_premium), len(due_free))


# ════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ════════════════════════════════════════════════════════════════════════════
async def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not BOT_TOKEN:
        raise SystemExit("Не задан BOT_TOKEN в .env")
    if not ADMIN_SECRET_KEY:
        log.warning("ADMIN_SECRET_KEY не задан — вход в админ-панель отключён")
    if not PROVIDER_TOKEN:
        log.warning("PAYMENT_PROVIDER_TOKEN не задан — оплата в рублях скрыта, доступны только Stars")
    try:
        ZoneInfo(DEFAULT_TZ)
    except ZoneInfoNotFoundError:
        raise SystemExit(f"Неизвестный DEFAULT_TIMEZONE: {DEFAULT_TZ}")

    await db.init()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(UserMiddleware())
    dp.callback_query.outer_middleware(UserMiddleware())
    dp.include_router(user_router)   # сначала пользовательский (меню сбрасывает состояния)
    dp.include_router(admin_router)

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        check_reminders, CronTrigger(second=0), args=[bot],
        id="reminders", max_instances=1, coalesce=True, misfire_grace_time=30,
    )
    scheduler.start()

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown(wait=False)
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
