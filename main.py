import ipaddress
import json
import logging
import os
import re
import secrets
import sqlite3
import time
import unicodedata
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# --- PATHS & CONFIG ---
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


def resolve_db_file() -> Path:
    raw_path = os.getenv("DB_PATH", "").strip()
    if not raw_path:
        legacy_path = BASE_DIR / "database.sqlite"
        if legacy_path.exists():
            return legacy_path
        return BASE_DIR / "data" / "database.sqlite"
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = BASE_DIR / candidate
    return candidate


DB_FILE = resolve_db_file()
DB_FILE.parent.mkdir(parents=True, exist_ok=True)
APP_BUILD_TAG = os.getenv("APP_BUILD_TAG", "paymentplatform-2026-03-10-log-v1").strip() or "paymentplatform-2026-03-10-log-v1"
APP_INSTANCE_ID = secrets.token_hex(4)
LOG_LEVEL_NAME = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("paymentplatform")


def _serialize_log_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _serialize_log_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize_log_value(item) for item in value]
    return str(value)


def runtime_log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    payload = {
        "event": event,
        "build_tag": APP_BUILD_TAG,
        "instance_id": APP_INSTANCE_ID,
        **{key: _serialize_log_value(value) for key, value in fields.items()},
    }
    logger.log(level, json.dumps(payload, ensure_ascii=False, sort_keys=True))


def parse_optional_int(raw_value: str, default: int | None = None) -> int | None:
    value = (raw_value or "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_admin_ids(*raw_values: str) -> set[int]:
    result: set[int] = set()
    for raw_value in raw_values:
        for chunk in (raw_value or "").replace(";", ",").split(","):
            admin_id = parse_optional_int(chunk)
            if admin_id is not None and admin_id > 0:
                result.add(admin_id)
    return result


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
INITIAL_ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_ID", ""), os.getenv("ADMIN_IDS", ""))
WEB_URL = os.getenv("WEB_URL", "http://localhost:8000").rstrip("/")
DEFAULT_CURRENCY = "EUR"
DEFAULT_REFRESH_MINUTES = max(1, parse_optional_int(os.getenv("DEFAULT_REFRESH_MINUTES", ""), 15) or 15)
DEFAULT_PREVIEW_AMOUNT = 250.0
ALLOW_PREVIEW_MODE = os.getenv("ALLOW_PREVIEW_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
CLIENT_NAME_MAX_LENGTH = 80

BOT_ENABLED = bool(BOT_TOKEN)
WAITING_GEO_SELECTION = 1
WAITING_REQUISITES = 2
WAITING_MANAGER_ACTION = 3
WAITING_MANAGER = 4
WAITING_LINK_AMOUNT = 5
WAITING_LINK_REQUISITES = 6
WAITING_LINK_MANAGER = 7
WAITING_LINK_LANGUAGE = 8
WAITING_LINK_LABEL = 9
WAITING_LINK_COMMENT = 10
MENU_BUTTONS_PATTERN = (
    r"^(🗺 Выбрать GEO|📊 GEO статус|📊 Активные реквизиты|📝 Реквизиты|🗂 История реквизитов|"
    r"🗑 Удалить реквизит|👤 Менеджер|👥 Права доступа|🔗 Ссылка на оплату|🛠 Админка|ℹ️ Помощь)$"
)
MENU_BUTTON_LABELS = {
    "🗺 Выбрать GEO",
    "📊 GEO статус",
    "📊 Активные реквизиты",
    "📝 Реквизиты",
    "🗂 История реквизитов",
    "🗑 Удалить реквизит",
    "👤 Менеджер",
    "👥 Права доступа",
    "🔗 Ссылка на оплату",
    "🛠 Админка",
    "ℹ️ Помощь",
}
MANAGER_KEEP_OPTION = "✅ Оставить текущего"
MANAGER_ADD_OPTION = "➕ Добавить нового"
MANAGER_EDIT_OPTION_LEGACY = "✏️ Изменить имя/ссылку"

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()
ADMIN_AUTH_CONFIGURED = bool(ADMIN_USERNAME and ADMIN_PASSWORD)
SESSION_COOKIE_NAME = "payment_admin_session"
SESSION_TTL_HOURS = 12
DEFAULT_ADMIN_PANEL_URL = "https://paymentplatform-production-8de8.up.railway.app/admin"
ADMIN_PANEL_URL = os.getenv("ADMIN_PANEL_URL", DEFAULT_ADMIN_PANEL_URL).strip() or DEFAULT_ADMIN_PANEL_URL

ADMIN_SESSIONS: dict[str, datetime] = {}
GEO_CACHE: dict[str, dict[str, Any]] = {}
BOT_RUNTIME_STARTED = False
BOT_RUNTIME_ERROR: str | None = None
LOGIN_ATTEMPTS: dict[str, list[datetime]] = {}
LOGIN_WINDOW_MINUTES = 10
MAX_LOGIN_ATTEMPTS = 5

LANGUAGE_OPTIONS = [
    {"code": "en", "label": "English"},
    {"code": "es", "label": "Español"},
    {"code": "it", "label": "Italiano"},
    {"code": "de", "label": "Deutsch"},
    {"code": "fr", "label": "Français"},
]
LANDING_LANGUAGE_SET = {item["code"] for item in LANGUAGE_OPTIONS}
BOT_ROLE_OPTIONS = [
    {"code": "handler", "label": "Обработчик"},
    {"code": "processor", "label": "Процессор"},
    {"code": "admin", "label": "Админ"},
]
BOT_ROLE_SET = {item["code"] for item in BOT_ROLE_OPTIONS}
BOT_ROLE_LABELS = {item["code"]: item["label"] for item in BOT_ROLE_OPTIONS}
BOT_ROLE_PERMISSIONS = {
    "handler": {"select_geo", "view_geo", "create_link"},
    "processor": {"select_geo", "view_geo", "edit_requisites", "view_requisites_history", "delete_requisites"},
    "admin": {
        "select_geo",
        "view_geo",
        "edit_requisites",
        "view_requisites_history",
        "delete_requisites",
        "edit_manager",
        "manage_access",
        "create_link",
        "open_admin_panel",
    },
}
LANGUAGE_TO_GEO_MAP = {
    "es": "ES",
    "ca": "ES",
    "eu": "ES",
    "gl": "ES",
    "it": "IT",
    "de": "DE",
    "fr": "FR",
}
SPECIAL_LANGUAGE_MAP = {
    "nb": "no",
    "nn": "no",
}
DEFAULT_GEO_CONFIGS = {
    "ES": {
        "geo_name": "Spain",
        "default_language": "es",
        "manager_name": "Spain manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "IT": {
        "geo_name": "Italy",
        "default_language": "it",
        "manager_name": "Italy manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "DE": {
        "geo_name": "Germany",
        "default_language": "de",
        "manager_name": "Germany manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "FR": {
        "geo_name": "France",
        "default_language": "fr",
        "manager_name": "France manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
}
SUPPORTED_GEOS = tuple(DEFAULT_GEO_CONFIGS.keys())
SUPPORTED_GEO_SET = set(SUPPORTED_GEOS)


class AdminLoginPayload(BaseModel):
    username: str
    password: str


class GeoConfigPayload(BaseModel):
    geo_name: str
    default_language: str
    refresh_minutes: int
    default_manager_id: int | None = None


class RequisitesPayload(BaseModel):
    bank_name: str
    card_number: str
    bic_swift: str = ""
    receiver_name: str


class ManagerPayload(BaseModel):
    manager_id: int | None = None
    geo_code: str
    manager_name: str
    manager_telegram_url: str = ""
    make_default: bool = False


class LandingClientPayload(BaseModel):
    visit_token: str
    first_name: str
    last_name: str


class BotUserPayload(BaseModel):
    user_id: int
    role: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})")}
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def sanitize_geo_code(value: str | None) -> str | None:
    code = (value or "").strip().upper()
    return code if code in SUPPORTED_GEO_SET else None


def sanitize_language_code(value: str | None) -> str | None:
    code = (value or "").strip().lower()
    return code if code in LANDING_LANGUAGE_SET else None


def sanitize_bot_role(value: str | None, default: str = "handler") -> str:
    code = (value or "").strip().lower()
    return code if code in BOT_ROLE_SET else default


def get_bot_role_label(role: str | None) -> str:
    return BOT_ROLE_LABELS.get(sanitize_bot_role(role, "handler"), "Обработчик")


def bot_role_has_permission(role: str | None, permission: str) -> bool:
    safe_role = sanitize_bot_role(role, "handler")
    return permission in BOT_ROLE_PERMISSIONS.get(safe_role, set())


def normalize_language_code(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower().replace("_", "-")
    normalized = normalized.split(",")[0].split(";")[0]
    code = normalized.split("-")[0]
    code = SPECIAL_LANGUAGE_MAP.get(code, code)
    return code or None


def clean_client_name(raw_value: str | None) -> str:
    value = re.sub(r"\s+", " ", (raw_value or "").strip())
    return value[:CLIENT_NAME_MAX_LENGTH]


def sanitize_payment_label(raw_value: str | None) -> str:
    value = (raw_value or "").strip().lower()
    if not value:
        return ""
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value[:80]


def payment_label_has_only_latin(raw_value: str | None) -> bool:
    value = (raw_value or "").strip()
    if not value:
        return True
    return re.fullmatch(r"[A-Za-z0-9 _-]+", value) is not None


def normalize_manager_link(raw_value: str | None) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    if value == "-":
        return ""
    if value.startswith("@") and len(value) > 1:
        return f"https://t.me/{value[1:]}"
    if value.startswith("t.me/"):
        return f"https://{value}"
    return value


def has_manager_contact(geo_code: str, manager_id: int | None = None) -> bool:
    manager = resolve_manager_for_geo(geo_code, manager_id)
    return bool(normalize_manager_link(manager.get("manager_telegram_url")))


def clean_payment_comment(raw_value: str | None) -> str:
    value = re.sub(r"\s+", " ", (raw_value or "").strip())
    return value[:300]


def normalize_button_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", (value or ""))
    text = text.replace("\ufe0f", "")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def is_main_menu_text(value: str | None) -> bool:
    text = normalize_button_text(value)
    if not text:
        return False
    normalized_labels = {normalize_button_text(label) for label in MENU_BUTTON_LABELS}
    if text in normalized_labels:
        return True
    # Fallback on keywords in case Telegram sends emoji-variant text.
    menu_keywords = (
        "выбрать geo",
        "geo статус",
        "активные реквизиты",
        "реквизиты",
        "история реквизитов",
        "удалить реквизит",
        "менеджер",
        "права доступа",
        "ссылка на оплату",
        "админка",
        "помощь",
    )
    return any(keyword in text for keyword in menu_keywords)


def is_menu_button_text(text: str | None) -> bool:
    return is_main_menu_text(text)


async def handle_menu_interrupt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    if message is None:
        return False
    if not is_main_menu_text(message.text):
        return False
    user_id = update.effective_user.id if update.effective_user else None
    runtime_log(
        "menu_interrupt",
        user_id=user_id,
        role=get_bot_user_role(user_id),
        text=message.text,
        selected_geo=get_selected_geo(context, user_id),
    )
    await message.reply_text(
        "Текущий шаг отменен. Нажмите нужную кнопку еще раз.",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return True


def parse_payment_amount(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip().replace(",", ".")
    if not normalized:
        return None
    try:
        amount = float(normalized)
    except ValueError:
        return None
    if amount <= 0:
        return None
    return round(amount, 2)


def format_query_amount(amount: float) -> str:
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def build_payment_link(
    amount: float,
    geo_code: str,
    label: str = "",
    comment: str = "",
    forced_language: str | None = None,
    requisites_id: int | None = None,
    manager_id: int | None = None,
    manager_link_override: str | None = None,
) -> str:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    safe_manager_link = normalize_manager_link(manager_link_override)
    if not safe_manager_link and not has_manager_contact(safe_geo, manager_id):
        raise HTTPException(status_code=400, detail=f"Для GEO {safe_geo} не задан контакт менеджера")
    params = {
        "payment": format_query_amount(amount),
        "geo": safe_geo,
    }
    clean_label = sanitize_payment_label(label)
    clean_comment = clean_payment_comment(comment)
    safe_language = sanitize_language_code(forced_language)
    if clean_label:
        params["label"] = clean_label
    if clean_comment:
        params["comment"] = clean_comment
    if safe_language:
        params["lang"] = safe_language
    if requisites_id is not None and requisites_id > 0:
        params["req"] = str(requisites_id)
    if manager_id is not None and manager_id > 0:
        params["mgr"] = str(manager_id)
    if safe_manager_link:
        params["mgr_link"] = safe_manager_link
    link = f"{WEB_URL}/?{urlencode(params)}"
    runtime_log(
        "build_payment_link",
        geo_code=safe_geo,
        amount=amount,
        requisites_id=requisites_id,
        manager_id=manager_id,
        manager_link_override=safe_manager_link,
        label=clean_label,
        comment=clean_comment,
        language=safe_language,
        link=link,
    )
    return link


def build_admin_panel_link() -> str:
    return ADMIN_PANEL_URL


def resolve_geo_code(requested_geo: str | None, country_code: str | None, browser_language: str | None) -> str:
    explicit_geo = sanitize_geo_code(requested_geo)
    if explicit_geo:
        return explicit_geo

    visitor_country = (country_code or "").strip().upper()
    if visitor_country in SUPPORTED_GEO_SET:
        return visitor_country

    mapped_geo = LANGUAGE_TO_GEO_MAP.get((browser_language or "").strip().lower())
    if mapped_geo:
        return mapped_geo

    return "ES"


def resolve_recommended_language(
    explicit_language: str | None,
    browser_language: str | None,
    country_code: str | None,
    geo_default_language: str | None,
) -> str:
    forced_language = sanitize_language_code(explicit_language)
    if forced_language:
        return forced_language

    browser_safe = sanitize_language_code(browser_language)
    if browser_safe:
        return browser_safe

    country_geo = sanitize_geo_code(country_code)
    if country_geo:
        country_default = sanitize_language_code(get_geo_profile(country_geo).get("default_language"))
        if country_default:
            return country_default

    return sanitize_language_code(geo_default_language) or "en"


def legacy_seed_requisites(conn: sqlite3.Connection) -> dict[str, str]:
    if table_exists(conn, "requisites"):
        row = conn.execute(
            """
            SELECT bank_name, card_number, receiver_name
            FROM requisites
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return dict(row)
    return {
        "bank_name": "Dream Team Pay",
        "card_number": "0000 0000 0000 0000",
        "bic_swift": "",
        "receiver_name": "Payment Operations",
    }


def seed_geo_data(conn: sqlite3.Connection) -> None:
    now_value = utc_now_iso()
    default_requisites = legacy_seed_requisites(conn)
    for geo_code, config in DEFAULT_GEO_CONFIGS.items():
        conn.execute(
            """
            INSERT OR IGNORE INTO geo_profiles (
                geo_code, geo_name, default_language, manager_name, manager_telegram_url,
                default_manager_id, refresh_minutes, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                geo_code,
                config["geo_name"],
                config["default_language"],
                config["manager_name"],
                config["manager_telegram_url"],
                config["default_manager_id"],
                config["refresh_minutes"],
                now_value,
            ),
        )

        row = conn.execute(
            "SELECT COUNT(*) AS value FROM geo_requisites WHERE geo_code = ?",
            (geo_code,),
        ).fetchone()
        if row and row["value"] == 0:
            conn.execute(
                """
                INSERT INTO geo_requisites (geo_code, bank_name, card_number, bic_swift, receiver_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    geo_code,
                    default_requisites["bank_name"],
                    default_requisites["card_number"],
                    default_requisites["bic_swift"],
                    default_requisites["receiver_name"],
                    now_value,
                ),
            )


def migrate_legacy_geo_managers(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT geo_code, manager_name, manager_telegram_url, default_manager_id
        FROM geo_profiles
        """
    ).fetchall()
    for row in rows:
        geo_code = sanitize_geo_code(row["geo_code"]) or "ES"
        if row["default_manager_id"]:
            manager = conn.execute(
                """
                SELECT id
                FROM geo_managers
                WHERE id = ? AND geo_code = ?
                LIMIT 1
                """,
                (row["default_manager_id"], geo_code),
            ).fetchone()
            if manager is not None:
                continue

        manager_name = (row["manager_name"] or "").strip()
        manager_url = normalize_manager_link(row["manager_telegram_url"])
        if not manager_name and not manager_url:
            continue

        existing = conn.execute(
            """
            SELECT id
            FROM geo_managers
            WHERE geo_code = ? AND manager_name = ? AND COALESCE(manager_telegram_url, '') = ?
            LIMIT 1
            """,
            (geo_code, manager_name, manager_url),
        ).fetchone()
        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO geo_managers (geo_code, manager_name, manager_telegram_url, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (geo_code, manager_name or f"{geo_code} manager", manager_url, utc_now_iso(), utc_now_iso()),
            )
            manager_id = int(cursor.lastrowid)
        else:
            manager_id = int(existing["id"])

        conn.execute(
            """
            UPDATE geo_profiles
            SET default_manager_id = ?, updated_at = COALESCE(updated_at, ?)
            WHERE geo_code = ?
            """,
            (manager_id, utc_now_iso(), geo_code),
        )


def seed_bot_admins(conn: sqlite3.Connection) -> None:
    now_value = utc_now_iso()
    for admin_id in sorted(INITIAL_ADMIN_IDS):
        conn.execute(
            """
            INSERT INTO bot_admins (user_id, username, full_name, role, added_at, added_by)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET role = excluded.role
            """,
            (admin_id, "", "", "admin", now_value, admin_id),
        )


def init_db() -> None:
    conn = get_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_admins (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            role TEXT NOT NULL DEFAULT 'admin',
            added_at TEXT,
            added_by INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_admin_preferences (
            user_id INTEGER PRIMARY KEY,
            selected_geo TEXT,
            selected_manager_id INTEGER,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_profiles (
            geo_code TEXT PRIMARY KEY,
            geo_name TEXT NOT NULL,
            default_language TEXT NOT NULL,
            manager_name TEXT NOT NULL,
            manager_telegram_url TEXT,
            default_manager_id INTEGER,
            refresh_minutes INTEGER NOT NULL DEFAULT 15,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_managers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            geo_code TEXT NOT NULL,
            manager_name TEXT NOT NULL,
            manager_telegram_url TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_geo_managers_lookup
        ON geo_managers (geo_code, id DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_requisites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            geo_code TEXT NOT NULL,
            bank_name TEXT NOT NULL,
            card_number TEXT NOT NULL,
            bic_swift TEXT DEFAULT '',
            receiver_name TEXT NOT NULL,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_geo_requisites_lookup
        ON geo_requisites (geo_code, id DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode TEXT,
            ip_address TEXT,
            country_code TEXT,
            country_name TEXT,
            city TEXT,
            region TEXT,
            timezone TEXT,
            currency TEXT,
            user_agent TEXT,
            accept_language TEXT,
            recommended_language TEXT,
            geo_code TEXT,
            payment_amount REAL,
            payment_label TEXT,
            referrer TEXT,
            page_path TEXT,
            query_string TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER,
            actor_role TEXT,
            action_type TEXT NOT NULL,
            geo_code TEXT,
            target_user_id INTEGER,
            payload TEXT,
            created_at TEXT
        )
        """
    )

    ensure_column(conn, "visits", "geo_code", "TEXT")
    ensure_column(conn, "visits", "payment_amount", "REAL")
    ensure_column(conn, "visits", "payment_label", "TEXT")
    ensure_column(conn, "visits", "visit_token", "TEXT")
    ensure_column(conn, "visits", "client_first_name", "TEXT")
    ensure_column(conn, "visits", "client_last_name", "TEXT")
    ensure_column(conn, "visits", "client_saved_at", "TEXT")
    ensure_column(conn, "visits", "requisites_id", "INTEGER")
    ensure_column(conn, "visits", "manager_id", "INTEGER")
    ensure_column(conn, "visits", "snapshot_manager_name", "TEXT")
    ensure_column(conn, "visits", "snapshot_manager_telegram_url", "TEXT")
    ensure_column(conn, "visits", "snapshot_bank_name", "TEXT")
    ensure_column(conn, "visits", "snapshot_card_number", "TEXT")
    ensure_column(conn, "visits", "snapshot_bic_swift", "TEXT")
    ensure_column(conn, "visits", "snapshot_receiver_name", "TEXT")
    ensure_column(conn, "visits", "payment_comment", "TEXT")
    ensure_column(conn, "bot_admins", "role", "TEXT NOT NULL DEFAULT 'admin'")
    ensure_column(conn, "bot_admin_preferences", "selected_manager_id", "INTEGER")
    ensure_column(conn, "geo_requisites", "bic_swift", "TEXT DEFAULT ''")
    ensure_column(conn, "geo_profiles", "default_manager_id", "INTEGER")
    ensure_column(conn, "bot_activity_log", "actor_role", "TEXT")
    ensure_column(conn, "bot_activity_log", "geo_code", "TEXT")
    ensure_column(conn, "bot_activity_log", "target_user_id", "INTEGER")
    ensure_column(conn, "bot_activity_log", "payload", "TEXT")
    ensure_column(conn, "bot_activity_log", "created_at", "TEXT")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_visits_visit_token
        ON visits (visit_token)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bot_activity_actor
        ON bot_activity_log (actor_user_id, id DESC)
        """
    )
    seed_bot_admins(conn)
    seed_geo_data(conn)
    migrate_legacy_geo_managers(conn)
    conn.commit()
    conn.close()


def get_geo_profile(geo_code: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            geo_code, geo_name, default_language, manager_name, manager_telegram_url,
            default_manager_id, refresh_minutes, updated_at
        FROM geo_profiles
        WHERE geo_code = ?
        """,
        (safe_geo,),
    ).fetchone()
    conn.close()
    if row:
        return dict(row)

    fallback = DEFAULT_GEO_CONFIGS[safe_geo]
    return {
        "geo_code": safe_geo,
        "geo_name": fallback["geo_name"],
        "default_language": fallback["default_language"],
        "manager_name": fallback["manager_name"],
        "manager_telegram_url": fallback["manager_telegram_url"],
        "default_manager_id": fallback["default_manager_id"],
        "refresh_minutes": fallback["refresh_minutes"],
        "updated_at": utc_now_iso(),
    }


def list_geo_profiles() -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            geo_code, geo_name, default_language, manager_name, manager_telegram_url,
            default_manager_id, refresh_minutes, updated_at
        FROM geo_profiles
        ORDER BY geo_code ASC
        """
    ).fetchall()
    conn.close()
    profiles_by_code = {row["geo_code"]: dict(row) for row in rows}
    return [profiles_by_code.get(geo_code, get_geo_profile(geo_code)) for geo_code in SUPPORTED_GEOS]


def get_geo_manager_by_id(geo_code: str, manager_id: int | None) -> dict[str, Any] | None:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    if manager_id is None or manager_id <= 0:
        return None
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, geo_code, manager_name, manager_telegram_url, created_at, updated_at
        FROM geo_managers
        WHERE geo_code = ? AND id = ?
        LIMIT 1
        """,
        (safe_geo, manager_id),
    ).fetchone()
    conn.close()
    return dict(row) if row is not None else None


def list_geo_managers(geo_code: str | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    if geo_code:
        safe_geo = sanitize_geo_code(geo_code) or "ES"
        rows = conn.execute(
            """
            SELECT id, geo_code, manager_name, manager_telegram_url, created_at, updated_at
            FROM geo_managers
            WHERE geo_code = ?
            ORDER BY id DESC
            """,
            (safe_geo,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, geo_code, manager_name, manager_telegram_url, created_at, updated_at
            FROM geo_managers
            ORDER BY geo_code ASC, id DESC
            """
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_default_manager_for_geo(geo_code: str) -> dict[str, Any] | None:
    profile = get_geo_profile(geo_code)
    manager_id = profile.get("default_manager_id")
    manager = get_geo_manager_by_id(geo_code, int(manager_id)) if manager_id else None
    if manager is not None:
        return manager
    managers = list_geo_managers(geo_code)
    return managers[0] if managers else None


def resolve_manager_for_geo(geo_code: str, manager_id: int | None = None) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    chosen = get_geo_manager_by_id(safe_geo, manager_id)
    if chosen is not None:
        return chosen
    fallback = get_default_manager_for_geo(safe_geo)
    if fallback is not None:
        return fallback
    return {
        "id": None,
        "geo_code": safe_geo,
        "manager_name": "",
        "manager_telegram_url": "",
        "created_at": None,
        "updated_at": None,
    }


def set_geo_default_manager(geo_code: str, manager_id: int | None) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    manager = get_geo_manager_by_id(safe_geo, manager_id) if manager_id else None
    conn = get_connection()
    conn.execute(
        """
        UPDATE geo_profiles
        SET default_manager_id = ?, updated_at = ?
        WHERE geo_code = ?
        """,
        (manager["id"] if manager else None, utc_now_iso(), safe_geo),
    )
    conn.commit()
    conn.close()
    return get_geo_profile(safe_geo)


def save_geo_manager(payload: ManagerPayload) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(payload.geo_code)
    if not safe_geo:
        raise HTTPException(status_code=400, detail="Неподдерживаемый GEO для менеджера")

    clean_name = payload.manager_name.strip()
    clean_url = normalize_manager_link(payload.manager_telegram_url)
    if not clean_name:
        raise HTTPException(status_code=400, detail="Имя менеджера обязательно")
    if not clean_url:
        raise HTTPException(status_code=400, detail="Нужна Telegram-ссылка менеджера")

    manager_id = payload.manager_id if payload.manager_id and payload.manager_id > 0 else None
    runtime_log(
        "save_geo_manager_started",
        geo_code=safe_geo,
        incoming_manager_id=manager_id,
        make_default=payload.make_default,
        manager_name=clean_name,
        manager_url=clean_url,
    )
    conn = get_connection()
    if manager_id:
        existing = conn.execute(
            """
            SELECT id
            FROM geo_managers
            WHERE id = ? AND geo_code = ?
            LIMIT 1
            """,
            (manager_id, safe_geo),
        ).fetchone()
        if existing is None:
            conn.close()
            raise HTTPException(status_code=404, detail="Менеджер не найден")
        conn.execute(
            """
            UPDATE geo_managers
            SET manager_name = ?, manager_telegram_url = ?, updated_at = ?
            WHERE id = ? AND geo_code = ?
            """,
            (clean_name, clean_url, utc_now_iso(), manager_id, safe_geo),
        )
        saved_manager_id = manager_id
    else:
        cursor = conn.execute(
            """
            INSERT INTO geo_managers (geo_code, manager_name, manager_telegram_url, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (safe_geo, clean_name, clean_url, utc_now_iso(), utc_now_iso()),
        )
        saved_manager_id = int(cursor.lastrowid)

    current_default = conn.execute(
        """
        SELECT default_manager_id
        FROM geo_profiles
        WHERE geo_code = ?
        LIMIT 1
        """,
        (safe_geo,),
    ).fetchone()
    if payload.make_default or not (current_default and current_default["default_manager_id"]):
        conn.execute(
            """
            UPDATE geo_profiles
            SET default_manager_id = ?, updated_at = ?
            WHERE geo_code = ?
            """,
            (saved_manager_id, utc_now_iso(), safe_geo),
        )
    conn.commit()
    conn.close()
    runtime_log(
        "save_geo_manager_completed",
        geo_code=safe_geo,
        saved_manager_id=saved_manager_id,
        make_default=payload.make_default,
    )
    return resolve_manager_for_geo(safe_geo, saved_manager_id)


def get_active_requisites(geo_code: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, geo_code, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        WHERE geo_code = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (safe_geo,),
    ).fetchone()
    conn.close()
    if row:
        return dict(row)

    fallback_conn = get_connection()
    defaults = legacy_seed_requisites(fallback_conn)
    fallback_conn.close()
    return {
        "id": None,
        "geo_code": safe_geo,
        "bank_name": defaults["bank_name"],
        "card_number": defaults["card_number"],
        "bic_swift": defaults["bic_swift"],
        "receiver_name": defaults["receiver_name"],
        "created_at": utc_now_iso(),
    }


def get_geo_requisites_by_id(geo_code: str, requisites_id: int | None) -> dict[str, Any] | None:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    if requisites_id is None or requisites_id <= 0:
        return None
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, geo_code, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        WHERE geo_code = ? AND id = ?
        LIMIT 1
        """,
        (safe_geo, requisites_id),
    ).fetchone()
    conn.close()
    return dict(row) if row is not None else None


def resolve_requisites_for_geo(geo_code: str, requisites_id: int | None = None) -> dict[str, Any]:
    chosen = get_geo_requisites_by_id(geo_code, requisites_id)
    if chosen is not None:
        return chosen
    return get_active_requisites(geo_code)


def list_geo_snapshots() -> list[dict[str, Any]]:
    return [
        {
            "profile": get_geo_profile(geo_code),
            "active_requisites": get_active_requisites(geo_code),
            "default_manager": get_default_manager_for_geo(geo_code),
            "managers": list_geo_managers(geo_code),
        }
        for geo_code in SUPPORTED_GEOS
    ]


def list_geo_requisites_history(limit: int = 40) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, geo_code, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def list_geo_requisites_history_for_geo(geo_code: str, limit: int = 8) -> list[dict[str, Any]]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, geo_code, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        WHERE geo_code = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (safe_geo, limit),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def save_geo_configuration(geo_code: str, payload: GeoConfigPayload) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code)
    if not safe_geo:
        raise HTTPException(status_code=400, detail="Неподдерживаемый GEO")

    geo_name = payload.geo_name.strip() or DEFAULT_GEO_CONFIGS[safe_geo]["geo_name"]
    default_language = sanitize_language_code(payload.default_language) or DEFAULT_GEO_CONFIGS[safe_geo]["default_language"]
    refresh_minutes = int(payload.refresh_minutes)
    if refresh_minutes < 1 or refresh_minutes > 120:
        raise HTTPException(status_code=400, detail="Таймер должен быть от 1 до 120 минут")
    default_manager_id = payload.default_manager_id if payload.default_manager_id and payload.default_manager_id > 0 else None
    if default_manager_id and get_geo_manager_by_id(safe_geo, default_manager_id) is None:
        raise HTTPException(status_code=400, detail="Менеджер по умолчанию не найден для выбранного GEO")

    conn = get_connection()
    conn.execute(
        """
        UPDATE geo_profiles
        SET
            geo_name = ?,
            default_language = ?,
            default_manager_id = ?,
            refresh_minutes = ?,
            updated_at = ?
        WHERE geo_code = ?
        """,
        (
            geo_name,
            default_language,
            default_manager_id,
            refresh_minutes,
            utc_now_iso(),
            safe_geo,
        ),
    )
    conn.commit()
    conn.close()
    return {
        "profile": get_geo_profile(safe_geo),
        "active_requisites": get_active_requisites(safe_geo),
        "default_manager": get_default_manager_for_geo(safe_geo),
        "managers": list_geo_managers(safe_geo),
    }


def update_geo_requisites(
    geo_code: str,
    bank_name: str,
    card_number: str,
    bic_swift: str,
    receiver_name: str,
) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    clean_bank = bank_name.strip()
    clean_card = card_number.strip()
    clean_bic_swift = bic_swift.strip()
    clean_receiver = receiver_name.strip()
    if not clean_bank or not clean_card or not clean_receiver:
        raise HTTPException(status_code=400, detail="Нужно указать банк, IBAN и получателя")

    runtime_log(
        "update_geo_requisites_started",
        geo_code=safe_geo,
        bank_name=clean_bank,
        card_number=clean_card,
        bic_swift=clean_bic_swift,
        receiver_name=clean_receiver,
    )
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO geo_requisites (geo_code, bank_name, card_number, bic_swift, receiver_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (safe_geo, clean_bank, clean_card, clean_bic_swift, clean_receiver, utc_now_iso()),
    )
    conn.commit()
    conn.close()
    active_requisites = get_active_requisites(safe_geo)
    runtime_log(
        "update_geo_requisites_completed",
        geo_code=safe_geo,
        active_requisites_id=active_requisites.get("id"),
    )
    return active_requisites


def update_geo_manager(geo_code: str, manager_name: str, manager_telegram_url: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    default_manager = get_default_manager_for_geo(safe_geo)
    saved_manager = save_geo_manager(
        ManagerPayload(
            manager_id=default_manager["id"] if default_manager and default_manager.get("id") else None,
            geo_code=safe_geo,
            manager_name=manager_name.strip() or DEFAULT_GEO_CONFIGS[safe_geo]["manager_name"],
            manager_telegram_url=manager_telegram_url,
            make_default=True,
        )
    )
    return saved_manager


def record_visit(
    mode: str,
    visitor: dict[str, Any],
    recommended_language: str,
    geo_code: str,
    payment_amount: float | None,
    payment_label: str | None,
    payment_comment: str | None,
    manager: dict[str, Any] | None,
    requisites: dict[str, Any] | None,
    request: Request,
) -> str:
    visit_token = secrets.token_urlsafe(18)
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO visits (
            mode, ip_address, country_code, country_name, city, region, timezone, currency,
            user_agent, accept_language, recommended_language, geo_code, payment_amount,
            payment_label, referrer, page_path, query_string, created_at, visit_token,
            client_first_name, client_last_name, client_saved_at, requisites_id, manager_id,
            snapshot_manager_name, snapshot_manager_telegram_url,
            snapshot_bank_name, snapshot_card_number, snapshot_bic_swift, snapshot_receiver_name, payment_comment
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mode,
            visitor.get("ip_address"),
            visitor.get("country_code"),
            visitor.get("country_name"),
            visitor.get("city"),
            visitor.get("region"),
            visitor.get("timezone"),
            visitor.get("currency"),
            visitor.get("user_agent"),
            visitor.get("accept_language"),
            recommended_language,
            geo_code,
            payment_amount,
            payment_label or None,
            visitor.get("referrer"),
            str(request.url.path),
            request.url.query,
            utc_now_iso(),
            visit_token,
            None,
            None,
            None,
            requisites.get("id") if requisites else None,
            manager.get("id") if manager else None,
            manager.get("manager_name") if manager else None,
            manager.get("manager_telegram_url") if manager else None,
            requisites.get("bank_name") if requisites else None,
            requisites.get("card_number") if requisites else None,
            requisites.get("bic_swift") if requisites else None,
            requisites.get("receiver_name") if requisites else None,
            payment_comment or None,
        ),
    )
    conn.commit()
    conn.close()
    return visit_token


def list_visits(limit: int = 120) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            id, mode, ip_address, country_code, country_name, city, region, timezone,
            currency, user_agent, accept_language, recommended_language, geo_code,
            payment_amount, payment_label, referrer, page_path, query_string, created_at,
            visit_token, client_first_name, client_last_name, client_saved_at, manager_id,
            snapshot_manager_name, snapshot_manager_telegram_url, requisites_id,
            snapshot_bank_name, snapshot_card_number, snapshot_bic_swift, snapshot_receiver_name,
            payment_comment
        FROM visits
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def list_bot_admins() -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id, username, full_name, role, added_at, added_by
        FROM bot_admins
        ORDER BY COALESCE(full_name, ''), user_id
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def log_bot_activity(
    actor_user_id: int | None,
    action_type: str,
    geo_code: str | None = None,
    target_user_id: int | None = None,
    payload: str = "",
) -> None:
    runtime_log(
        "bot_activity",
        actor_user_id=actor_user_id,
        actor_role=get_bot_user_role(actor_user_id),
        action_type=action_type,
        geo_code=sanitize_geo_code(geo_code),
        target_user_id=target_user_id,
        payload=payload[:200],
    )
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO bot_activity_log (
            actor_user_id, actor_role, action_type, geo_code, target_user_id, payload, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            actor_user_id,
            get_bot_user_role(actor_user_id),
            action_type,
            sanitize_geo_code(geo_code),
            target_user_id,
            payload[:500],
            utc_now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def list_bot_activity(limit: int = 200) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            log.id,
            log.actor_user_id,
            log.actor_role,
            log.action_type,
            log.geo_code,
            log.target_user_id,
            log.payload,
            log.created_at,
            admin.username AS actor_username,
            admin.full_name AS actor_full_name
        FROM bot_activity_log AS log
        LEFT JOIN bot_admins AS admin
            ON admin.user_id = log.actor_user_id
        ORDER BY log.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_worker_stats() -> list[dict[str, Any]]:
    users = list_bot_admins()
    activity = list_bot_activity(limit=1000)
    stats_by_user: dict[int, dict[str, Any]] = {}
    for user in users:
        user_id = int(user["user_id"])
        stats_by_user[user_id] = {
            **user,
            "telegram_url": f"https://t.me/{user['username']}" if user.get("username") else "",
            "actions_total": 0,
            "links_created": 0,
            "requisites_updated": 0,
            "requisites_restored": 0,
            "requisites_deleted": 0,
            "manager_updates": 0,
            "last_action_at": "",
            "last_action_type": "",
        }

    for item in activity:
        actor_user_id = item.get("actor_user_id")
        if actor_user_id is None or actor_user_id not in stats_by_user:
            continue
        stats = stats_by_user[actor_user_id]
        action_type = item.get("action_type") or ""
        stats["actions_total"] += 1
        if not stats["last_action_at"]:
            stats["last_action_at"] = item.get("created_at") or ""
            stats["last_action_type"] = action_type
        if action_type == "create_link":
            stats["links_created"] += 1
        elif action_type == "update_requisites":
            stats["requisites_updated"] += 1
        elif action_type == "restore_requisites":
            stats["requisites_restored"] += 1
        elif action_type == "delete_requisites":
            stats["requisites_deleted"] += 1
        elif action_type == "update_manager":
            stats["manager_updates"] += 1

    return sorted(
        stats_by_user.values(),
        key=lambda item: (-int(item["actions_total"]), item.get("full_name") or "", int(item["user_id"])),
    )


def get_bot_user_role(user_id: int | None) -> str | None:
    if user_id is None:
        return None
    conn = get_connection()
    row = conn.execute(
        "SELECT role FROM bot_admins WHERE user_id = ? LIMIT 1",
        (user_id,),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return sanitize_bot_role(row["role"], "admin")


def is_bot_admin(user_id: int | None) -> bool:
    return get_bot_user_role(user_id) == "admin"


def has_bot_access(user_id: int | None) -> bool:
    return get_bot_user_role(user_id) is not None


def upsert_bot_admin_identity(user: Any) -> None:
    user_id = getattr(user, "id", None)
    if user_id is None or not has_bot_access(user_id):
        return
    username = getattr(user, "username", None) or ""
    full_name = getattr(user, "full_name", None) or getattr(user, "name", None) or ""
    conn = get_connection()
    conn.execute(
        """
        UPDATE bot_admins
        SET username = ?, full_name = ?
        WHERE user_id = ?
        """,
        (username, full_name, user_id),
    )
    conn.commit()
    conn.close()


def add_bot_admin(actor_id: int, target_user_id: int) -> bool:
    now_value = utc_now_iso()
    conn = get_connection()
    existing = conn.execute(
        "SELECT user_id FROM bot_admins WHERE user_id = ? LIMIT 1",
        (target_user_id,),
    ).fetchone()
    if existing is not None:
        conn.close()
        return False
    conn.execute(
        """
        INSERT INTO bot_admins (user_id, username, full_name, role, added_at, added_by)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (target_user_id, "", "", "admin", now_value, actor_id),
    )
    conn.commit()
    conn.close()
    return True


def save_bot_user_role(actor_id: int, target_user_id: int, role: str) -> dict[str, Any]:
    safe_role = sanitize_bot_role(role, "handler")
    now_value = utc_now_iso()
    conn = get_connection()
    existing = conn.execute(
        "SELECT user_id, role FROM bot_admins WHERE user_id = ? LIMIT 1",
        (target_user_id,),
    ).fetchone()
    if existing is not None and existing["role"] == "admin" and safe_role != "admin":
        count_row = conn.execute("SELECT COUNT(*) AS value FROM bot_admins WHERE role = 'admin'").fetchone()
        admins_total = int(count_row["value"]) if count_row else 0
        if admins_total <= 1:
            conn.close()
            raise HTTPException(status_code=400, detail="Нельзя снять роль у последнего администратора")
    if existing is None:
        conn.execute(
            """
            INSERT INTO bot_admins (user_id, username, full_name, role, added_at, added_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (target_user_id, "", "", safe_role, now_value, actor_id),
        )
    else:
        conn.execute(
            """
            UPDATE bot_admins
            SET role = ?
            WHERE user_id = ?
            """,
            (safe_role, target_user_id),
        )
    conn.commit()
    row = conn.execute(
        """
        SELECT user_id, username, full_name, role, added_at, added_by
        FROM bot_admins
        WHERE user_id = ?
        LIMIT 1
        """,
        (target_user_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row is not None else {
        "user_id": target_user_id,
        "username": "",
        "full_name": "",
        "role": safe_role,
        "added_at": now_value,
        "added_by": actor_id,
    }


def remove_bot_admin(target_user_id: int) -> tuple[bool, str]:
    conn = get_connection()
    count_row = conn.execute("SELECT COUNT(*) AS value FROM bot_admins WHERE role = 'admin'").fetchone()
    admins_total = int(count_row["value"]) if count_row else 0
    existing = conn.execute(
        "SELECT user_id, role FROM bot_admins WHERE user_id = ? LIMIT 1",
        (target_user_id,),
    ).fetchone()
    if existing is None:
        conn.close()
        return False, "Админ с таким ID не найден."
    if existing["role"] == "admin" and admins_total <= 1:
        conn.close()
        return False, "Нельзя удалить последнего администратора."
    conn.execute("DELETE FROM bot_admins WHERE user_id = ?", (target_user_id,))
    conn.commit()
    conn.close()
    return True, "Администратор удален."


def get_bot_admin_selected_geo(user_id: int | None) -> str | None:
    if user_id is None:
        return None
    conn = get_connection()
    row = conn.execute(
        """
        SELECT selected_geo
        FROM bot_admin_preferences
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return sanitize_geo_code(row["selected_geo"])


def set_bot_admin_selected_geo(user_id: int | None, geo_code: str) -> None:
    safe_geo = sanitize_geo_code(geo_code)
    if user_id is None or not safe_geo:
        return
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO bot_admin_preferences (user_id, selected_geo, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            selected_geo = excluded.selected_geo,
            updated_at = excluded.updated_at
        """,
        (user_id, safe_geo, utc_now_iso()),
    )
    conn.commit()
    conn.close()


def restore_geo_requisites_from_history(geo_code: str, history_id: int) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    runtime_log("restore_geo_requisites_started", geo_code=safe_geo, history_id=history_id)
    conn = get_connection()
    row = conn.execute(
        """
        SELECT bank_name, card_number, bic_swift, receiver_name
        FROM geo_requisites
        WHERE id = ? AND geo_code = ?
        LIMIT 1
        """,
        (history_id, safe_geo),
    ).fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Реквизиты с таким ID для выбранного GEO не найдены")

    conn.execute(
        """
        INSERT INTO geo_requisites (geo_code, bank_name, card_number, bic_swift, receiver_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (safe_geo, row["bank_name"], row["card_number"], row["bic_swift"], row["receiver_name"], utc_now_iso()),
    )
    conn.commit()
    conn.close()
    active_requisites = get_active_requisites(safe_geo)
    runtime_log(
        "restore_geo_requisites_completed",
        geo_code=safe_geo,
        history_id=history_id,
        active_requisites_id=active_requisites.get("id"),
    )
    return active_requisites


def delete_geo_requisites_history_item(geo_code: str, history_id: int) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    runtime_log("delete_geo_requisites_started", geo_code=safe_geo, history_id=history_id)
    conn = get_connection()
    existing = conn.execute(
        """
        SELECT id
        FROM geo_requisites
        WHERE id = ? AND geo_code = ?
        LIMIT 1
        """,
        (history_id, safe_geo),
    ).fetchone()
    if existing is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Реквизиты с таким ID для выбранного GEO не найдены")

    total_row = conn.execute(
        "SELECT COUNT(*) AS value FROM geo_requisites WHERE geo_code = ?",
        (safe_geo,),
    ).fetchone()
    total_count = int(total_row["value"]) if total_row else 0
    if total_count <= 1:
        conn.close()
        raise HTTPException(status_code=400, detail="Нельзя удалить последний комплект реквизитов для GEO")

    conn.execute("DELETE FROM geo_requisites WHERE id = ? AND geo_code = ?", (history_id, safe_geo))
    conn.commit()
    conn.close()
    runtime_log("delete_geo_requisites_completed", geo_code=safe_geo, history_id=history_id)
    return {
        "deleted_id": history_id,
        "active_requisites": get_active_requisites(safe_geo),
        "history": list_geo_requisites_history_for_geo(safe_geo),
    }


def parse_admin_ids_from_args(args: list[str]) -> list[int]:
    if not args:
        return []
    combined = ",".join(args)
    return sorted(parse_admin_ids(combined))


def save_landing_client(visit_token: str, first_name: str, last_name: str) -> dict[str, Any]:
    clean_token = (visit_token or "").strip()
    clean_first_name = clean_client_name(first_name)
    clean_last_name = clean_client_name(last_name)

    if not clean_token:
        raise HTTPException(status_code=400, detail="Отсутствует токен визита")
    if not clean_first_name or not clean_last_name:
        raise HTTPException(status_code=400, detail="Имя и фамилия обязательны")

    conn = get_connection()
    cursor = conn.execute(
        """
        UPDATE visits
        SET client_first_name = ?, client_last_name = ?, client_saved_at = ?
        WHERE visit_token = ?
        """,
        (clean_first_name, clean_last_name, utc_now_iso(), clean_token),
    )
    if cursor.rowcount == 0:
        conn.close()
        raise HTTPException(status_code=404, detail="Визит не найден")

    row = conn.execute(
        """
        SELECT visit_token, client_first_name, client_last_name, client_saved_at
        FROM visits
        WHERE visit_token = ?
        LIMIT 1
        """,
        (clean_token,),
    ).fetchone()
    conn.commit()
    conn.close()
    return dict(row) if row is not None else {
        "visit_token": clean_token,
        "client_first_name": clean_first_name,
        "client_last_name": clean_last_name,
        "client_saved_at": utc_now_iso(),
    }


def get_summary_stats() -> dict[str, Any]:
    conn = get_connection()
    today_start = datetime.now(timezone.utc).date().isoformat()

    visits_total = conn.execute("SELECT COUNT(*) AS value FROM visits").fetchone()["value"]
    visits_today = conn.execute(
        "SELECT COUNT(*) AS value FROM visits WHERE created_at >= ?",
        (today_start,),
    ).fetchone()["value"]
    unique_ips = conn.execute(
        "SELECT COUNT(DISTINCT ip_address) AS value FROM visits WHERE ip_address IS NOT NULL AND ip_address != ''"
    ).fetchone()["value"]
    preview_visits = conn.execute("SELECT COUNT(*) AS value FROM visits WHERE mode = 'preview'").fetchone()["value"]
    live_visits = conn.execute("SELECT COUNT(*) AS value FROM visits WHERE mode = 'live'").fetchone()["value"]
    configured_geos = conn.execute("SELECT COUNT(*) AS value FROM geo_profiles").fetchone()["value"]
    manager_links_configured = conn.execute(
        """
        SELECT COUNT(*) AS value
        FROM geo_profiles gp
        JOIN geo_managers gm ON gm.id = gp.default_manager_id
        WHERE gm.manager_telegram_url IS NOT NULL AND TRIM(gm.manager_telegram_url) != ''
        """
    ).fetchone()["value"]

    top_countries_rows = conn.execute(
        """
        SELECT COALESCE(country_name, country_code, 'Unknown') AS label, COUNT(*) AS count_value
        FROM visits
        GROUP BY COALESCE(country_name, country_code, 'Unknown')
        ORDER BY count_value DESC
        LIMIT 8
        """
    ).fetchall()
    top_languages_rows = conn.execute(
        """
        SELECT COALESCE(recommended_language, 'unknown') AS label, COUNT(*) AS count_value
        FROM visits
        GROUP BY COALESCE(recommended_language, 'unknown')
        ORDER BY count_value DESC
        LIMIT 8
        """
    ).fetchall()
    top_geos_rows = conn.execute(
        """
        SELECT COALESCE(geo_code, 'Unknown') AS label, COUNT(*) AS count_value
        FROM visits
        GROUP BY COALESCE(geo_code, 'Unknown')
        ORDER BY count_value DESC
        LIMIT 8
        """
    ).fetchall()
    conn.close()

    return {
        "visits_total": visits_total,
        "visits_today": visits_today,
        "unique_ips": unique_ips,
        "preview_visits": preview_visits,
        "live_visits": live_visits,
        "configured_geos": configured_geos,
        "manager_links_configured": manager_links_configured,
        "top_countries": [{"label": row["label"], "count": row["count_value"]} for row in top_countries_rows],
        "top_languages": [{"label": row["label"], "count": row["count_value"]} for row in top_languages_rows],
        "top_geos": [{"label": row["label"], "count": row["count_value"]} for row in top_geos_rows],
    }


def cleanup_sessions() -> None:
    now_value = utc_now()
    expired_tokens = [
        token
        for token, created_at in ADMIN_SESSIONS.items()
        if created_at + timedelta(hours=SESSION_TTL_HOURS) < now_value
    ]
    for token in expired_tokens:
        ADMIN_SESSIONS.pop(token, None)


def cleanup_login_attempts() -> None:
    cutoff = utc_now() - timedelta(minutes=LOGIN_WINDOW_MINUTES)
    expired_ips = []
    for ip_address, attempts in LOGIN_ATTEMPTS.items():
        fresh_attempts = [item for item in attempts if item >= cutoff]
        if fresh_attempts:
            LOGIN_ATTEMPTS[ip_address] = fresh_attempts
        else:
            expired_ips.append(ip_address)
    for ip_address in expired_ips:
        LOGIN_ATTEMPTS.pop(ip_address, None)


def register_failed_login(ip_address: str) -> None:
    cleanup_login_attempts()
    LOGIN_ATTEMPTS.setdefault(ip_address, []).append(utc_now())


def clear_failed_logins(ip_address: str) -> None:
    LOGIN_ATTEMPTS.pop(ip_address, None)


def ensure_login_not_rate_limited(ip_address: str) -> None:
    cleanup_login_attempts()
    attempts = LOGIN_ATTEMPTS.get(ip_address, [])
    if len(attempts) >= MAX_LOGIN_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Слишком много попыток входа. Попробуйте позже.")


def url_origin(raw_url: str | None) -> str:
    if not raw_url:
        return ""
    parsed = urlparse(raw_url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}".lower()


def allowed_admin_origins() -> set[str]:
    return {
        origin
        for origin in {
            url_origin(WEB_URL),
            url_origin(ADMIN_PANEL_URL),
            "http://127.0.0.1:8000",
            "http://localhost:8000",
        }
        if origin
    }


def get_request_origin(request: Request) -> str:
    return url_origin(request.headers.get("origin") or request.headers.get("referer") or "")


def ensure_admin_request_origin(request: Request) -> None:
    request_origin = get_request_origin(request)
    current_origin = url_origin(str(request.base_url).rstrip("/"))
    allowed = allowed_admin_origins()
    if current_origin:
        allowed.add(current_origin)
    if not request_origin or request_origin not in allowed:
        raise HTTPException(status_code=403, detail="Forbidden origin")


def use_secure_cookie(request: Request) -> bool:
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    if forwarded_proto == "https":
        return True
    return request.url.scheme == "https"


def create_admin_session() -> str:
    cleanup_sessions()
    token = secrets.token_urlsafe(32)
    ADMIN_SESSIONS[token] = utc_now()
    return token


def ensure_admin_session(request: Request) -> None:
    cleanup_sessions()
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token or token not in ADMIN_SESSIONS:
        raise HTTPException(status_code=401, detail="Unauthorized")


def extract_client_ip(request: Request) -> str:
    for header_name in ("cf-connecting-ip", "x-real-ip", "x-forwarded-for"):
        header_value = request.headers.get(header_name, "").strip()
        if header_value:
            return header_value.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def is_public_ip(ip_address: str) -> bool:
    try:
        parsed = ipaddress.ip_address(ip_address)
        return not (
            parsed.is_private
            or parsed.is_loopback
            or parsed.is_reserved
            or parsed.is_multicast
            or parsed.is_unspecified
        )
    except ValueError:
        return False


async def fetch_geo_for_ip(ip_address: str) -> dict[str, Any]:
    if not ip_address or not is_public_ip(ip_address):
        return {}

    cached = GEO_CACHE.get(ip_address)
    if cached:
        return cached

    try:
        async with httpx.AsyncClient(timeout=1.8) as client:
            response = await client.get(f"https://ipwho.is/{ip_address}")
            response.raise_for_status()
            payload = response.json()
    except Exception:
        return {}

    if not payload.get("success"):
        return {}

    timezone_payload = payload.get("timezone") or {}
    currency_payload = payload.get("currency") or {}
    geo_data = {
        "country_code": (payload.get("country_code") or "").upper(),
        "country_name": payload.get("country") or "",
        "city": payload.get("city") or "",
        "region": payload.get("region") or "",
        "timezone": timezone_payload.get("id") or "",
        "currency": currency_payload.get("code") or "",
    }
    GEO_CACHE[ip_address] = geo_data
    return geo_data


async def build_visitor_context(request: Request) -> tuple[dict[str, Any], str | None]:
    ip_address = extract_client_ip(request)
    visitor = {
        "ip_address": ip_address,
        "country_code": (
            request.headers.get("cf-ipcountry")
            or request.headers.get("x-vercel-ip-country")
            or request.headers.get("cloudfront-viewer-country")
            or request.headers.get("x-country-code")
            or ""
        ).upper(),
        "country_name": "",
        "city": request.headers.get("x-vercel-ip-city", "") or request.headers.get("x-city", ""),
        "region": request.headers.get("x-vercel-ip-country-region", "") or request.headers.get("x-region", ""),
        "timezone": request.headers.get("x-vercel-ip-timezone", "") or request.headers.get("x-timezone", ""),
        "currency": "",
        "user_agent": request.headers.get("user-agent", ""),
        "accept_language": request.headers.get("accept-language", ""),
        "referrer": request.headers.get("referer", ""),
    }

    geo_data = await fetch_geo_for_ip(ip_address)
    for key, value in geo_data.items():
        if not visitor.get(key):
            visitor[key] = value

    browser_language = normalize_language_code(visitor["accept_language"])
    return visitor, browser_language


init_db()
runtime_log(
    "module_initialized",
    db_path=DB_FILE,
    db_exists=DB_FILE.exists(),
    bot_enabled=BOT_ENABLED,
    web_url=WEB_URL,
    admin_panel_url=ADMIN_PANEL_URL,
    initial_admin_ids=sorted(INITIAL_ADMIN_IDS),
)

# --- TELEGRAM BOT ---
bot_app = Application.builder().token(BOT_TOKEN).build() if BOT_ENABLED else None


def get_bot_status() -> dict[str, Any]:
    runtime_running = False
    updater_running = False
    users = list_bot_admins()
    admins_total = sum(1 for item in users if item.get("role") == "admin")
    processors_total = sum(1 for item in users if item.get("role") == "processor")
    handlers_total = sum(1 for item in users if item.get("role") == "handler")

    if bot_app is not None:
        runtime_running = bool(getattr(bot_app, "running", False))
        updater = getattr(bot_app, "updater", None)
        updater_running = bool(updater and getattr(updater, "running", False))

    return {
        "enabled": BOT_ENABLED,
        "admin_id_configured": admins_total > 0,
        "admin_count": admins_total,
        "user_count": len(users),
        "processor_count": processors_total,
        "handler_count": handlers_total,
        "runtime_started": BOT_RUNTIME_STARTED,
        "app_running": runtime_running,
        "updater_running": updater_running,
        "control_panel_ready": BOT_ENABLED and admins_total > 0 and BOT_RUNTIME_STARTED,
        "web_url": WEB_URL,
        "error": BOT_RUNTIME_ERROR,
    }


def get_selected_geo(context: ContextTypes.DEFAULT_TYPE, user_id: int | None = None) -> str:
    selected_geo = sanitize_geo_code(str(context.user_data.get("selected_geo", "")))
    if not selected_geo:
        selected_geo = get_bot_admin_selected_geo(user_id)
    if selected_geo:
        context.user_data["selected_geo"] = selected_geo
        return selected_geo
    selected_geo = sanitize_geo_code("ES")
    return selected_geo or "ES"


def main_keyboard(role: str | None) -> ReplyKeyboardMarkup:
    safe_role = sanitize_bot_role(role, "handler")
    rows: list[list[str]] = [["🗺 Выбрать GEO", "📊 Активные реквизиты"]]
    if bot_role_has_permission(safe_role, "edit_requisites"):
        rows.append(["📝 Реквизиты", "🗂 История реквизитов"])
        rows.append(["🗑 Удалить реквизит"])
    if bot_role_has_permission(safe_role, "edit_manager"):
        rows.append(["👤 Менеджер", "👥 Права доступа"])
    elif bot_role_has_permission(safe_role, "manage_access"):
        rows.append(["👥 Права доступа"])
    if bot_role_has_permission(safe_role, "create_link"):
        rows.append(["🔗 Ссылка на оплату"])
    if bot_role_has_permission(safe_role, "open_admin_panel"):
        rows.append(["🛠 Админка"])
    rows.append(["ℹ️ Помощь"])
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
    )


def geo_picker_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["ES", "IT"], ["DE", "FR"]], resize_keyboard=True, one_time_keyboard=True)


def manager_action_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[MANAGER_KEEP_OPTION], [MANAGER_ADD_OPTION]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def build_geo_details_text(geo_code: str) -> str:
    profile = get_geo_profile(geo_code)
    requisites = get_active_requisites(geo_code)
    manager = get_default_manager_for_geo(geo_code)
    manager_name = manager["manager_name"] if manager and manager.get("manager_name") else "не задан"
    manager_link = manager["manager_telegram_url"] if manager and manager.get("manager_telegram_url") else "не указан"
    return (
        f"GEO: {profile['geo_code']} ({profile['geo_name']})\n"
        f"Язык по умолчанию: {profile['default_language']}\n"
        f"Таймер: {profile['refresh_minutes']} мин\n"
        f"Менеджер по умолчанию: {manager_name}\n"
        f"Telegram: {manager_link}\n\n"
        f"Банк: {requisites['bank_name']}\n"
        f"IBAN: {requisites['card_number']}\n"
        f"BIC / SWIFT: {requisites['bic_swift'] or 'не указан'}\n"
        f"Получатель: {requisites['receiver_name']}"
    )


def build_geo_overview_text() -> str:
    blocks = ["Активные реквизиты по GEO:"]
    for snapshot in list_geo_snapshots():
        profile = snapshot["profile"]
        requisites = snapshot["active_requisites"]
        manager = snapshot.get("default_manager") or {}
        manager_status = manager.get("manager_name") or "менеджер не задан"
        blocks.append(
            f"\n{profile['geo_code']} | {profile['geo_name']} | {profile['default_language']} | "
            f"таймер {profile['refresh_minutes']} мин | менеджер: {manager_status}\n"
            f"{requisites['bank_name']} | {requisites['card_number']} | {requisites['bic_swift'] or 'BIC/SWIFT не указан'} | {requisites['receiver_name']}"
        )
    return "\n".join(blocks)


def build_admins_text() -> str:
    admins = list_bot_admins()
    lines = ["Пользователи бота:"]
    for item in admins:
        display_name = item["full_name"] or item["username"] or "Без имени"
        username = f"@{item['username']}" if item["username"] else "username не задан"
        role_label = get_bot_role_label(item.get("role"))
        lines.append(f"\nID: {item['user_id']} | {display_name} | {username} | роль: {role_label}")
    lines.append(
        "\nРоли:\n"
        "Обработчик -> только ссылки\n"
        "Процессор -> только реквизиты\n"
        "Админ -> полный доступ\n\n"
        "Добавить админа: /addadmin 123456789\n"
        "Поставить роль: /setrole 123456789 handler|processor|admin\n"
        "Удалить: /removeadmin 123456789\n"
        "Список: /admins"
    )
    return "\n".join(lines)


def build_help_text(role: str | None, geo_code: str) -> str:
    safe_role = sanitize_bot_role(role, "handler")
    lines = [
        "Что можно делать сейчас:",
        f"Текущий GEO: {geo_code}",
        "",
        "1. Сначала выберите GEO, если нужен другой.",
        "2. Потом используйте доступные вам кнопки снизу.",
    ]
    if bot_role_has_permission(safe_role, "create_link"):
        lines.append("3. Для ссылки на оплату нажмите `🔗 Ссылка на оплату` и следуйте шагам.")
    if bot_role_has_permission(safe_role, "edit_requisites"):
        lines.append("3. Для реквизитов нажмите `📝 Реквизиты`.")
        lines.append("4. История реквизитов открывается кнопкой `🗂 История реквизитов`.")
    if bot_role_has_permission(safe_role, "edit_manager"):
        lines.append("5. Для менеджера по умолчанию нажмите `👤 Менеджер`.")
    if bot_role_has_permission(safe_role, "manage_access"):
        lines.append("6. Права доступа смотрите в `👥 Права доступа`.")
    return "\n".join(lines)


def build_requisites_history_text(geo_code: str, action: str = "restore") -> str:
    items = list_geo_requisites_history_for_geo(geo_code)
    if not items:
        return (
            f"История реквизитов для {geo_code} пока пуста.\n\n"
            "Сначала сохраните хотя бы один комплект реквизитов."
        )

    action_hint = (
        "Нажмите кнопку под нужной записью, чтобы снова сделать её активной."
        if action == "restore"
        else "Нажмите кнопку под нужной записью, чтобы удалить запись из истории."
    )
    lines = [
        f"История реквизитов для {geo_code}:",
        action_hint,
        "",
    ]
    for item in items:
        lines.append(
            f"ID {item['id']} | {item['bank_name']} | {item['card_number']} | {item['bic_swift'] or 'без BIC/SWIFT'} | "
            f"{item['receiver_name']} | {item['created_at']}"
        )
    return "\n".join(lines)


def build_link_requisites_selection_text(geo_code: str) -> str:
    items = list_geo_requisites_history_for_geo(geo_code)
    lines = [
        f"Текущий GEO: {geo_code}",
        "Выберите реквизиты для ссылки.",
        "Отправьте `latest` или `-`, чтобы взять текущие активные.",
        "",
    ]
    for item in items:
        lines.append(
            f"ID {item['id']} | {item['bank_name']} | {item['card_number']} | {item['bic_swift'] or 'без BIC/SWIFT'}"
        )
    return "\n".join(lines)


def build_link_manager_selection_text(geo_code: str) -> str:
    default_manager = get_default_manager_for_geo(geo_code)
    items = list_geo_managers(geo_code)
    lines = [
        f"Текущий GEO: {geo_code}",
        "Выберите менеджера для ссылки.",
        "Отправьте `default` или `-`, чтобы взять менеджера GEO по умолчанию.",
        "Или отправьте Telegram-ссылку / @username, чтобы подставить свой контакт в эту ссылку.",
    ]
    if default_manager and default_manager.get("id"):
        lines.append(
            f"По умолчанию: ID {default_manager['id']} | {default_manager['manager_name']} | "
            f"{default_manager['manager_telegram_url'] or 'без ссылки'}"
        )
    lines.append("")
    for item in items:
        lines.append(
            f"ID {item['id']} | {item['manager_name']} | {item['manager_telegram_url'] or 'без ссылки'}"
        )
    return "\n".join(lines)


def build_link_language_selection_text() -> str:
    options = ", ".join(item["code"] for item in LANGUAGE_OPTIONS)
    return (
        "Выберите язык лендинга.\n"
        "Отправьте `auto` или `-`, чтобы оставить автоопределение.\n"
        f"Доступно: {options}"
    )


def requisites_history_keyboard(geo_code: str, action: str = "restore") -> InlineKeyboardMarkup:
    items = list_geo_requisites_history_for_geo(geo_code)
    rows: list[list[InlineKeyboardButton]] = []
    action_label = "Активировать" if action == "restore" else "Удалить"
    for item in items:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{action_label} ID {item['id']}",
                    callback_data=f"req:{action}:{geo_code}:{item['id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="Обновить список", callback_data=f"req:list:{action}:{geo_code}")])
    return InlineKeyboardMarkup(rows)


async def admin_check(update: Update, permission: str | None = None) -> bool:
    message = update.effective_message
    if message is None:
        return False
    if not list_bot_admins():
        await message.reply_text("Пользователи бота еще не настроены. Добавьте ADMIN_ID или ADMIN_IDS в окружение.")
        return False
    user = update.effective_user
    if user is None or not has_bot_access(user.id):
        await message.reply_text("Нет доступа. Ваш Telegram ID не добавлен в систему.")
        return False
    role = get_bot_user_role(user.id)
    if permission and not bot_role_has_permission(role, permission):
        await message.reply_text(
            f"Недостаточно прав. Ваша роль: {get_bot_role_label(role)}."
        )
        return False
    upsert_bot_admin_identity(user)
    return True


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update):
        return
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    role = get_bot_user_role(user_id)
    await update.effective_message.reply_text(
        "Внутренняя панель команды активна.\n\n"
        f"Ваша роль: {get_bot_role_label(role)}\n"
        f"Текущий выбранный GEO: {selected_geo}\n\n"
        f"{build_geo_details_text(selected_geo)}\n\n{build_help_text(role, selected_geo)}",
        reply_markup=main_keyboard(role),
    )


async def show_geo_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "view_geo"):
        return
    user_id = update.effective_user.id if update.effective_user else None
    await update.effective_message.reply_text(
        build_geo_overview_text(),
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update):
        return
    user_id = update.effective_user.id if update.effective_user else None
    role = get_bot_user_role(user_id)
    selected_geo = get_selected_geo(context, user_id)
    await update.effective_message.reply_text(
        build_help_text(role, selected_geo),
        reply_markup=main_keyboard(role),
    )


async def show_admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "manage_access"):
        return
    user_id = update.effective_user.id if update.effective_user else None
    await update.effective_message.reply_text(
        build_admins_text(),
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )


async def show_admin_panel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "open_admin_panel"):
        return
    user_id = update.effective_user.id if update.effective_user else None
    await update.effective_message.reply_text(
        "Быстрый вход в веб-админку:\n"
        f"{build_admin_panel_link()}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )


async def add_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "manage_access"):
        return
    if not context.args:
        await update.effective_message.reply_text(
            "Укажите один или несколько Telegram ID.\n"
            "Пример: /addadmin 123456789 987654321\n"
            "Также можно: /addadmin 123456789,987654321",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    target_ids = parse_admin_ids_from_args(context.args)
    if not target_ids:
        await update.effective_message.reply_text(
            "Нужен корректный числовой Telegram ID.",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    added_ids: list[int] = []
    skipped_ids: list[int] = []
    for target_id in target_ids:
        added = add_bot_admin(update.effective_user.id, target_id)
        if added:
            added_ids.append(target_id)
            log_bot_activity(update.effective_user.id, "set_role", target_user_id=target_id, payload="admin")
        else:
            skipped_ids.append(target_id)
    summary_lines = []
    if added_ids:
        summary_lines.append(f"Добавлены: {', '.join(str(item) for item in added_ids)}")
    if skipped_ids:
        summary_lines.append(f"Уже были в списке: {', '.join(str(item) for item in skipped_ids)}")
    if not summary_lines:
        summary_lines.append("Никого не удалось добавить.")
    await update.effective_message.reply_text(
        f"{chr(10).join(summary_lines)}\n\n{build_admins_text()}",
        reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
    )


async def remove_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "manage_access"):
        return
    if not context.args:
        await update.effective_message.reply_text(
            "Укажите один или несколько Telegram ID.\n"
            "Пример: /removeadmin 123456789 987654321\n"
            "Также можно: /removeadmin 123456789,987654321",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    target_ids = parse_admin_ids_from_args(context.args)
    if not target_ids:
        await update.effective_message.reply_text(
            "Нужен корректный числовой Telegram ID.",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    removed_ids: list[int] = []
    failed_messages: list[str] = []
    for target_id in target_ids:
        success, message = remove_bot_admin(target_id)
        if success:
            removed_ids.append(target_id)
            log_bot_activity(update.effective_user.id, "remove_user", target_user_id=target_id)
        else:
            failed_messages.append(f"{target_id}: {message}")
    summary_lines = []
    if removed_ids:
        summary_lines.append(f"Удалены: {', '.join(str(item) for item in removed_ids)}")
    if failed_messages:
        summary_lines.extend(failed_messages)
    if not summary_lines:
        summary_lines.append("Никого не удалось удалить.")
    await update.effective_message.reply_text(
        f"{chr(10).join(summary_lines)}\n\n{build_admins_text()}",
        reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
    )


async def set_role_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "manage_access"):
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text(
            "Формат: /setrole 123456789 handler|processor|admin",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    target_user_id = parse_optional_int(context.args[0])
    target_role = sanitize_bot_role(context.args[1], "")
    if target_user_id is None or target_user_id <= 0 or target_role not in BOT_ROLE_SET:
        await update.effective_message.reply_text(
            "Пример: /setrole 123456789 processor",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    try:
        save_bot_user_role(update.effective_user.id, target_user_id, target_role)
    except HTTPException as exc:
        await update.effective_message.reply_text(
            str(exc.detail),
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return
    log_bot_activity(update.effective_user.id, "set_role", target_user_id=target_user_id, payload=target_role)
    await update.effective_message.reply_text(
        f"Роль для {target_user_id} обновлена: {get_bot_role_label(target_role)}\n\n{build_admins_text()}",
        reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
    )


async def select_geo_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "select_geo"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\nВыберите новый GEO.",
        reply_markup=geo_picker_keyboard(),
    )
    return WAITING_GEO_SELECTION


async def select_geo_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    geo_code = sanitize_geo_code(update.effective_message.text)
    if not geo_code:
        await update.effective_message.reply_text("Поддерживаются только ES, IT, DE и FR.")
        return WAITING_GEO_SELECTION

    context.user_data["selected_geo"] = geo_code
    set_bot_admin_selected_geo(update.effective_user.id if update.effective_user else None, geo_code)
    log_bot_activity(update.effective_user.id if update.effective_user else None, "select_geo", geo_code=geo_code)
    await update.effective_message.reply_text(
        f"GEO переключен на {geo_code}.\n\n{build_geo_details_text(geo_code)}",
        reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
    )
    return ConversationHandler.END


async def change_reqs_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "edit_requisites"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    requisites = get_active_requisites(selected_geo)
    runtime_log(
        "change_reqs_start",
        user_id=user_id,
        selected_geo=selected_geo,
        active_requisites_id=requisites.get("id"),
    )
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\n"
        f"Банк: {requisites['bank_name']}\n"
        f"IBAN: {requisites['card_number']}\n"
        f"BIC / SWIFT: {requisites['bic_swift'] or 'не указан'}\n"
        f"Получатель: {requisites['receiver_name']}\n\n"
        "Отправьте новые реквизиты четырьмя строками:\n"
        "Банк\n"
        "IBAN\n"
        "BIC / SWIFT (можно оставить пустым, поставив -)\n"
        "Получатель"
    )
    return WAITING_REQUISITES


async def change_reqs_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    lines = [line.strip() for line in update.effective_message.text.strip().splitlines() if line.strip()]
    if len(lines) < 4:
        await update.effective_message.reply_text("Нужно 4 строки: банк, IBAN, BIC / SWIFT и получатель.")
        return WAITING_REQUISITES

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    runtime_log("change_reqs_save", user_id=user_id, selected_geo=selected_geo, lines_count=len(lines))
    update_geo_requisites(selected_geo, lines[0], lines[1], "" if lines[2] == "-" else lines[2], lines[3])
    log_bot_activity(user_id, "update_requisites", geo_code=selected_geo, payload=lines[1])
    await update.effective_message.reply_text(
        f"Реквизиты для {selected_geo} обновлены.\n\n{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def show_requisites_history_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "view_requisites_history"):
        return
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    await update.effective_message.reply_text(
        build_requisites_history_text(selected_geo, "restore"),
        reply_markup=requisites_history_keyboard(selected_geo, "restore"),
    )


async def show_requisites_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update, "delete_requisites"):
        return
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    await update.effective_message.reply_text(
        f"{build_requisites_history_text(selected_geo, 'delete')}\n\n"
        "Последний комплект реквизитов удалить нельзя.",
        reply_markup=requisites_history_keyboard(selected_geo, "delete"),
    )


async def requisites_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    if not await admin_check(update, "view_requisites_history"):
        await query.answer()
        return
    payload = (query.data or "").split(":")
    if len(payload) < 4:
        await query.answer()
        return
    _, action, geo_code, history_id_raw = payload[0], payload[1], payload[2], payload[3]
    if payload[1] == "list":
        action = payload[2]
        geo_code = payload[3]
        await query.edit_message_text(
            build_requisites_history_text(geo_code, action),
            reply_markup=requisites_history_keyboard(geo_code, action),
        )
        await query.answer("Список обновлен")
        return
    history_id = parse_optional_int(history_id_raw)
    user_id = update.effective_user.id if update.effective_user else None
    if history_id is None or history_id <= 0:
        await query.answer("Некорректный ID")
        return
    try:
        if action == "restore":
            restore_geo_requisites_from_history(geo_code, history_id)
            log_bot_activity(user_id, "restore_requisites", geo_code=geo_code, payload=str(history_id))
            text = (
                f"Реквизиты из истории ID {history_id} снова активны для {geo_code}.\n\n"
                f"{build_geo_details_text(geo_code)}"
            )
            await query.edit_message_text(text)
            await query.answer("Реквизиты активированы")
            return
        if action == "delete":
            if not bot_role_has_permission(get_bot_user_role(user_id), "delete_requisites"):
                await query.answer("Недостаточно прав")
                return
            delete_geo_requisites_history_item(geo_code, history_id)
            log_bot_activity(user_id, "delete_requisites", geo_code=geo_code, payload=str(history_id))
            await query.edit_message_text(
                f"Реквизиты ID {history_id} удалены для {geo_code}.\n\n"
                f"{build_requisites_history_text(geo_code, 'delete')}",
                reply_markup=requisites_history_keyboard(geo_code, "delete"),
            )
            await query.answer("Реквизиты удалены")
            return
    except HTTPException as exc:
        await query.answer(str(exc.detail), show_alert=True)
        return


async def show_requisites_history_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_message.text.strip() == "🗂 История реквизитов":
        await show_requisites_history_start(update, context)
        return ConversationHandler.END
    history_id = parse_optional_int(update.effective_message.text)
    if history_id is None or history_id <= 0:
        await update.effective_message.reply_text(
            "История уже показана выше. Нажмите кнопку у нужной записи или отправьте /cancel."
        )
        return ConversationHandler.END

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    try:
        restore_geo_requisites_from_history(selected_geo, history_id)
        log_bot_activity(user_id, "restore_requisites", geo_code=selected_geo, payload=str(history_id))
    except HTTPException as exc:
        await update.effective_message.reply_text(str(exc.detail))
        return ConversationHandler.END

    await update.effective_message.reply_text(
        f"Реквизиты из истории ID {history_id} снова активны для {selected_geo}.\n\n"
        f"{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def delete_requisites_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await show_requisites_delete_start(update, context)
    return ConversationHandler.END


async def delete_requisites_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    history_id = parse_optional_int(update.effective_message.text)
    if history_id is None or history_id <= 0:
        await update.effective_message.reply_text("История уже показана выше. Нажмите кнопку у нужной записи или отправьте /cancel.")
        return ConversationHandler.END

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    try:
        delete_geo_requisites_history_item(selected_geo, history_id)
        log_bot_activity(user_id, "delete_requisites", geo_code=selected_geo, payload=str(history_id))
    except HTTPException as exc:
        await update.effective_message.reply_text(str(exc.detail))
        return ConversationHandler.END

    await update.effective_message.reply_text(
        f"Реквизиты ID {history_id} удалены для {selected_geo}.\n\n{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def change_manager_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "edit_manager"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    manager = get_default_manager_for_geo(selected_geo) or {}
    runtime_log(
        "change_manager_start",
        user_id=user_id,
        selected_geo=selected_geo,
        manager_id=manager.get("id"),
        manager_name=manager.get("manager_name"),
        manager_url=manager.get("manager_telegram_url"),
    )
    manager_name = manager.get("manager_name") or "не задан"
    manager_link = manager.get("manager_telegram_url") or "не указан"
    if manager.get("id"):
        await update.effective_message.reply_text(
            f"Текущий GEO: {selected_geo}\n"
            f"Менеджер по умолчанию: {manager_name}\n"
            f"Telegram: {manager_link}\n\n"
            "Выберите действие: оставить текущего или добавить нового.",
            reply_markup=manager_action_keyboard(),
        )
        return WAITING_MANAGER_ACTION

    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\n"
        f"Менеджер по умолчанию: {manager_name}\n"
        f"Telegram: {manager_link}\n\n"
        "Отправьте две строки:\n"
        "Имя менеджера\n"
        "Telegram-ссылка менеджера",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return WAITING_MANAGER


async def change_manager_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    choice = (update.effective_message.text or "").strip()
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    runtime_log("change_manager_action", user_id=user_id, selected_geo=selected_geo, choice=choice)
    if choice == MANAGER_KEEP_OPTION:
        await update.effective_message.reply_text(
            f"Настройки менеджера для {selected_geo} оставлены без изменений.",
            reply_markup=main_keyboard(get_bot_user_role(user_id)),
        )
        return ConversationHandler.END
    if choice in {MANAGER_ADD_OPTION, MANAGER_EDIT_OPTION_LEGACY}:
        await update.effective_message.reply_text(
            f"Текущий GEO: {selected_geo}\n"
            "Отправьте две строки:\n"
            "Имя менеджера\n"
            "Telegram-ссылка менеджера",
            reply_markup=main_keyboard(get_bot_user_role(user_id)),
        )
        return WAITING_MANAGER

    await update.effective_message.reply_text(
        "Выберите вариант кнопкой ниже: оставить текущего или добавить нового.",
        reply_markup=manager_action_keyboard(),
    )
    return WAITING_MANAGER_ACTION


async def change_manager_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    lines = [line.strip() for line in update.effective_message.text.strip().splitlines()]
    if len(lines) < 2 or not lines[0]:
        await update.effective_message.reply_text("Нужно 2 строки: имя менеджера и Telegram-ссылка.")
        return WAITING_MANAGER

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    runtime_log("change_manager_save", user_id=user_id, selected_geo=selected_geo, lines_count=len(lines))
    saved_manager = save_geo_manager(
        ManagerPayload(
            manager_id=None,
            geo_code=selected_geo,
            manager_name=lines[0],
            manager_telegram_url=lines[1],
            make_default=True,
        )
    )
    log_bot_activity(user_id, "update_manager", geo_code=selected_geo, payload=lines[0])
    await update.effective_message.reply_text(
        f"Новый менеджер для {selected_geo} добавлен и назначен по умолчанию.\n\n"
        f"ID: {saved_manager.get('id')}\n"
        f"{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def create_link_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "create_link"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    runtime_log("create_link_start", user_id=user_id, selected_geo=selected_geo)
    context.user_data.pop("temp_amount", None)
    context.user_data.pop("temp_requisites_id", None)
    context.user_data.pop("temp_manager_id", None)
    context.user_data.pop("temp_manager_link", None)
    context.user_data.pop("temp_language", None)
    context.user_data.pop("temp_label", None)
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\nВведите сумму к оплате, например: 250"
    )
    return WAITING_LINK_AMOUNT


async def create_link_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    amount = parse_payment_amount(update.effective_message.text)
    if amount is None:
        await update.effective_message.reply_text("Нужно ввести положительную сумму, например: 250")
        return WAITING_LINK_AMOUNT

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    context.user_data["temp_amount"] = amount
    active_requisites = get_active_requisites(selected_geo)
    active_requisites_id = active_requisites.get("id")
    context.user_data["temp_requisites_id"] = active_requisites_id if active_requisites_id else None
    runtime_log(
        "create_link_amount",
        user_id=user_id,
        selected_geo=selected_geo,
        amount=amount,
        active_requisites_id=active_requisites_id,
    )
    await update.effective_message.reply_text(
        f"Использую активные реквизиты GEO {selected_geo}: "
        f"ID {active_requisites_id if active_requisites_id else 'latest'}.\n"
        "Если нужно выбрать другой ID, сначала активируйте его в `🗂 История реквизитов`.\n\n"
        f"{build_link_manager_selection_text(selected_geo)}"
    )
    return WAITING_LINK_MANAGER


async def create_link_requisites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    raw_value = update.effective_message.text.strip().lower()
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    requisites_id: int | None = None
    if raw_value not in {"-", "latest"}:
        requisites_id = parse_optional_int(raw_value)
        if requisites_id is None or get_geo_requisites_by_id(selected_geo, requisites_id) is None:
            await update.effective_message.reply_text("Нужен `latest` или корректный ID реквизитов.")
            return WAITING_LINK_REQUISITES

    context.user_data["temp_requisites_id"] = requisites_id
    await update.effective_message.reply_text(build_link_manager_selection_text(selected_geo))
    return WAITING_LINK_MANAGER


async def create_link_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    raw_text = update.effective_message.text.strip()
    raw_value = raw_text.lower()
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    manager_id: int | None = None
    manager_link = ""
    if raw_value not in {"-", "default"}:
        parsed_manager_id = parse_optional_int(raw_value)
        if parsed_manager_id is not None and get_geo_manager_by_id(selected_geo, parsed_manager_id) is not None:
            manager_id = parsed_manager_id
        else:
            manager_link = normalize_manager_link(raw_text)
            if not manager_link:
                await update.effective_message.reply_text(
                    "Нужен `default`, корректный ID менеджера или Telegram-ссылка вида @username / https://t.me/username."
                )
                return WAITING_LINK_MANAGER

    context.user_data["temp_manager_id"] = manager_id
    context.user_data["temp_manager_link"] = manager_link
    runtime_log(
        "create_link_manager",
        user_id=user_id,
        selected_geo=selected_geo,
        manager_id=manager_id,
        manager_link=manager_link,
        raw_text=raw_text,
    )
    await update.effective_message.reply_text(build_link_language_selection_text())
    return WAITING_LINK_LANGUAGE


async def create_link_language(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    raw_value = update.effective_message.text.strip().lower()
    safe_language: str | None = None
    if raw_value not in {"-", "auto"}:
        safe_language = sanitize_language_code(raw_value)
        if not safe_language:
            available = ", ".join(item["code"] for item in LANGUAGE_OPTIONS)
            await update.effective_message.reply_text(f"Нужен `auto` или один из кодов: {available}")
            return WAITING_LINK_LANGUAGE

    context.user_data["temp_language"] = safe_language
    await update.effective_message.reply_text(
        "Введите назначение платежа или отправьте - если оно не нужно."
    )
    return WAITING_LINK_LABEL


async def create_link_label(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    label = update.effective_message.text.strip()
    if label != "-" and not payment_label_has_only_latin(label):
        await update.effective_message.reply_text(
            "Назначение платежа можно вводить только латиницей, цифрами, пробелом и дефисом.\n"
            "Пример: service payment или service-payment"
        )
        return WAITING_LINK_LABEL

    context.user_data["temp_label"] = "" if label == "-" else sanitize_payment_label(label)
    await update.effective_message.reply_text(
        "Введите комментарий для лендинга или отправьте - если он не нужен."
    )
    return WAITING_LINK_COMMENT


async def create_link_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await handle_menu_interrupt(update, context):
        return ConversationHandler.END

    amount = float(context.user_data.get("temp_amount", 0))
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    requisites_id = context.user_data.get("temp_requisites_id")
    manager_id = context.user_data.get("temp_manager_id")
    manager_link = normalize_manager_link(context.user_data.get("temp_manager_link"))
    forced_language = context.user_data.get("temp_language")
    clean_label = str(context.user_data.get("temp_label", ""))
    comment = update.effective_message.text.strip()
    clean_comment = "" if comment == "-" else clean_payment_comment(comment)
    runtime_log(
        "create_link_comment_started",
        user_id=user_id,
        selected_geo=selected_geo,
        amount=amount,
        requisites_id=requisites_id,
        manager_id=manager_id,
        manager_link=manager_link,
        language=forced_language,
        label=clean_label,
        comment=clean_comment,
    )
    try:
        link = build_payment_link(
            amount,
            selected_geo,
            clean_label,
            clean_comment,
            forced_language=forced_language,
            requisites_id=requisites_id,
            manager_id=manager_id,
            manager_link_override=manager_link,
        )
    except HTTPException as exc:
        await update.effective_message.reply_text(
            str(exc.detail),
            reply_markup=main_keyboard(get_bot_user_role(user_id)),
        )
        return ConversationHandler.END
    manager = resolve_manager_for_geo(selected_geo, manager_id)
    if manager_link:
        manager = {
            **manager,
            "manager_name": manager.get("manager_name") or "персональный контакт",
            "manager_telegram_url": manager_link,
        }
    context.user_data.pop("temp_amount", None)
    context.user_data.pop("temp_requisites_id", None)
    context.user_data.pop("temp_manager_id", None)
    context.user_data.pop("temp_manager_link", None)
    context.user_data.pop("temp_language", None)
    context.user_data.pop("temp_label", None)
    log_bot_activity(user_id, "create_link", geo_code=selected_geo, payload=f"{amount:.2f}")
    runtime_log(
        "create_link_comment_completed",
        user_id=user_id,
        selected_geo=selected_geo,
        amount=amount,
        requisites_id=requisites_id,
        manager_name=manager.get("manager_name"),
        manager_link=manager.get("manager_telegram_url"),
        link=link,
    )
    await update.effective_message.reply_text(
        f"Ссылка готова.\n\n"
        f"GEO: {selected_geo}\n"
        f"Сумма: {amount:.2f} {DEFAULT_CURRENCY}\n"
        f"Реквизиты: {requisites_id or 'latest'}\n"
        f"Менеджер: {manager.get('manager_name') or 'default'}\n"
        f"Назначение: {clean_label or 'не указано'}\n"
        f"Комментарий: {clean_comment or 'не указан'}\n"
        f"Язык: {forced_language or 'auto'}\n"
        f"Ссылка: {link}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id if update.effective_user else None
    await update.effective_message.reply_text(
        "Действие отменено.",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


if bot_app is not None:
    conversation_text_filter = filters.TEXT & ~filters.COMMAND & ~filters.Regex(MENU_BUTTONS_PATTERN)
    conv_handler_geo = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🗺 Выбрать GEO$"), select_geo_start)],
        states={WAITING_GEO_SELECTION: [MessageHandler(conversation_text_filter, select_geo_save)]},
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            MessageHandler(filters.Regex(MENU_BUTTONS_PATTERN), cancel_cmd),
        ],
    )
    conv_handler_req = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📝 Реквизиты$"), change_reqs_start)],
        states={WAITING_REQUISITES: [MessageHandler(conversation_text_filter, change_reqs_save)]},
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            MessageHandler(filters.Regex(MENU_BUTTONS_PATTERN), cancel_cmd),
        ],
    )
    conv_handler_manager = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^👤 Менеджер$"), change_manager_start)],
        states={
            WAITING_MANAGER_ACTION: [MessageHandler(conversation_text_filter, change_manager_action)],
            WAITING_MANAGER: [MessageHandler(conversation_text_filter, change_manager_save)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            MessageHandler(filters.Regex(MENU_BUTTONS_PATTERN), cancel_cmd),
        ],
    )
    conv_handler_link = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🔗 Ссылка на оплату$"), create_link_start)],
        states={
            WAITING_LINK_AMOUNT: [MessageHandler(conversation_text_filter, create_link_amount)],
            WAITING_LINK_REQUISITES: [MessageHandler(conversation_text_filter, create_link_requisites)],
            WAITING_LINK_MANAGER: [MessageHandler(conversation_text_filter, create_link_manager)],
            WAITING_LINK_LANGUAGE: [MessageHandler(conversation_text_filter, create_link_language)],
            WAITING_LINK_LABEL: [MessageHandler(conversation_text_filter, create_link_label)],
            WAITING_LINK_COMMENT: [MessageHandler(conversation_text_filter, create_link_comment)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            MessageHandler(filters.Regex(MENU_BUTTONS_PATTERN), cancel_cmd),
        ],
    )

    bot_app.add_handler(CommandHandler("start", start_cmd))
    bot_app.add_handler(CommandHandler("help", help_cmd))
    bot_app.add_handler(CommandHandler("admins", show_admins_cmd))
    bot_app.add_handler(CommandHandler("addadmin", add_admin_cmd))
    bot_app.add_handler(CommandHandler("setrole", set_role_cmd))
    bot_app.add_handler(CommandHandler("removeadmin", remove_admin_cmd))
    bot_app.add_handler(CommandHandler("adminpanel", show_admin_panel_cmd))
    bot_app.add_handler(CommandHandler("reqhistory", show_requisites_history_start))
    bot_app.add_handler(MessageHandler(filters.Regex("^(📊 GEO статус|📊 Активные реквизиты)$"), show_geo_status_cmd))
    bot_app.add_handler(MessageHandler(filters.Regex("^👥 Права доступа$"), show_admins_cmd))
    bot_app.add_handler(MessageHandler(filters.Regex("^ℹ️ Помощь$"), help_cmd))
    bot_app.add_handler(MessageHandler(filters.Regex("^🗂 История реквизитов$"), show_requisites_history_start))
    bot_app.add_handler(MessageHandler(filters.Regex("^🗑 Удалить реквизит$"), show_requisites_delete_start))
    bot_app.add_handler(MessageHandler(filters.Regex("^🛠 Админка$"), show_admin_panel_cmd))
    bot_app.add_handler(CallbackQueryHandler(requisites_history_callback, pattern=r"^req:"))
    bot_app.add_handler(conv_handler_geo)
    bot_app.add_handler(conv_handler_req)
    bot_app.add_handler(conv_handler_manager)
    bot_app.add_handler(conv_handler_link)


# --- FASTAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_RUNTIME_ERROR, BOT_RUNTIME_STARTED

    runtime_log(
        "lifespan_starting",
        db_path=DB_FILE,
        db_exists=DB_FILE.exists(),
        bot_enabled=BOT_ENABLED,
        web_url=WEB_URL,
    )
    if bot_app is not None:
        try:
            await bot_app.initialize()
            await bot_app.start()
            if bot_app.updater is not None:
                await bot_app.updater.start_polling()
            BOT_RUNTIME_STARTED = True
            BOT_RUNTIME_ERROR = None
            runtime_log("bot_runtime_started", updater_running=bot_app.updater is not None)
        except Exception as exc:
            BOT_RUNTIME_STARTED = False
            BOT_RUNTIME_ERROR = str(exc)
            runtime_log("bot_runtime_failed", level=logging.ERROR, error=str(exc))

    yield

    if bot_app is not None and BOT_RUNTIME_STARTED:
        try:
            if bot_app.updater is not None:
                await bot_app.updater.stop()
            await bot_app.stop()
            await bot_app.shutdown()
            runtime_log("bot_runtime_stopped")
        except Exception:
            pass


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def runtime_logging_middleware(request: Request, call_next):
    request_id = secrets.token_hex(6)
    started_at = time.perf_counter()
    runtime_log(
        "http_request_started",
        request_id=request_id,
        method=request.method,
        path=str(request.url.path),
        query=request.url.query,
        client_ip=request.client.host if request.client else None,
    )
    try:
        response = await call_next(request)
    except Exception as exc:
        runtime_log(
            "http_request_failed",
            level=logging.ERROR,
            request_id=request_id,
            method=request.method,
            path=str(request.url.path),
            error=str(exc),
            elapsed_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
        raise

    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    response.headers["X-Request-ID"] = request_id
    runtime_log(
        "http_request_completed",
        request_id=request_id,
        method=request.method,
        path=str(request.url.path),
        status_code=response.status_code,
        elapsed_ms=elapsed_ms,
    )
    return response
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        WEB_URL,
    ],
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


@app.get("/")
async def landing_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/admin")
async def admin_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "admin.html")


@app.get("/api/health")
async def healthcheck():
    return {
        "status": "ok",
        "build_tag": APP_BUILD_TAG,
        "instance_id": APP_INSTANCE_ID,
        "bot": get_bot_status(),
        "database_exists": DB_FILE.exists(),
        "db_path": str(DB_FILE),
    }


@app.get("/api/landing-context")
async def landing_context(
    request: Request,
    payment: str | None = None,
    geo: str | None = None,
    req: int | None = None,
    mgr: int | None = None,
    mgr_link: str | None = None,
    lang: str | None = None,
    label: str | None = None,
    comment: str | None = None,
):
    visitor, browser_language = await build_visitor_context(request)
    resolved_geo = resolve_geo_code(geo, visitor.get("country_code"), browser_language)
    profile = get_geo_profile(resolved_geo)
    manager = resolve_manager_for_geo(resolved_geo, mgr)
    forced_manager_link = normalize_manager_link(mgr_link)
    if forced_manager_link:
        manager = {
            **manager,
            "manager_name": manager.get("manager_name") or "personal manager",
            "manager_telegram_url": forced_manager_link,
        }
    payment_amount = parse_payment_amount(payment)
    payment_label = (label or "").strip()
    mode = "live" if payment_amount is not None else ("preview" if ALLOW_PREVIEW_MODE else "invalid")
    invalid_reason = ""
    if mode != "invalid" and not normalize_manager_link(manager.get("manager_telegram_url")):
        mode = "invalid"
        invalid_reason = "manager_missing"
    requisites = resolve_requisites_for_geo(resolved_geo, req) if mode != "invalid" else None
    refresh_seconds = max(60, int(profile["refresh_minutes"]) * 60) if mode != "invalid" else 0
    recommended_language = resolve_recommended_language(
        explicit_language=lang,
        browser_language=browser_language,
        country_code=visitor.get("country_code"),
        geo_default_language=profile.get("default_language"),
    )
    payment_comment = clean_payment_comment(comment)

    visit_token = record_visit(
        mode=mode,
        visitor=visitor,
        recommended_language=recommended_language,
        geo_code=resolved_geo,
        payment_amount=payment_amount,
        payment_label=payment_label or None,
        payment_comment=payment_comment or None,
        manager=manager,
        requisites=requisites,
        request=request,
    )

    return {
        "mode": mode,
        "invalid_reason": invalid_reason,
        "recommended_language": recommended_language,
        "available_languages": LANGUAGE_OPTIONS,
        "payment": {
            "amount": payment_amount if payment_amount is not None else (DEFAULT_PREVIEW_AMOUNT if mode == "preview" else None),
            "currency": DEFAULT_CURRENCY,
            "label": payment_label,
            "comment": payment_comment,
        },
        "geo": {
            **profile,
            "refresh_seconds": refresh_seconds,
        },
        "manager": manager,
        "requisites": requisites,
        "visit": {
            "token": visit_token,
            "client_first_name": "",
            "client_last_name": "",
        },
        "visitor": visitor,
        "timer": {
            "refresh_seconds": refresh_seconds,
            "expires_at": (utc_now() + timedelta(seconds=refresh_seconds)).isoformat() if refresh_seconds else None,
        },
    }


@app.post("/api/landing-client")
async def landing_client(payload: LandingClientPayload):
    client = save_landing_client(
        visit_token=payload.visit_token,
        first_name=payload.first_name,
        last_name=payload.last_name,
    )
    return {"ok": True, "client": client}


@app.get("/api/admin/session")
async def admin_session(request: Request):
    cleanup_sessions()
    token = request.cookies.get(SESSION_COOKIE_NAME)
    return {
        "authenticated": bool(token and token in ADMIN_SESSIONS),
        "configured": ADMIN_AUTH_CONFIGURED,
    }


@app.post("/api/admin/login")
async def admin_login(payload: AdminLoginPayload, request: Request):
    ensure_admin_request_origin(request)
    if not ADMIN_AUTH_CONFIGURED:
        raise HTTPException(
            status_code=503,
            detail="Вход отключен: задайте ADMIN_USERNAME и ADMIN_PASSWORD в переменных окружения.",
        )
    client_ip = extract_client_ip(request)
    ensure_login_not_rate_limited(client_ip)
    if not (
        secrets.compare_digest(payload.username, ADMIN_USERNAME)
        and secrets.compare_digest(payload.password, ADMIN_PASSWORD)
    ):
        register_failed_login(client_ip)
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    response = JSONResponse({"authenticated": True})
    session_token = create_admin_session()
    clear_failed_logins(client_ip)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_token,
        httponly=True,
        secure=use_secure_cookie(request),
        samesite="strict",
        max_age=SESSION_TTL_HOURS * 3600,
    )
    return response


@app.post("/api/admin/logout")
async def admin_logout(request: Request):
    ensure_admin_request_origin(request)
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE_NAME, samesite="strict")
    return response


@app.get("/api/admin/dashboard")
async def admin_dashboard(request: Request):
    ensure_admin_session(request)
    return {
        "generated_at": utc_now_iso(),
        "web_url": WEB_URL,
        "db_path": str(DB_FILE),
        "bot": get_bot_status(),
        "bot_users": list_bot_admins(),
        "bot_roles": BOT_ROLE_OPTIONS,
        "worker_stats": get_worker_stats(),
        "bot_activity": list_bot_activity(),
        "stats": get_summary_stats(),
        "geos": list_geo_snapshots(),
        "managers": list_geo_managers(),
        "requisites_history": list_geo_requisites_history(),
        "visits": list_visits(),
        "languages": LANGUAGE_OPTIONS,
    }


@app.post("/api/admin/geos/{geo_code}")
async def admin_update_geo(geo_code: str, payload: GeoConfigPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    snapshot = save_geo_configuration(geo_code, payload)
    return {"ok": True, "geo": snapshot}


@app.post("/api/admin/requisites/{geo_code}")
async def admin_save_requisites(geo_code: str, payload: RequisitesPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    active_requisites = update_geo_requisites(
        geo_code,
        payload.bank_name,
        payload.card_number,
        payload.bic_swift,
        payload.receiver_name,
    )
    return {"ok": True, "active_requisites": active_requisites}


@app.post("/api/admin/managers")
async def admin_save_manager(payload: ManagerPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    manager = save_geo_manager(payload)
    return {"ok": True, "manager": manager, "profile": get_geo_profile(payload.geo_code)}


@app.post("/api/admin/bot-users")
async def admin_save_bot_user(payload: BotUserPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    if payload.user_id <= 0:
        raise HTTPException(status_code=400, detail="Нужен корректный Telegram ID")
    bot_user = save_bot_user_role(0, payload.user_id, payload.role)
    return {"ok": True, "bot_user": bot_user}


@app.delete("/api/admin/bot-users/{user_id}")
async def admin_delete_bot_user(user_id: int, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    success, message = remove_bot_admin(user_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message}


@app.post("/api/admin/requisites/{geo_code}/history/{history_id}/activate")
async def admin_activate_requisites_history(geo_code: str, history_id: int, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    active_requisites = restore_geo_requisites_from_history(geo_code, history_id)
    return {"ok": True, "active_requisites": active_requisites}


@app.delete("/api/admin/requisites/{geo_code}/history/{history_id}")
async def admin_delete_requisites_history(geo_code: str, history_id: int, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_session(request)
    result = delete_geo_requisites_history_item(geo_code, history_id)
    return {"ok": True, **result}


STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
