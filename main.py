import asyncio
import ipaddress
import logging
import os
import re
import secrets
import sqlite3
import sys
import time
from uuid import uuid4
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
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
LOG_LEVEL_NAME = (os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO")
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
    force=True,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("paymentplatform")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
# Volume mount path (e.g. /data) — логотипы сохраняются в {UPLOADS_VOLUME_PATH}/channel-logos
# Railway: добавьте переменную UPLOADS_VOLUME_PATH=/data
UPLOADS_VOLUME_PATH = os.getenv("UPLOADS_VOLUME_PATH", "").strip() or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
if UPLOADS_VOLUME_PATH:
    CHANNEL_LOGO_DIR = Path(UPLOADS_VOLUME_PATH) / "channel-logos"
else:
    CHANNEL_LOGO_DIR = STATIC_DIR / "uploads" / "channel-logos"


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


def parse_optional_int(raw_value: str, default: int | None = None) -> int | None:
    value = (raw_value or "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_optional_float(raw_value: str, default: float | None = None) -> float | None:
    value = (raw_value or "").strip()
    if not value:
        return default
    try:
        return float(value)
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
DEFAULT_TELEGRAM_PROXY_URL = "socks5://user351165:35rmsy@78.142.238.14:16481"
TELEGRAM_PROXY_URL = os.getenv("TELEGRAM_PROXY_URL", "").strip() or DEFAULT_TELEGRAM_PROXY_URL
TELEGRAM_CONNECT_TIMEOUT = max(5.0, parse_optional_float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", ""), 20.0) or 20.0)
TELEGRAM_READ_TIMEOUT = max(5.0, parse_optional_float(os.getenv("TELEGRAM_READ_TIMEOUT", ""), 20.0) or 20.0)
TELEGRAM_WRITE_TIMEOUT = max(5.0, parse_optional_float(os.getenv("TELEGRAM_WRITE_TIMEOUT", ""), 20.0) or 20.0)
TELEGRAM_POOL_TIMEOUT = max(1.0, parse_optional_float(os.getenv("TELEGRAM_POOL_TIMEOUT", ""), 10.0) or 10.0)
TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT = max(
    5.0,
    parse_optional_float(os.getenv("TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT", ""), TELEGRAM_CONNECT_TIMEOUT) or TELEGRAM_CONNECT_TIMEOUT,
)
TELEGRAM_GET_UPDATES_READ_TIMEOUT = max(
    5.0,
    parse_optional_float(os.getenv("TELEGRAM_GET_UPDATES_READ_TIMEOUT", ""), 70.0) or 70.0,
)
TELEGRAM_GET_UPDATES_WRITE_TIMEOUT = max(
    5.0,
    parse_optional_float(os.getenv("TELEGRAM_GET_UPDATES_WRITE_TIMEOUT", ""), TELEGRAM_WRITE_TIMEOUT) or TELEGRAM_WRITE_TIMEOUT,
)
TELEGRAM_GET_UPDATES_POOL_TIMEOUT = max(
    1.0,
    parse_optional_float(os.getenv("TELEGRAM_GET_UPDATES_POOL_TIMEOUT", ""), TELEGRAM_POOL_TIMEOUT) or TELEGRAM_POOL_TIMEOUT,
)
DEFAULT_CURRENCY = "EUR"
DEFAULT_REFRESH_MINUTES = max(1, parse_optional_int(os.getenv("DEFAULT_REFRESH_MINUTES", ""), 15) or 15)
DEFAULT_PREVIEW_AMOUNT = 250.0
ALLOW_PREVIEW_MODE = os.getenv("ALLOW_PREVIEW_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
CLIENT_NAME_MAX_LENGTH = 80
CHANNEL_NAME_MAX_LENGTH = 120
CHANNEL_LOGO_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
CHANNEL_LOGO_MAX_BYTES = 2 * 1024 * 1024
PAYMENT_LINK_TOKEN_BYTES = 18

BOT_ENABLED = bool(BOT_TOKEN)
WAITING_GEO_SELECTION = 1
WAITING_REQUISITES = 2
WAITING_MANAGER_ACTION = 3
WAITING_MANAGER = 4
WAITING_LINK_AMOUNT = 5
WAITING_LINK_CURRENCY = 6
WAITING_LINK_REQUISITES = 7
WAITING_LINK_LABEL = 8
WAITING_LINK_COMMENT = 9
WAITING_LINK_PAYMENT_COMMENT = 15
WAITING_LINK_MANAGER = 10
WAITING_LINK_LANGUAGE = 11
WAITING_ADD_REQ_GEO = 12
WAITING_DELETE_REQ_GEO = 13
WAITING_NEW_GEO_DETAILS = 14
ADD_REQUISITE_BTN = "➕ Добавить реквизит"
DELETE_REQUISITE_BTN = "🗑 Удалить реквизит"
ADD_GEO_BTN = "➕ Добавить GEO"
MENU_BUTTONS_PATTERN = (
    r"^(🗺 Выбрать GEO|📊 GEO статус|📊 Активные реквизиты|📝 Реквизиты|🗂 История реквизитов|"
    r"🗑 Удалить реквизит|➕ Добавить реквизит|➕ Добавить GEO|👥 Права доступа|🔗 Ссылка на оплату|🛠 Админка|ℹ️ Помощь)$"
)
MENU_BUTTON_LABELS = {
    "🗺 Выбрать GEO",
    "📊 GEO статус",
    "📊 Активные реквизиты",
    "📝 Реквизиты",
    "🗂 История реквизитов",
    "🗑 Удалить реквизит",
    ADD_REQUISITE_BTN,
    ADD_GEO_BTN,
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
BOT_RUNTIME_LAST_ATTEMPT_AT: str | None = None
BOT_RUNTIME_LAST_SUCCESS_AT: str | None = None
BOT_RUNTIME_ATTEMPTS = 0
BOT_RUNTIME_TASK: asyncio.Task | None = None
BOT_RUNTIME_SHUTDOWN = False
BOT_STARTUP_RETRY_SECONDS = max(5, parse_optional_int(os.getenv("BOT_STARTUP_RETRY_SECONDS", ""), 30) or 30)
BOT_HEALTHCHECK_SECONDS = max(5, parse_optional_int(os.getenv("BOT_HEALTHCHECK_SECONDS", ""), 15) or 15)
LOGIN_ATTEMPTS: dict[str, list[datetime]] = {}
LOGIN_WINDOW_MINUTES = 10
MAX_LOGIN_ATTEMPTS = 5

LANGUAGE_OPTIONS = [
    {"code": "es", "label": "Español"},
    {"code": "en", "label": "English"},
    {"code": "de", "label": "Deutsch"},
    {"code": "fr", "label": "Français"},
    {"code": "ro", "label": "Română"},
    {"code": "it", "label": "Italiano"},
    {"code": "lt", "label": "Lietuvių"},
    {"code": "pl", "label": "Polski"},
]
FALLBACK_LANGUAGE_CODE = LANGUAGE_OPTIONS[0]["code"]
LANDING_COMMENT_OPTIONS = [
    {
        "code": "en",
        "button": "Английский",
        "comment": "Only INSTANT TRANSFERS are accepted",
    },
    {
        "code": "de",
        "button": "Немецкий",
        "comment": "Es werden nur ECHTZEIT-UEBERWEISUNGEN akzeptiert",
    },
    {
        "code": "es",
        "button": "Испанский",
        "comment": "Solo se acepta PAGO INSTANTÁNEO",
    },
    {
        "code": "fr",
        "button": "Французский",
        "comment": "Seuls les PAIEMENTS INSTANTANÉS sont acceptés",
    },
    {
        "code": "ro",
        "button": "Румынский",
        "comment": "Se acceptă doar PLATĂ INSTANTANEE.",
    },
    {
        "code": "it",
        "button": "Итальянский",
        "comment": "È accettato solo PAGAMENTO ISTANTANEO",
    },
    {
        "code": "lt",
        "button": "Литовский",
        "comment": "Priimami tik AKIMIRKŠNIAI PERVEDIMAI",
    },
    {
        "code": "pl",
        "button": "Польский",
        "comment": "Akceptowane są tylko NATYCHMIASTOWE PRZELEWY",
    },
]
LANDING_COMMENT_BY_BUTTON = {
    item["button"].casefold(): item for item in LANDING_COMMENT_OPTIONS
}
LANDING_COMMENT_BUTTON_BY_CODE = {
    item["code"]: item["button"] for item in LANDING_COMMENT_OPTIONS
}
LANDING_LANGUAGE_SET = {item["code"] for item in LANGUAGE_OPTIONS}
CURRENCY_OPTIONS = [
    {"code": "EUR", "label": "Евро"},
    {"code": "USD", "label": "Доллар"},
    {"code": "RON", "label": "Румынский рон"},
]
CURRENCY_SET = {item["code"] for item in CURRENCY_OPTIONS}
BOT_ROLE_OPTIONS = [
    {"code": "handler", "label": "Обработчик"},
    {"code": "processor", "label": "Процессор"},
    {"code": "admin", "label": "Админ"},
]
BOT_ROLE_SET = {item["code"] for item in BOT_ROLE_OPTIONS}
BOT_ROLE_LABELS = {item["code"]: item["label"] for item in BOT_ROLE_OPTIONS}
BOT_ROLE_PERMISSIONS = {
    "handler": {"view_geo", "create_link"},
    "processor": {"edit_requisites", "delete_requisites"},
    "admin": {
        "select_geo",
        "view_geo",
        "edit_requisites",
        "view_requisites_history",
        "delete_requisites",
        "manage_access",
        "create_link",
        "open_admin_panel",
    },
}
LANGUAGE_TO_GEO_MAP = {
    "en": "EN",
    "es": "ES",
    "ca": "ES",
    "eu": "ES",
    "gl": "ES",
    "de": "DE",
    "fr": "FR",
    "it": "IT",
    "lt": "LT",
    "pl": "PL",
}
SPECIAL_LANGUAGE_MAP = {
    "nb": "no",
    "nn": "no",
}
DEFAULT_GEO_CONFIGS = {
    "DE": {
        "geo_name": "Germany",
        "default_language": "de",
        "manager_name": "Germany manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "EN": {
        "geo_name": "English",
        "default_language": "en",
        "manager_name": "English manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "ES": {
        "geo_name": "Spain",
        "default_language": "es",
        "manager_name": "Spain manager",
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
    "IT": {
        "geo_name": "Italy",
        "default_language": "it",
        "manager_name": "Italy manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "LT": {
        "geo_name": "Lithuania",
        "default_language": "lt",
        "manager_name": "Lithuania manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "PL": {
        "geo_name": "Poland",
        "default_language": "pl",
        "manager_name": "Poland manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
}
SUPPORTED_GEOS = tuple(DEFAULT_GEO_CONFIGS.keys())
SUPPORTED_GEO_SET = set(SUPPORTED_GEOS)
GEO_CODE_PATTERN = re.compile(r"^[A-Z0-9_-]{2,12}$")
# В боте «Выбрать GEO» показываются только эти три региона (Испания, Германия, Литва)
BOT_SELECT_GEO_WHITELIST = ("DE", "ES", "LT")


class AdminLoginPayload(BaseModel):
    username: str
    password: str


class GeoConfigPayload(BaseModel):
    geo_name: str
    default_language: str
    refresh_minutes: int
    default_manager_id: int | None = None


class RequisitesPayload(BaseModel):
    geo_code: str | None = None
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
    channel_id: int | None = None


class CreatePaymentLinkPayload(BaseModel):
    amount: float
    geo_code: str
    currency_code: str | None = None
    language_code: str | None = None
    handler_user_id: int
    label: str = ""
    comment: str = ""  # комментарий для лендинга (стандартная фраза)
    payment_comment: str = ""  # комментарий к платежу (что указать при переводе)


class ChannelPayload(BaseModel):
    channel_id: int | None = None
    channel_name: str
    logo_path: str | None = None


def build_runtime_snapshot() -> dict[str, Any]:
    return {
        "log_level": LOG_LEVEL_NAME,
        "bot_enabled": BOT_ENABLED,
        "bot_token_configured": bool(BOT_TOKEN),
        "initial_admin_ids_count": len(INITIAL_ADMIN_IDS),
        "db_path": str(DB_FILE),
        "db_exists": DB_FILE.exists(),
        "web_url": WEB_URL,
        "telegram_proxy_configured": bool(TELEGRAM_PROXY_URL),
        "telegram_connect_timeout": TELEGRAM_CONNECT_TIMEOUT,
        "telegram_read_timeout": TELEGRAM_READ_TIMEOUT,
        "telegram_write_timeout": TELEGRAM_WRITE_TIMEOUT,
        "telegram_pool_timeout": TELEGRAM_POOL_TIMEOUT,
        "telegram_get_updates_connect_timeout": TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT,
        "telegram_get_updates_read_timeout": TELEGRAM_GET_UPDATES_READ_TIMEOUT,
        "uploads_path": str(CHANNEL_LOGO_DIR),
        "uploads_path_exists": CHANNEL_LOGO_DIR.exists(),
        "admin_auth_configured": ADMIN_AUTH_CONFIGURED,
        "bot_startup_retry_seconds": BOT_STARTUP_RETRY_SECONDS,
        "bot_healthcheck_seconds": BOT_HEALTHCHECK_SECONDS,
    }


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


def normalize_geo_code(value: str | None) -> str | None:
    code = (value or "").strip().upper()
    return code if GEO_CODE_PATTERN.fullmatch(code) else None


def build_geo_default_config(geo_code: str) -> dict[str, Any]:
    safe_geo = normalize_geo_code(geo_code) or "ES"
    fallback = DEFAULT_GEO_CONFIGS.get(safe_geo)
    if fallback is not None:
        return fallback
    return {
        "geo_name": safe_geo,
        "default_language": FALLBACK_LANGUAGE_CODE,
        "manager_name": f"{safe_geo} manager",
        "manager_telegram_url": "",
        "default_manager_id": None,
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    }


def list_known_geo_codes() -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT geo_code FROM geo_profiles
        UNION
        SELECT geo_code FROM geo_requisites
        UNION
        SELECT geo_code FROM geo_managers
        ORDER BY geo_code ASC
        """
    ).fetchall()
    conn.close()
    codes = {code for code in (normalize_geo_code(row["geo_code"]) for row in rows) if code}
    codes.update(DEFAULT_GEO_CONFIGS.keys())
    return sorted(codes)


def list_geo_codes_with_requisites() -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT DISTINCT geo_code
        FROM geo_requisites
        ORDER BY geo_code ASC
        """
    ).fetchall()
    conn.close()
    return [code for code in (normalize_geo_code(row["geo_code"]) for row in rows) if code]


def sanitize_geo_code(value: str | None, allow_unknown: bool = False) -> str | None:
    code = normalize_geo_code(value)
    if not code:
        return None
    if allow_unknown:
        return code
    return code if code in set(list_known_geo_codes()) else None


def geo_has_requisites(geo_code: str) -> bool:
    safe_geo = sanitize_geo_code(geo_code) or normalize_geo_code(geo_code)
    if not safe_geo:
        return False
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS value FROM geo_requisites WHERE geo_code = ?",
        (safe_geo,),
    ).fetchone()
    conn.close()
    return bool(row and int(row["value"]) > 0)


def get_first_available_geo_code() -> str | None:
    available = list_geo_codes_with_requisites()
    return available[0] if available else None


def sanitize_language_code(value: str | None) -> str | None:
    code = (value or "").strip().lower()
    return code if code in LANDING_LANGUAGE_SET else None


def sanitize_currency_code(value: str | None) -> str | None:
    code = (value or "").strip().upper()
    return code if code in CURRENCY_SET else None


def sanitize_bot_role(value: str | None, default: str = "handler") -> str:
    code = (value or "").strip().lower()
    return code if code in BOT_ROLE_SET else default


def sanitize_channel_name(value: str | None) -> str:
    clean_value = re.sub(r"\s+", " ", (value or "").strip())
    return clean_value[:CHANNEL_NAME_MAX_LENGTH]


def normalize_static_asset_path(value: str | None) -> str:
    path = (value or "").strip()
    if not path:
        return ""
    if path.startswith("/static/"):
        return path
    if path.startswith("static/"):
        return f"/{path}"
    return ""


def channel_logo_public_url(logo_path: str | None) -> str:
    return normalize_static_asset_path(logo_path)


def get_logo_extension(filename: str | None) -> str:
    suffix = Path(filename or "").suffix.lower()
    return suffix if suffix in CHANNEL_LOGO_EXTENSIONS else ""


def remove_channel_logo_file(logo_path: str | None) -> None:
    public_path = normalize_static_asset_path(logo_path)
    if not public_path.startswith("/static/uploads/channel-logos/"):
        return
    filename = public_path.removeprefix("/static/uploads/channel-logos/")
    if not filename or "/" in filename or "\\" in filename:
        return
    file_path = CHANNEL_LOGO_DIR / filename
    if file_path.is_file():
        file_path.unlink(missing_ok=True)


async def store_channel_logo(upload: UploadFile) -> str:
    extension = get_logo_extension(upload.filename)
    if not extension:
        raise HTTPException(status_code=400, detail="Логотип должен быть PNG, JPG, JPEG, WEBP или GIF")
    filename = f"channel-{uuid4().hex}{extension}"
    target_path = CHANNEL_LOGO_DIR / filename
    content = await upload.read(CHANNEL_LOGO_MAX_BYTES + 1)
    if not content:
        raise HTTPException(status_code=400, detail="Файл логотипа пустой")
    if len(content) > CHANNEL_LOGO_MAX_BYTES:
        raise HTTPException(status_code=400, detail="Логотип не должен быть больше 2 МБ")
    target_path.write_bytes(content)
    return f"/static/uploads/channel-logos/{filename}"


def get_bot_role_label(role: str | None) -> str:
    return BOT_ROLE_LABELS.get(sanitize_bot_role(role, "handler"), "Обработчик")


def bot_role_has_permission(role: str | None, permission: str) -> bool:
    safe_role = sanitize_bot_role(role, "handler")
    return permission in BOT_ROLE_PERMISSIONS.get(safe_role, set())


def web_permissions_for_role(role: str | None) -> list[str]:
    safe_role = sanitize_bot_role(role, "handler")
    return sorted(BOT_ROLE_PERMISSIONS.get(safe_role, set()))


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
    value = re.sub(r"\s+", " ", (raw_value or "").strip())
    if not value:
        return ""
    return value[:160]


def payment_label_has_only_latin(raw_value: str | None) -> bool:
    return True


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


def extract_telegram_username(raw_value: str | None) -> str:
    value = normalize_manager_link(raw_value)
    if not value:
        return ""
    if value.startswith("https://t.me/"):
        return value.removeprefix("https://t.me/").strip().strip("/")
    if value.startswith("http://t.me/"):
        return value.removeprefix("http://t.me/").strip().strip("/")
    if value.startswith("@"):
        return value[1:].strip()
    return ""


def clean_payment_comment(raw_value: str | None) -> str:
    value = re.sub(r"\s+", " ", (raw_value or "").strip())
    return value[:300]


def is_menu_button_text(text: str | None) -> bool:
    return (text or "").strip() in MENU_BUTTON_LABELS


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
    currency_code: str | None = None,
    label: str = "",
    comment: str = "",
    forced_language: str | None = None,
    requisites_id: int | None = None,
    manager_id: int | None = None,
    manager_link_override: str | None = None,
) -> str:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    handler_contact = resolve_handler_contact(manager_id, manager_link_override)
    if resolve_requisites_for_geo(safe_geo, requisites_id) is None:
        raise HTTPException(status_code=400, detail="Не найдено ни одного реквизита")
    if handler_contact is None or not normalize_manager_link(handler_contact.get("manager_telegram_url")):
        raise HTTPException(status_code=400, detail="Не выбран обработчик с Telegram-ссылкой")
    params = {
        "payment": format_query_amount(amount),
        "geo": safe_geo,
        "currency": sanitize_currency_code(currency_code) or DEFAULT_CURRENCY,
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
    params["mgr_link"] = str(handler_contact.get("manager_telegram_url") or "")
    return f"{WEB_URL}/?{urlencode(params)}"


def build_admin_panel_link() -> str:
    return ADMIN_PANEL_URL


def resolve_geo_code(requested_geo: str | None, country_code: str | None, browser_language: str | None) -> str:
    explicit_geo = sanitize_geo_code(requested_geo)
    if explicit_geo:
        return explicit_geo

    available_geos = set(list_geo_codes_with_requisites())
    visitor_country = (country_code or "").strip().upper()
    if visitor_country in available_geos:
        return visitor_country

    mapped_geo = LANGUAGE_TO_GEO_MAP.get((browser_language or "").strip().lower())
    if mapped_geo and mapped_geo in available_geos:
        return mapped_geo

    return get_first_available_geo_code() or "ES"


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

    return sanitize_language_code(geo_default_language) or FALLBACK_LANGUAGE_CODE


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


def get_channel_by_id(channel_id: int | None, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    if channel_id is None or channel_id <= 0:
        return None
    owns_connection = conn is None
    db = conn or get_connection()
    row = db.execute(
        """
        SELECT id, channel_name, logo_path, created_at, updated_at
        FROM channels
        WHERE id = ?
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    if owns_connection:
        db.close()
    if row is None:
        return None
    item = dict(row)
    item["logo_url"] = channel_logo_public_url(item.get("logo_path"))
    return item


def list_channels() -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            c.id,
            c.channel_name,
            c.logo_path,
            c.created_at,
            c.updated_at,
            COUNT(admin.user_id) AS handler_count
        FROM channels AS c
        LEFT JOIN bot_admins AS admin
            ON admin.channel_id = c.id AND admin.role = 'handler'
        GROUP BY c.id
        ORDER BY COALESCE(c.channel_name, ''), c.id
        """
    ).fetchall()
    conn.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["logo_url"] = channel_logo_public_url(item.get("logo_path"))
        result.append(item)
    return result


async def save_channel(
    channel_name: str,
    channel_id: int | None = None,
    logo_upload: UploadFile | None = None,
    remove_logo: bool = False,
) -> dict[str, Any]:
    clean_name = sanitize_channel_name(channel_name)
    if not clean_name:
        raise HTTPException(status_code=400, detail="Название канала обязательно")

    conn = get_connection()
    existing = get_channel_by_id(channel_id, conn) if channel_id else None
    if channel_id and existing is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Канал не найден")

    new_logo_path = existing.get("logo_path") if existing else ""
    if remove_logo and new_logo_path:
        remove_channel_logo_file(new_logo_path)
        new_logo_path = ""
    if logo_upload is not None and logo_upload.filename:
        uploaded_logo_path = await store_channel_logo(logo_upload)
        if new_logo_path and new_logo_path != uploaded_logo_path:
            remove_channel_logo_file(new_logo_path)
        new_logo_path = uploaded_logo_path

    now_value = utc_now_iso()
    if existing is None:
        cursor = conn.execute(
            """
            INSERT INTO channels (channel_name, logo_path, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (clean_name, new_logo_path or None, now_value, now_value),
        )
        saved_id = int(cursor.lastrowid)
    else:
        saved_id = int(existing["id"])
        conn.execute(
            """
            UPDATE channels
            SET channel_name = ?, logo_path = ?, updated_at = ?
            WHERE id = ?
            """,
            (clean_name, new_logo_path or None, now_value, saved_id),
        )

    conn.commit()
    conn.close()
    return get_channel_by_id(saved_id)


def delete_channel(channel_id: int) -> dict[str, Any]:
    existing = get_channel_by_id(channel_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Канал не найден")

    conn = get_connection()
    linked = conn.execute(
        "SELECT COUNT(*) AS value FROM bot_admins WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()
    if linked and int(linked["value"]) > 0:
        conn.close()
        raise HTTPException(status_code=400, detail="Канал привязан к обработчикам. Сначала отвяжите его.")

    conn.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
    conn.commit()
    conn.close()
    remove_channel_logo_file(existing.get("logo_path"))
    return {"deleted_channel_id": channel_id}


def seed_geo_data(conn: sqlite3.Connection) -> None:
    now_value = utc_now_iso()
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


def sync_geo_names_from_config(conn: sqlite3.Connection) -> None:
    """Синхронизирует geo_name и default_language из DEFAULT_GEO_CONFIGS для существующих профилей."""
    now_value = utc_now_iso()
    for geo_code, config in DEFAULT_GEO_CONFIGS.items():
        conn.execute(
            """
            UPDATE geo_profiles
            SET geo_name = ?, default_language = ?, updated_at = ?
            WHERE geo_code = ?
            """,
            (config["geo_name"], config["default_language"], now_value, geo_code),
        )


def backfill_geo_requisites_sequence_numbers(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, geo_code, sequence_number
        FROM geo_requisites
        ORDER BY geo_code ASC, created_at ASC, id ASC
        """
    ).fetchall()
    counters: dict[str, int] = {}
    for row in rows:
        safe_geo = normalize_geo_code(row["geo_code"])
        if not safe_geo:
            continue
        counters[safe_geo] = counters.get(safe_geo, 0) + 1
        if row["sequence_number"] and int(row["sequence_number"]) > 0:
            continue
        conn.execute(
            "UPDATE geo_requisites SET sequence_number = ? WHERE id = ?",
            (counters[safe_geo], row["id"]),
        )


def get_next_requisites_sequence(geo_code: str, conn: sqlite3.Connection | None = None) -> int:
    safe_geo = sanitize_geo_code(geo_code) or normalize_geo_code(geo_code) or "ES"
    owns_connection = conn is None
    db = conn or get_connection()
    row = db.execute(
        "SELECT COALESCE(MAX(sequence_number), 0) AS value FROM geo_requisites WHERE geo_code = ?",
        (safe_geo,),
    ).fetchone()
    if owns_connection:
        db.close()
    return int(row["value"]) + 1 if row else 1


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
            channel_id INTEGER,
            added_at TEXT,
            added_by INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT NOT NULL,
            logo_path TEXT,
            created_at TEXT,
            updated_at TEXT
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
            sequence_number INTEGER,
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
        CREATE TABLE IF NOT EXISTS payment_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_token TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'active',
            geo_code TEXT NOT NULL,
            requisites_id INTEGER,
            handler_user_id INTEGER,
            payment_amount REAL NOT NULL,
            payment_currency TEXT NOT NULL,
            forced_language TEXT,
            payment_label TEXT,
            payment_comment TEXT,
            snapshot_handler_name TEXT,
            snapshot_handler_username TEXT,
            snapshot_handler_telegram_url TEXT,
            snapshot_channel_name TEXT,
            snapshot_channel_logo_url TEXT,
            snapshot_bank_name TEXT NOT NULL,
            snapshot_card_number TEXT NOT NULL,
            snapshot_bic_swift TEXT,
            snapshot_receiver_name TEXT NOT NULL,
            created_by_user_id INTEGER,
            creator_role TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            expired_at TEXT,
            last_opened_at TEXT,
            open_count INTEGER NOT NULL DEFAULT 0
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
    ensure_column(conn, "visits", "payment_currency", "TEXT")
    ensure_column(conn, "visits", "payment_link_token", "TEXT")
    ensure_column(conn, "visits", "payment_link_status", "TEXT")
    ensure_column(conn, "bot_admins", "role", "TEXT NOT NULL DEFAULT 'admin'")
    ensure_column(conn, "bot_admins", "channel_id", "INTEGER")
    ensure_column(conn, "bot_admin_preferences", "selected_manager_id", "INTEGER")
    ensure_column(conn, "geo_requisites", "bic_swift", "TEXT DEFAULT ''")
    ensure_column(conn, "geo_requisites", "sequence_number", "INTEGER")
    ensure_column(conn, "geo_profiles", "default_manager_id", "INTEGER")
    ensure_column(conn, "bot_activity_log", "actor_role", "TEXT")
    ensure_column(conn, "bot_activity_log", "geo_code", "TEXT")
    ensure_column(conn, "bot_activity_log", "target_user_id", "INTEGER")
    ensure_column(conn, "bot_activity_log", "payload", "TEXT")
    ensure_column(conn, "bot_activity_log", "created_at", "TEXT")
    ensure_column(conn, "channels", "logo_path", "TEXT")
    ensure_column(conn, "channels", "created_at", "TEXT")
    ensure_column(conn, "channels", "updated_at", "TEXT")
    ensure_column(conn, "payment_links", "status", "TEXT NOT NULL DEFAULT 'active'")
    ensure_column(conn, "payment_links", "geo_code", "TEXT")
    ensure_column(conn, "payment_links", "requisites_id", "INTEGER")
    ensure_column(conn, "payment_links", "handler_user_id", "INTEGER")
    ensure_column(conn, "payment_links", "payment_amount", "REAL")
    ensure_column(conn, "payment_links", "payment_currency", "TEXT")
    ensure_column(conn, "payment_links", "forced_language", "TEXT")
    ensure_column(conn, "payment_links", "payment_label", "TEXT")
    ensure_column(conn, "payment_links", "landing_comment", "TEXT")
    ensure_column(conn, "payment_links", "payment_comment", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_handler_name", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_handler_username", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_handler_telegram_url", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_channel_name", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_channel_logo_url", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_bank_name", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_card_number", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_bic_swift", "TEXT")
    ensure_column(conn, "payment_links", "snapshot_receiver_name", "TEXT")
    ensure_column(conn, "payment_links", "created_by_user_id", "INTEGER")
    ensure_column(conn, "payment_links", "creator_role", "TEXT")
    ensure_column(conn, "payment_links", "created_at", "TEXT")
    ensure_column(conn, "payment_links", "expires_at", "TEXT")
    ensure_column(conn, "payment_links", "expired_at", "TEXT")
    ensure_column(conn, "payment_links", "last_opened_at", "TEXT")
    ensure_column(conn, "payment_links", "open_count", "INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_visits_visit_token
        ON visits (visit_token)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_geo_requisites_sequence
        ON geo_requisites (geo_code, sequence_number ASC, id ASC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bot_activity_actor
        ON bot_activity_log (actor_user_id, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payment_links_token
        ON payment_links (link_token)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payment_links_status
        ON payment_links (status, expires_at)
        """
    )
    seed_bot_admins(conn)
    seed_geo_data(conn)
    sync_geo_names_from_config(conn)
    backfill_geo_requisites_sequence_numbers(conn)
    migrate_legacy_geo_managers(conn)
    conn.commit()
    conn.close()


def get_geo_profile(geo_code: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or normalize_geo_code(geo_code) or "ES"
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

    fallback = build_geo_default_config(safe_geo)
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
    codes = sorted(set(profiles_by_code.keys()) | set(DEFAULT_GEO_CONFIGS.keys()) | set(list_geo_codes_with_requisites()))
    return [profiles_by_code.get(geo_code, get_geo_profile(geo_code)) for geo_code in codes]


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
    return resolve_manager_for_geo(safe_geo, saved_manager_id)


def get_active_requisites(geo_code: str) -> dict[str, Any] | None:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, geo_code, sequence_number, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        WHERE geo_code = ?
        ORDER BY sequence_number ASC, id ASC
        LIMIT 1
        """,
        (safe_geo,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_geo_requisites_by_id(geo_code: str, requisites_id: int | None) -> dict[str, Any] | None:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    if requisites_id is None or requisites_id <= 0:
        return None
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, geo_code, sequence_number, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        WHERE geo_code = ? AND id = ?
        LIMIT 1
        """,
        (safe_geo, requisites_id),
    ).fetchone()
    conn.close()
    return dict(row) if row is not None else None


def resolve_requisites_for_geo(geo_code: str, requisites_id: int | None = None) -> dict[str, Any] | None:
    chosen = get_geo_requisites_by_id(geo_code, requisites_id)
    if chosen is not None:
        return chosen
    return get_active_requisites(geo_code)


def list_geo_snapshots() -> list[dict[str, Any]]:
    return [
        {
            "profile": get_geo_profile(geo_code),
            "active_requisites": get_active_requisites(geo_code),
            "requisites_count": len(list_geo_requisites_history_for_geo(geo_code, limit=1000)),
            "has_requisites": geo_has_requisites(geo_code),
        }
        for geo_code in [profile["geo_code"] for profile in list_geo_profiles()]
    ]


def list_geo_requisites_history(limit: int = 500) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, geo_code, sequence_number, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        ORDER BY geo_code ASC, sequence_number ASC, id ASC
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
        SELECT id, geo_code, sequence_number, bank_name, card_number, bic_swift, receiver_name, created_at
        FROM geo_requisites
        WHERE geo_code = ?
        ORDER BY sequence_number ASC, id ASC
        LIMIT ?
        """,
        (safe_geo, limit),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def save_geo_configuration(geo_code: str, payload: GeoConfigPayload) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code, allow_unknown=True)
    if not safe_geo:
        raise HTTPException(status_code=400, detail="Некорректный GEO-код")

    fallback = build_geo_default_config(safe_geo)
    geo_name = payload.geo_name.strip() or fallback["geo_name"]
    default_language = sanitize_language_code(payload.default_language) or fallback["default_language"]
    refresh_minutes = int(payload.refresh_minutes)
    if refresh_minutes < 1 or refresh_minutes > 120:
        raise HTTPException(status_code=400, detail="Таймер должен быть от 1 до 120 минут")
    default_manager_id = payload.default_manager_id if payload.default_manager_id and payload.default_manager_id > 0 else None
    if default_manager_id and get_geo_manager_by_id(safe_geo, default_manager_id) is None:
        raise HTTPException(status_code=400, detail="Менеджер по умолчанию не найден для выбранного GEO")

    conn = get_connection()
    conn.execute(
        """
        INSERT INTO geo_profiles (
            geo_code, geo_name, default_language, manager_name, manager_telegram_url,
            default_manager_id, refresh_minutes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(geo_code) DO UPDATE SET
            geo_name = excluded.geo_name,
            default_language = excluded.default_language,
            manager_name = geo_profiles.manager_name,
            manager_telegram_url = geo_profiles.manager_telegram_url,
            default_manager_id = excluded.default_manager_id,
            refresh_minutes = excluded.refresh_minutes,
            updated_at = excluded.updated_at
        """,
        (
            safe_geo,
            geo_name,
            default_language,
            fallback["manager_name"],
            fallback["manager_telegram_url"],
            default_manager_id,
            refresh_minutes,
            utc_now_iso(),
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
    safe_geo = sanitize_geo_code(geo_code, allow_unknown=True) or "ES"
    clean_bank = bank_name.strip()
    clean_card = card_number.strip()
    clean_bic_swift = bic_swift.strip()
    clean_receiver = receiver_name.strip()
    if not clean_bank or not clean_card or not clean_receiver:
        raise HTTPException(status_code=400, detail="Нужно указать банк, IBAN и получателя")

    conn = get_connection()
    sequence_number = get_next_requisites_sequence(safe_geo, conn)
    conn.execute(
        """
        INSERT INTO geo_requisites (geo_code, sequence_number, bank_name, card_number, bic_swift, receiver_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (safe_geo, sequence_number, clean_bank, clean_card, clean_bic_swift, clean_receiver, utc_now_iso()),
    )
    conn.commit()
    conn.close()
    return get_active_requisites(safe_geo)


def update_geo_manager(geo_code: str, manager_name: str, manager_telegram_url: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    fallback = build_geo_default_config(safe_geo)
    default_manager = get_default_manager_for_geo(safe_geo)
    saved_manager = save_geo_manager(
        ManagerPayload(
            manager_id=default_manager["id"] if default_manager and default_manager.get("id") else None,
            geo_code=safe_geo,
            manager_name=manager_name.strip() or fallback["manager_name"],
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
    payment_currency: str | None,
    payment_label: str | None,
    payment_comment: str | None,
    manager: dict[str, Any] | None,
    requisites: dict[str, Any] | None,
    request: Request,
    payment_link_token: str | None = None,
    payment_link_status: str | None = None,
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
            snapshot_bank_name, snapshot_card_number, snapshot_bic_swift, snapshot_receiver_name, payment_comment,
            payment_currency, payment_link_token, payment_link_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            payment_currency or None,
            payment_link_token or None,
            payment_link_status or None,
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
            payment_comment, payment_currency, payment_link_token, payment_link_status
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
        SELECT
            admin.user_id,
            admin.username,
            admin.full_name,
            admin.role,
            admin.channel_id,
            admin.added_at,
            admin.added_by,
            channels.channel_name,
            channels.logo_path AS channel_logo_path
        FROM bot_admins AS admin
        LEFT JOIN channels
            ON channels.id = admin.channel_id
        ORDER BY COALESCE(admin.full_name, ''), admin.user_id
        """
    ).fetchall()
    conn.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["channel_logo_url"] = channel_logo_public_url(item.get("channel_logo_path"))
        result.append(item)
    return result


def get_bot_admin_for_login(login_value: str) -> dict[str, Any] | None:
    raw_value = (login_value or "").strip()
    if not raw_value:
        return None
    normalized_username = raw_value.removeprefix("@").strip().lower()
    numeric_id = parse_optional_int(raw_value)
    for item in list_bot_admins():
        if numeric_id is not None and int(item.get("user_id") or 0) == numeric_id:
            return item
        if normalized_username and str(item.get("username") or "").strip().lower() == normalized_username:
            return item
    return None


def list_bot_users_by_role(role: str) -> list[dict[str, Any]]:
    safe_role = sanitize_bot_role(role, "handler")
    return [item for item in list_bot_admins() if sanitize_bot_role(item.get("role"), "handler") == safe_role]


def build_handler_contact(user: dict[str, Any] | None) -> dict[str, Any] | None:
    if not user:
        return None
    username = str(user.get("username") or "").strip()
    if not username:
        return None
    full_name = str(user.get("full_name") or "").strip()
    return {
        "id": user.get("user_id"),
        "manager_name": full_name or f"@{username}",
        "manager_telegram_url": f"https://t.me/{username}",
        "username": username,
        "channel_id": user.get("channel_id"),
        "channel_name": user.get("channel_name") or "",
        "channel_logo_url": channel_logo_public_url(user.get("channel_logo_url") or user.get("channel_logo_path")),
    }


def list_handler_contacts() -> list[dict[str, Any]]:
    contacts = [build_handler_contact(item) for item in list_bot_users_by_role("handler")]
    return [item for item in contacts if item]


def get_handler_contact_by_user_id(user_id: int | None) -> dict[str, Any] | None:
    if user_id is None or user_id <= 0:
        return None
    user = next((item for item in list_bot_users_by_role("handler") if int(item.get("user_id") or 0) == int(user_id)), None)
    return build_handler_contact(user)


def get_handler_contact_by_username(username: str | None) -> dict[str, Any] | None:
    clean_username = extract_telegram_username(username).lower()
    if not clean_username:
        return None
    for item in list_handler_contacts():
        if str(item.get("username") or "").lower() == clean_username:
            return item
    return None


def resolve_handler_contact(
    handler_user_id: int | None = None,
    raw_handler_link: str | None = None,
) -> dict[str, Any] | None:
    by_id = get_handler_contact_by_user_id(handler_user_id)
    if by_id is not None:
        return by_id
    by_username = get_handler_contact_by_username(raw_handler_link)
    if by_username is not None:
        return by_username
    normalized_link = normalize_manager_link(raw_handler_link)
    if not normalized_link:
        return None
    username = extract_telegram_username(normalized_link)
    return {
        "id": None,
        "manager_name": f"@{username}" if username else "Обработчик",
        "manager_telegram_url": normalized_link,
        "username": username,
        "channel_id": None,
        "channel_name": "",
        "channel_logo_url": "",
    }


def parse_iso_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value)
    except ValueError:
        return None


def build_payment_link_url(link_token: str, forced_language: str | None = None) -> str:
    params = {"link": str(link_token or "")}
    safe_language = sanitize_language_code(forced_language)
    if safe_language:
        params["lang"] = safe_language
    return f"{WEB_URL}/?{urlencode(params)}"


def expire_payment_link_if_needed(record: dict[str, Any], conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    expires_at = parse_iso_datetime(record.get("expires_at"))
    if record.get("status") != "active" or expires_at is None or expires_at > utc_now():
        return record

    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True
    expired_at = utc_now_iso()
    conn.execute(
        "UPDATE payment_links SET status = 'expired', expired_at = COALESCE(expired_at, ?) WHERE id = ?",
        (expired_at, record["id"]),
    )
    if own_conn:
        conn.commit()
    updated = {**record, "status": "expired", "expired_at": record.get("expired_at") or expired_at}
    if own_conn:
        conn.close()
    return updated


def roll_payment_link_refresh_window(record: dict[str, Any], conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Когда истекло окно expires_at у активной ссылки: подтянуть актуальные реквизиты GEO,
    продлить таймер или пометить ссылку истёкшей, если реквизитов больше нет.
    """
    geo_code = str(record.get("geo_code") or "")
    profile = get_geo_profile(geo_code)
    refresh_minutes = max(1, int(profile.get("refresh_minutes") or DEFAULT_REFRESH_MINUTES))
    active_req = get_active_requisites(geo_code)
    link_id = record.get("id")
    if active_req is None:
        expired_at = utc_now_iso()
        conn.execute(
            """
            UPDATE payment_links
            SET status = 'expired', expired_at = COALESCE(expired_at, ?)
            WHERE id = ?
            """,
            (expired_at, link_id),
        )
        return {**record, "status": "expired", "expired_at": record.get("expired_at") or expired_at}
    new_expires = utc_now() + timedelta(minutes=refresh_minutes)
    conn.execute(
        """
        UPDATE payment_links
        SET
            expires_at = ?,
            requisites_id = ?,
            snapshot_bank_name = ?,
            snapshot_card_number = ?,
            snapshot_bic_swift = ?,
            snapshot_receiver_name = ?
        WHERE id = ?
        """,
        (
            new_expires.isoformat(),
            active_req.get("id"),
            active_req.get("bank_name"),
            active_req.get("card_number"),
            (active_req.get("bic_swift") or ""),
            active_req.get("receiver_name"),
            link_id,
        ),
    )
    return {
        **record,
        "expires_at": new_expires.isoformat(),
        "requisites_id": active_req.get("id"),
        "snapshot_bank_name": active_req.get("bank_name"),
        "snapshot_card_number": active_req.get("card_number"),
        "snapshot_bic_swift": (active_req.get("bic_swift") or ""),
        "snapshot_receiver_name": active_req.get("receiver_name"),
    }


def get_payment_link_by_token(link_token: str, mark_opened: bool = False) -> dict[str, Any] | None:
    clean_token = (link_token or "").strip()
    if not clean_token:
        return None
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            id, link_token, status, geo_code, requisites_id, handler_user_id,
            payment_amount, payment_currency, forced_language, payment_label, landing_comment, payment_comment,
            snapshot_handler_name, snapshot_handler_username, snapshot_handler_telegram_url,
            snapshot_channel_name, snapshot_channel_logo_url,
            snapshot_bank_name, snapshot_card_number, snapshot_bic_swift, snapshot_receiver_name,
            created_by_user_id, creator_role, created_at, expires_at, expired_at, last_opened_at, open_count
        FROM payment_links
        WHERE link_token = ?
        LIMIT 1
        """,
        (clean_token,),
    ).fetchone()
    if row is None:
        conn.close()
        return None
    record = dict(row)
    if record.get("status") == "active":
        expires_at = parse_iso_datetime(record.get("expires_at"))
        if expires_at is not None and expires_at <= utc_now():
            record = roll_payment_link_refresh_window(record, conn)
    record = expire_payment_link_if_needed(record, conn)
    if mark_opened and record.get("status") == "active":
        opened_at = utc_now_iso()
        conn.execute(
            """
            UPDATE payment_links
            SET open_count = COALESCE(open_count, 0) + 1, last_opened_at = ?
            WHERE id = ?
            """,
            (opened_at, record["id"]),
        )
        record["open_count"] = int(record.get("open_count") or 0) + 1
        record["last_opened_at"] = opened_at
    conn.commit()
    conn.close()
    return record


def list_payment_links(limit: int = 60) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            id, link_token, status, geo_code, requisites_id, handler_user_id,
            payment_amount, payment_currency, forced_language, payment_label, landing_comment, payment_comment,
            snapshot_handler_name, snapshot_handler_username, snapshot_handler_telegram_url,
            snapshot_channel_name, snapshot_channel_logo_url,
            snapshot_bank_name, snapshot_card_number, snapshot_bic_swift, snapshot_receiver_name,
            created_by_user_id, creator_role, created_at, expires_at, expired_at, last_opened_at, open_count
        FROM payment_links
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        record = dict(row)
        if record.get("status") == "active":
            ea = parse_iso_datetime(record.get("expires_at"))
            if ea is not None and ea <= utc_now():
                record = roll_payment_link_refresh_window(record, conn)
        result.append(expire_payment_link_if_needed(record, conn))
    conn.commit()
    conn.close()
    return result


def create_payment_link_record(
    amount: float,
    geo_code: str,
    creator_user_id: int | None,
    creator_role: str | None,
    currency_code: str | None = None,
    label: str = "",
    landing_comment: str = "",
    payment_comment: str = "",
    forced_language: str | None = None,
    handler_user_id: int | None = None,
) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    handler = get_handler_contact_by_user_id(handler_user_id)
    if handler is None or not normalize_manager_link(handler.get("manager_telegram_url")):
        raise HTTPException(status_code=400, detail="Выберите обработчика с Telegram username")
    requisites = get_active_requisites(safe_geo)
    if requisites is None:
        raise HTTPException(status_code=400, detail="Не найдено ни одного реквизита")

    profile = get_geo_profile(safe_geo)
    refresh_minutes = max(1, int(profile.get("refresh_minutes") or DEFAULT_REFRESH_MINUTES))
    link_token = secrets.token_urlsafe(PAYMENT_LINK_TOKEN_BYTES)
    created_at = utc_now()
    expires_at = created_at + timedelta(minutes=refresh_minutes)
    clean_label = sanitize_payment_label(label)
    clean_landing = (landing_comment or "").strip()
    payment_comment_cleaned = clean_payment_comment(payment_comment)
    safe_language = sanitize_language_code(forced_language)
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO payment_links (
            link_token, status, geo_code, requisites_id, handler_user_id,
            payment_amount, payment_currency, forced_language, payment_label, landing_comment, payment_comment,
            snapshot_handler_name, snapshot_handler_username, snapshot_handler_telegram_url,
            snapshot_channel_name, snapshot_channel_logo_url,
            snapshot_bank_name, snapshot_card_number, snapshot_bic_swift, snapshot_receiver_name,
            created_by_user_id, creator_role, created_at, expires_at, open_count
        )
        VALUES (?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (
            link_token,
            safe_geo,
            requisites.get("id"),
            handler.get("id"),
            amount,
            sanitize_currency_code(currency_code) or DEFAULT_CURRENCY,
            safe_language,
            clean_label or None,
            clean_landing or None,
            payment_comment_cleaned or None,
            handler.get("manager_name"),
            handler.get("username"),
            handler.get("manager_telegram_url"),
            handler.get("channel_name") or "",
            handler.get("channel_logo_url") or "",
            requisites.get("bank_name"),
            requisites.get("card_number"),
            requisites.get("bic_swift") or "",
            requisites.get("receiver_name"),
            creator_user_id,
            sanitize_bot_role(creator_role, "handler"),
            created_at.isoformat(),
            expires_at.isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    return get_payment_link_by_token(link_token) or {}


def resolve_payment_link_context(link_token: str, mark_opened: bool = True) -> dict[str, Any] | None:
    record = get_payment_link_by_token(link_token, mark_opened=mark_opened)
    if record is None:
        return None
    snapshot_handler = {
        "id": record.get("handler_user_id"),
        "manager_name": record.get("snapshot_handler_name") or "Обработчик",
        "manager_telegram_url": record.get("snapshot_handler_telegram_url") or "",
        "username": record.get("snapshot_handler_username") or "",
        "channel_name": record.get("snapshot_channel_name") or "",
        "channel_logo_url": record.get("snapshot_channel_logo_url") or "",
    }
    handler = snapshot_handler
    return {
        "record": record,
        "handler": handler,
        "requisites": {
            "id": record.get("requisites_id"),
            "geo_code": record.get("geo_code"),
            "bank_name": record.get("snapshot_bank_name"),
            "card_number": record.get("snapshot_card_number"),
            "bic_swift": record.get("snapshot_bic_swift") or "",
            "receiver_name": record.get("snapshot_receiver_name"),
            "created_at": record.get("created_at"),
        },
    }


def log_bot_activity(
    actor_user_id: int | None,
    action_type: str,
    geo_code: str | None = None,
    target_user_id: int | None = None,
    payload: str = "",
) -> None:
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


def save_bot_user_role(actor_id: int, target_user_id: int, role: str, channel_id: int | None = None) -> dict[str, Any]:
    safe_role = sanitize_bot_role(role, "handler")
    safe_channel_id = channel_id if safe_role == "handler" and channel_id and channel_id > 0 else None
    now_value = utc_now_iso()
    conn = get_connection()
    if safe_channel_id and get_channel_by_id(safe_channel_id, conn) is None:
        conn.close()
        raise HTTPException(status_code=400, detail="Выбранный канал не найден")
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
            INSERT INTO bot_admins (user_id, username, full_name, role, channel_id, added_at, added_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (target_user_id, "", "", safe_role, safe_channel_id, now_value, actor_id),
        )
    else:
        conn.execute(
            """
            UPDATE bot_admins
            SET role = ?, channel_id = ?
            WHERE user_id = ?
            """,
            (safe_role, safe_channel_id, target_user_id),
        )
    conn.commit()
    conn.close()
    row = next((item for item in list_bot_admins() if int(item.get("user_id") or 0) == target_user_id), None)
    return row if row is not None else {
        "user_id": target_user_id,
        "username": "",
        "full_name": "",
        "role": safe_role,
        "channel_id": safe_channel_id,
        "channel_name": get_channel_by_id(safe_channel_id).get("channel_name") if safe_channel_id and get_channel_by_id(safe_channel_id) else "",
        "channel_logo_url": get_channel_by_id(safe_channel_id).get("logo_url") if safe_channel_id and get_channel_by_id(safe_channel_id) else "",
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
        UPDATE geo_requisites
        SET sequence_number = COALESCE(sequence_number, 0) + 1
        WHERE geo_code = ?
        """,
        (safe_geo,),
    )
    conn.execute(
        """
        INSERT INTO geo_requisites (geo_code, sequence_number, bank_name, card_number, bic_swift, receiver_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (safe_geo, 1, row["bank_name"], row["card_number"], row["bic_swift"], row["receiver_name"], utc_now_iso()),
    )
    conn.commit()
    conn.close()
    return get_active_requisites(safe_geo)


def delete_geo_requisites_history_item(geo_code: str, history_id: int) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
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

    conn.execute("DELETE FROM geo_requisites WHERE id = ? AND geo_code = ?", (history_id, safe_geo))
    conn.commit()
    conn.close()
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
    active_links = conn.execute("SELECT COUNT(*) AS value FROM payment_links WHERE status = 'active'").fetchone()["value"]
    expired_links = conn.execute("SELECT COUNT(*) AS value FROM payment_links WHERE status = 'expired'").fetchone()["value"]

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
        "active_links": active_links,
        "expired_links": expired_links,
        "top_countries": [{"label": row["label"], "count": row["count_value"]} for row in top_countries_rows],
        "top_languages": [{"label": row["label"], "count": row["count_value"]} for row in top_languages_rows],
        "top_geos": [{"label": row["label"], "count": row["count_value"]} for row in top_geos_rows],
    }


def build_admin_dashboard_payload(session: dict[str, Any]) -> dict[str, Any]:
    role = sanitize_bot_role(session.get("role"), "handler")
    permissions = web_permissions_for_role(role)
    payload: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "web_url": WEB_URL,
        "current_user": {
            "user_id": session.get("user_id"),
            "username": session.get("username"),
            "full_name": session.get("full_name"),
            "role": role,
        },
        "permissions": permissions,
        "geos": list_geo_snapshots(),
        "requisites_history": list_geo_requisites_history(),
        "languages": LANGUAGE_OPTIONS,
        "currencies": CURRENCY_OPTIONS,
        "handlers": list_handler_contacts() if bot_role_has_permission(role, "create_link") else [],
        "bot_users": list_bot_admins() if bot_role_has_permission(role, "manage_access") else [],
        "bot_roles": BOT_ROLE_OPTIONS if bot_role_has_permission(role, "manage_access") else [],
        "channels": list_channels() if bot_role_has_permission(role, "manage_access") else [],
        "stats": get_summary_stats() if role == "admin" else {},
        "worker_stats": get_worker_stats() if role == "admin" else [],
        "bot_activity": list_bot_activity() if role == "admin" else [],
        "visits": list_visits() if role == "admin" else [],
        "payment_links": list_payment_links() if role == "admin" else [],
        "bot": get_bot_status() if role == "admin" else {},
        "db_path": str(DB_FILE) if role == "admin" else "",
    }
    return payload


def cleanup_sessions() -> None:
    now_value = utc_now()
    expired_tokens = [
        token
        for token, session in ADMIN_SESSIONS.items()
        if session["created_at"] + timedelta(hours=SESSION_TTL_HOURS) < now_value
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


def build_session_user(bot_user: dict[str, Any] | None, fallback_login: str = "") -> dict[str, Any]:
    if bot_user:
        return {
            "user_id": int(bot_user.get("user_id") or 0),
            "username": bot_user.get("username") or "",
            "full_name": bot_user.get("full_name") or "",
            "role": sanitize_bot_role(bot_user.get("role"), "handler"),
        }
    return {
        "user_id": 0,
        "username": fallback_login,
        "full_name": fallback_login or "Admin",
        "role": "admin",
    }


def resolve_web_login_user(login_value: str) -> dict[str, Any] | None:
    bot_user = get_bot_admin_for_login(login_value)
    if bot_user is not None:
        return build_session_user(bot_user)
    if ADMIN_USERNAME and secrets.compare_digest(login_value.strip(), ADMIN_USERNAME):
        return build_session_user(None, fallback_login=ADMIN_USERNAME)
    return None


def create_admin_session(user: dict[str, Any]) -> str:
    cleanup_sessions()
    token = secrets.token_urlsafe(32)
    ADMIN_SESSIONS[token] = {
        "created_at": utc_now(),
        "user_id": int(user.get("user_id") or 0),
        "username": str(user.get("username") or ""),
        "full_name": str(user.get("full_name") or ""),
        "role": sanitize_bot_role(user.get("role"), "admin"),
    }
    return token


def get_admin_session(request: Request) -> dict[str, Any]:
    cleanup_sessions()
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token or token not in ADMIN_SESSIONS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return ADMIN_SESSIONS[token]


def ensure_admin_session(request: Request) -> dict[str, Any]:
    return get_admin_session(request)


def ensure_admin_permission(request: Request, permission: str) -> dict[str, Any]:
    session = get_admin_session(request)
    if not bot_role_has_permission(session.get("role"), permission):
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    return session


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


try:
    init_db()
    logger.info("Database initialized successfully: %s", str(DB_FILE))
except Exception:
    logger.exception("Database initialization failed: %s", str(DB_FILE))
    raise

# --- TELEGRAM BOT ---

def build_bot_application() -> Application | None:
    if not BOT_ENABLED:
        return None

    builder = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .read_timeout(TELEGRAM_READ_TIMEOUT)
        .write_timeout(TELEGRAM_WRITE_TIMEOUT)
        .pool_timeout(TELEGRAM_POOL_TIMEOUT)
        .get_updates_connect_timeout(TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT)
        .get_updates_read_timeout(TELEGRAM_GET_UPDATES_READ_TIMEOUT)
        .get_updates_write_timeout(TELEGRAM_GET_UPDATES_WRITE_TIMEOUT)
        .get_updates_pool_timeout(TELEGRAM_GET_UPDATES_POOL_TIMEOUT)
    )

    if TELEGRAM_PROXY_URL:
        builder = builder.proxy(TELEGRAM_PROXY_URL).get_updates_proxy(TELEGRAM_PROXY_URL)
        logger.warning("Telegram proxy is enabled")

    return builder.build()


bot_app = build_bot_application()


def is_bot_runtime_healthy() -> bool:
    if bot_app is None:
        return False
    runtime_running = bool(getattr(bot_app, "running", False))
    updater = getattr(bot_app, "updater", None)
    updater_running = updater is None or bool(getattr(updater, "running", False))
    return BOT_RUNTIME_STARTED and runtime_running and updater_running


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
        "runtime_healthy": is_bot_runtime_healthy(),
        "startup_attempts": BOT_RUNTIME_ATTEMPTS,
        "last_attempt_at": BOT_RUNTIME_LAST_ATTEMPT_AT,
        "last_success_at": BOT_RUNTIME_LAST_SUCCESS_AT,
        "control_panel_ready": BOT_ENABLED and admins_total > 0 and is_bot_runtime_healthy(),
        "web_url": WEB_URL,
        "error": BOT_RUNTIME_ERROR,
    }


def summarize_update(update: object) -> dict[str, Any]:
    if not isinstance(update, Update):
        return {"raw_update_type": type(update).__name__}

    message = update.effective_message
    callback_query = update.callback_query
    return {
        "update_id": update.update_id,
        "user_id": update.effective_user.id if update.effective_user else None,
        "chat_id": update.effective_chat.id if update.effective_chat else None,
        "message_text": (message.text or "")[:200] if message and message.text else "",
        "callback_data": (callback_query.data or "")[:200] if callback_query and callback_query.data else "",
    }


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = getattr(context, "error", None)
    update_summary = summarize_update(update)
    if error is None:
        logger.error("Telegram handler error without exception details: %s", update_summary)
        return
    logger.error("Telegram handler error: %s", update_summary, exc_info=error)


async def stop_bot_runtime() -> None:
    global BOT_RUNTIME_STARTED

    if bot_app is None:
        return

    updater = getattr(bot_app, "updater", None)
    if updater is not None and getattr(updater, "running", False):
        try:
            await updater.stop()
        except Exception:
            logger.exception("Telegram updater shutdown failed")

    if getattr(bot_app, "running", False):
        try:
            await bot_app.stop()
        except Exception:
            logger.exception("Telegram application stop failed")

    try:
        await bot_app.shutdown()
    except Exception:
        logger.debug("Telegram application shutdown skipped or failed", exc_info=True)

    BOT_RUNTIME_STARTED = False


async def start_bot_runtime() -> bool:
    global BOT_RUNTIME_ATTEMPTS, BOT_RUNTIME_ERROR, BOT_RUNTIME_LAST_ATTEMPT_AT, BOT_RUNTIME_LAST_SUCCESS_AT, BOT_RUNTIME_STARTED

    if bot_app is None:
        return False
    if BOT_RUNTIME_STARTED:
        return True

    BOT_RUNTIME_ATTEMPTS += 1
    BOT_RUNTIME_LAST_ATTEMPT_AT = utc_now_iso()
    logger.info("Initializing Telegram bot runtime (attempt %s)", BOT_RUNTIME_ATTEMPTS)

    try:
        await bot_app.initialize()
        me = bot_app.bot.bot
        await bot_app.start()
        if bot_app.updater is not None:
            await bot_app.updater.start_polling()
        BOT_RUNTIME_STARTED = True
        BOT_RUNTIME_ERROR = None
        BOT_RUNTIME_LAST_SUCCESS_AT = utc_now_iso()
        logger.info("Telegram bot is ready: @%s (id=%s)", me.username or "unknown", me.id)
        logger.info("Telegram bot polling started successfully")
        return True
    except Exception as exc:
        BOT_RUNTIME_STARTED = False
        BOT_RUNTIME_ERROR = f"{type(exc).__name__}: {exc}"
        logger.exception("Telegram bot startup failed on attempt %s", BOT_RUNTIME_ATTEMPTS)
        await stop_bot_runtime()
        return False


async def supervise_bot_runtime() -> None:
    global BOT_RUNTIME_ERROR

    logger.info(
        "Telegram bot supervisor started with retry interval %s seconds and healthcheck interval %s seconds",
        BOT_STARTUP_RETRY_SECONDS,
        BOT_HEALTHCHECK_SECONDS,
    )
    try:
        while not BOT_RUNTIME_SHUTDOWN:
            if not BOT_RUNTIME_STARTED:
                started = await start_bot_runtime()
                if not started and not BOT_RUNTIME_SHUTDOWN:
                    logger.warning("Telegram bot is offline; next retry in %s seconds", BOT_STARTUP_RETRY_SECONDS)
                    await asyncio.sleep(BOT_STARTUP_RETRY_SECONDS)
                    continue
            if BOT_RUNTIME_STARTED and not is_bot_runtime_healthy():
                BOT_RUNTIME_ERROR = "Runtime became unhealthy and restart was requested"
                logger.warning("Telegram bot runtime became unhealthy; restarting bot")
                await stop_bot_runtime()
                if not BOT_RUNTIME_SHUTDOWN:
                    await asyncio.sleep(BOT_STARTUP_RETRY_SECONDS)
                    continue
            await asyncio.sleep(BOT_HEALTHCHECK_SECONDS)
    except asyncio.CancelledError:
        logger.info("Telegram bot supervisor cancelled")
        raise


def get_selected_geo(context: ContextTypes.DEFAULT_TYPE, user_id: int | None = None) -> str:
    selected_geo = sanitize_geo_code(str(context.user_data.get("selected_geo", "")))
    if not selected_geo:
        selected_geo = get_bot_admin_selected_geo(user_id)
    if selected_geo:
        context.user_data["selected_geo"] = selected_geo
        return selected_geo
    selected_geo = get_first_available_geo_code()
    if selected_geo:
        context.user_data["selected_geo"] = selected_geo
        return selected_geo
    fallback = list_known_geo_codes()
    if fallback:
        context.user_data["selected_geo"] = fallback[0]
        return fallback[0]
    return "ES"


def main_keyboard(role: str | None) -> ReplyKeyboardMarkup:
    safe_role = sanitize_bot_role(role, "handler")
    rows: list[list[str]] = []
    is_processor = (
        bot_role_has_permission(safe_role, "edit_requisites")
        and bot_role_has_permission(safe_role, "delete_requisites")
        and not bot_role_has_permission(safe_role, "view_requisites_history")
    )
    if is_processor:
        rows.append([ADD_REQUISITE_BTN, DELETE_REQUISITE_BTN])
    else:
        if bot_role_has_permission(safe_role, "select_geo"):
            rows.append(["🗺 Выбрать GEO", "📊 Активные реквизиты"])
        elif bot_role_has_permission(safe_role, "view_geo"):
            rows.append(["📊 Активные реквизиты"])
        if bot_role_has_permission(safe_role, "view_requisites_history"):
            rows.append(["📝 Реквизиты", "🗂 История реквизитов"])
            rows.append([DELETE_REQUISITE_BTN])
    if bot_role_has_permission(safe_role, "manage_access"):
        rows.append(["👥 Права доступа", ADD_GEO_BTN])
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
    codes = [profile["geo_code"] for profile in list_geo_profiles()]
    rows = [codes[index:index + 3] for index in range(0, len(codes), 3)] or [["ES"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def bot_select_geo_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура «Выбрать GEO» в боте: только DE, ES, LT (из существующих профилей)."""
    all_profiles = {p["geo_code"]: p for p in list_geo_profiles()}
    codes = [c for c in BOT_SELECT_GEO_WHITELIST if c in all_profiles]
    rows = [codes[index:index + 3] for index in range(0, len(codes), 3)] or [["ES"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def geo_picker_with_requisites_keyboard() -> ReplyKeyboardMarkup:
    codes = list_geo_codes_with_requisites()
    rows = [codes[index:index + 3] for index in range(0, len(codes), 3)] or [["ES"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def currency_picker_keyboard() -> ReplyKeyboardMarkup:
    rows = [[item["code"] for item in CURRENCY_OPTIONS]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def manager_action_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[MANAGER_KEEP_OPTION], [MANAGER_ADD_OPTION]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def build_geo_details_text(geo_code: str) -> str:
    profile = get_geo_profile(geo_code)
    requisites = get_active_requisites(geo_code)
    requisites_text = (
        f"Банк: {requisites['bank_name']}\n"
        f"IBAN: {requisites['card_number']}\n"
        f"BIC / SWIFT: {requisites['bic_swift'] or 'не указан'}\n"
        f"Получатель: {requisites['receiver_name']}"
        if requisites
        else "Реквизиты: отсутствуют"
    )
    return (
        f"Язык по умолчанию: {profile['default_language']}\n"
        f"Таймер: {profile['refresh_minutes']} мин\n"
        f"Обработчик выбирается при создании ссылки.\n\n"
        f"{requisites_text}"
    )


def build_geo_requisites_list_text(geo_code: str) -> str:
    items = list_geo_requisites_history_for_geo(geo_code, limit=1000)
    if not items:
        return "Реквизиты: отсутствуют"

    active_requisites = get_active_requisites(geo_code) or {}
    active_id = active_requisites.get("id")
    lines = [f"Реквизиты GEO {geo_code}:", ""]
    for item in items:
        active_suffix = " | АКТИВНЫЕ" if item["id"] == active_id else ""
        lines.append(f"#{item['sequence_number']} | ID {item['id']}{active_suffix}")
        lines.append(f"Банк: {item['bank_name']}")
        lines.append(f"IBAN: {item['card_number']}")
        lines.append(f"BIC / SWIFT: {item['bic_swift'] or 'не указан'}")
        lines.append(f"Получатель: {item['receiver_name']}")
        lines.append(f"Создано: {item['created_at']}")
        lines.append("")
    return "\n".join(lines).strip()


def build_geo_overview_text() -> str:
    blocks = ["Активные реквизиты:"]
    for snapshot in list_geo_snapshots():
        profile = snapshot["profile"]
        requisites = snapshot["active_requisites"]
        requisites_status = (
            f"{requisites['bank_name']} | {requisites['card_number']} | {requisites['bic_swift'] or 'BIC/SWIFT не указан'} | {requisites['receiver_name']}"
            if requisites
            else "Реквизиты отсутствуют"
        )
        blocks.append(
            f"\n{profile['geo_name']} | {profile['default_language']} | "
            f"таймер {profile['refresh_minutes']} мин\n"
            f"{requisites_status}"
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
        "Обработчик -> ссылки (сумма, валюта, комментарии, обработчик, язык)\n"
        "Процессор -> добавить/удалить реквизит (регион выбирается в потоке)\n"
        "Админ -> полный доступ и добавление новых регионов\n\n"
        "Добавить админа: /addadmin 123456789\n"
        "Поставить роль: /setrole 123456789 handler|processor|admin\n"
        "Удалить: /removeadmin 123456789\n"
        "Список: /admins"
    )
    return "\n".join(lines)


def build_help_text(role: str | None, geo_code: str) -> str:
    safe_role = sanitize_bot_role(role, "handler")
    is_processor = (
        bot_role_has_permission(safe_role, "edit_requisites")
        and bot_role_has_permission(safe_role, "delete_requisites")
        and not bot_role_has_permission(safe_role, "view_requisites_history")
    )
    if is_processor:
        return (
            "Что можно делать:\n\n"
            "➕ Добавить реквизит — выберите регион, затем отправьте 4 строки:\n"
            "Банк, IBAN, BIC/SWIFT (или -), Получатель\n\n"
            "🗑 Удалить реквизит — выберите регион, затем нажмите кнопку под записью."
        )
    lines = [
        "Что можно делать сейчас:",
        "",
    ]
    if bot_role_has_permission(safe_role, "select_geo"):
        lines.append("1. Сначала выберите регион, если нужен другой.")
    lines.append("2. Используйте доступные вам кнопки снизу.")
    if bot_role_has_permission(safe_role, "create_link"):
        lines.append("3. Для ссылки на оплату нажмите `🔗 Ссылка на оплату` и следуйте шагам.")
    if bot_role_has_permission(safe_role, "view_requisites_history"):
        lines.append("3. Для реквизитов нажмите `📝 Реквизиты`.")
        lines.append("4. История реквизитов открывается кнопкой `🗂 История реквизитов`.")
    if bot_role_has_permission(safe_role, "manage_access"):
        lines.append("5. Права доступа смотрите в `👥 Права доступа`.")
        lines.append("6. Новый регион создается кнопкой `➕ Добавить GEO`.")
    return "\n".join(lines)


def build_add_geo_text() -> str:
    options = ", ".join(f"{item['code']} ({item['label']})" for item in LANGUAGE_OPTIONS)
    return (
        "Отправьте новый регион четырьмя строками:\n"
        "1. Код региона\n"
        "2. Название региона\n"
        "3. Язык по умолчанию\n"
        "4. Таймер в минутах (или - для значения по умолчанию)\n\n"
        "Пример:\n"
        "PL\n"
        "Poland\n"
        "es\n"
        "15\n\n"
        f"Доступные языки: {options}"
    )


def build_requisites_history_text(geo_code: str, action: str = "restore") -> str:
    items = list_geo_requisites_history_for_geo(geo_code)
    if not items:
        return (
            "История реквизитов пока пуста.\n\n"
            "Сначала сохраните хотя бы один комплект реквизитов."
        )

    action_hint = (
        "Нажмите кнопку под нужной записью, чтобы снова сделать её активной."
        if action == "restore"
        else "Нажмите кнопку под нужной записью, чтобы удалить запись из истории."
    )
    lines = [
        "История реквизитов:",
        action_hint,
        "",
    ]
    for item in items:
        lines.append(
            f"#{item['sequence_number']} | ID {item['id']} | {item['bank_name']} | {item['card_number']} | {item['bic_swift'] or 'без BIC/SWIFT'} | "
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
    items = list_handler_contacts()
    lines = [
        f"Текущий GEO: {geo_code}",
        "Выберите обработчика.",
        "Отправьте @username обработчика из списка ниже.",
        "На лендинг попадет именно эта Telegram-ссылка.",
    ]
    for item in items:
        channel_suffix = f" | канал: {item['channel_name']}" if item.get("channel_name") else ""
        lines.append(
            f"{item['manager_name']} | @{item['username']}{channel_suffix}"
        )
    if not items:
        lines.append("")
        lines.append("Нет ни одного обработчика с username. Администратор должен назначить роль handler и пользователь должен зайти в бота хотя бы один раз.")
    return "\n".join(lines)


def build_link_language_selection_text() -> str:
    options = ", ".join(item["code"] for item in LANGUAGE_OPTIONS)
    return (
        "Выберите язык лендинга.\n"
        f"По умолчанию будет `{DEFAULT_CURRENCY}` только для валюты, а язык выбирается явно.\n"
        f"Доступно: {options}"
    )


def build_landing_comment_selection_text() -> str:
    return "Выберите кнопку с комментарием для лендинга"


def build_payment_comment_prompt_text() -> str:
    return "Какой комментарий к платежу прописать? (клиент увидит это на лендинге и должен указать при переводе). Можно отправить «-» или пустое сообщение, если не нужен."


def landing_comment_picker_keyboard() -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
    current_row: list[str] = []
    for item in LANDING_COMMENT_OPTIONS:
        current_row.append(item["button"])
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def build_link_currency_selection_text() -> str:
    options = ", ".join(f"{item['code']} ({item['label']})" for item in CURRENCY_OPTIONS)
    return (
        "Выберите валюту платежа.\n"
        f"По умолчанию используется {DEFAULT_CURRENCY}.\n"
        f"Доступно: {options}"
    )


def build_link_geo_selection_text(selected_geo: str | None = None) -> str:
    available = list_geo_codes_with_requisites()
    preferred = sanitize_geo_code(selected_geo)
    lines = [
        f"Выберите GEO для реквизита. Текущее GEO: {preferred or 'не выбрано'}",
        "На лендинг подтянется первый доступный комплект реквизитов выбранного GEO.",
        "",
        f"Доступные GEO: {', '.join(available) if available else 'нет доступных GEO'}",
    ]
    return "\n".join(lines)


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
    role = get_bot_user_role(user_id)
    selected_geo = get_selected_geo(context, user_id)
    is_processor = (
        bot_role_has_permission(role, "edit_requisites")
        and bot_role_has_permission(role, "delete_requisites")
        and not bot_role_has_permission(role, "view_requisites_history")
    )
    if is_processor:
        await update.effective_message.reply_text(
            "Внутренняя панель команды активна.\n\n"
            f"Ваша роль: {get_bot_role_label(role)}\n\n"
            "Добавить реквизит — выберите регион и введите данные.\n"
            "Удалить реквизит — выберите регион и выберите запись для удаления.",
            reply_markup=main_keyboard(role),
        )
    else:
        await update.effective_message.reply_text(
            "Внутренняя панель команды активна.\n\n"
            f"Ваша роль: {get_bot_role_label(role)}\n\n"
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
        "Выберите регион.",
        reply_markup=bot_select_geo_keyboard(),
    )
    return WAITING_GEO_SELECTION


async def select_geo_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    geo_code = sanitize_geo_code(update.effective_message.text)
    if not geo_code:
        available = ", ".join(c for c in BOT_SELECT_GEO_WHITELIST)
        await update.effective_message.reply_text(f"Выберите регион из списка: {available}", reply_markup=bot_select_geo_keyboard())
        return WAITING_GEO_SELECTION
    if geo_code not in BOT_SELECT_GEO_WHITELIST:
        await update.effective_message.reply_text(
            f"В этом меню доступны только: {', '.join(BOT_SELECT_GEO_WHITELIST)}.",
            reply_markup=bot_select_geo_keyboard(),
        )
        return WAITING_GEO_SELECTION

    context.user_data["selected_geo"] = geo_code
    set_bot_admin_selected_geo(update.effective_user.id if update.effective_user else None, geo_code)
    log_bot_activity(update.effective_user.id if update.effective_user else None, "select_geo", geo_code=geo_code)
    await update.effective_message.reply_text(
        build_geo_details_text(geo_code),
        reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
    )
    return ConversationHandler.END


async def add_requisite_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "edit_requisites"):
        return ConversationHandler.END
    available = [p["geo_code"] for p in list_geo_profiles()]
    await update.effective_message.reply_text(
        "Выберите регион для добавления реквизита.\n"
        f"Доступно: {', '.join(available) if available else 'нет'}",
        reply_markup=geo_picker_keyboard(),
    )
    return WAITING_ADD_REQ_GEO


async def add_geo_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "manage_access"):
        return ConversationHandler.END
    await update.effective_message.reply_text(build_add_geo_text())
    return WAITING_NEW_GEO_DETAILS


async def add_geo_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    incoming_text = update.effective_message.text
    if is_menu_button_text(incoming_text):
        await update.effective_message.reply_text(
            "Создание региона отменено. Нажмите нужную кнопку еще раз.",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return ConversationHandler.END

    lines = [line.strip() for line in incoming_text.strip().splitlines() if line.strip()]
    if len(lines) < 3:
        await update.effective_message.reply_text(
            f"{build_add_geo_text()}\n\nНужно минимум 3 строки: код, название и язык."
        )
        return WAITING_NEW_GEO_DETAILS

    geo_code = normalize_geo_code(lines[0])
    if not geo_code:
        await update.effective_message.reply_text("Код региона должен содержать 2-12 символов: A-Z, 0-9, _ или -.")
        return WAITING_NEW_GEO_DETAILS
    if geo_code in set(list_known_geo_codes()):
        await update.effective_message.reply_text(
            "Регион уже существует. Используйте админку, если хотите поменять настройки."
        )
        return WAITING_NEW_GEO_DETAILS

    geo_name = lines[1].strip()
    if not geo_name:
        await update.effective_message.reply_text("Название региона не может быть пустым.")
        return WAITING_NEW_GEO_DETAILS

    default_language = sanitize_language_code(lines[2])
    if not default_language:
        available = ", ".join(item["code"] for item in LANGUAGE_OPTIONS)
        await update.effective_message.reply_text(f"Язык должен быть одним из кодов: {available}")
        return WAITING_NEW_GEO_DETAILS

    timer_line = lines[3] if len(lines) >= 4 else "-"
    refresh_minutes = DEFAULT_REFRESH_MINUTES if timer_line == "-" else parse_optional_int(timer_line)
    if refresh_minutes is None or refresh_minutes < 1 or refresh_minutes > 120:
        await update.effective_message.reply_text("Таймер должен быть числом от 1 до 120 или символом `-`.")
        return WAITING_NEW_GEO_DETAILS

    save_geo_configuration(
        geo_code,
        GeoConfigPayload(
            geo_name=geo_name,
            default_language=default_language,
            refresh_minutes=refresh_minutes,
            default_manager_id=None,
        ),
    )
    user_id = update.effective_user.id if update.effective_user else None
    context.user_data["selected_geo"] = geo_code
    set_bot_admin_selected_geo(user_id, geo_code)
    log_bot_activity(user_id, "create_geo", geo_code=geo_code, payload=geo_name)
    await update.effective_message.reply_text(
        f"Регион создан.\n\n{build_geo_details_text(geo_code)}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def add_req_geo_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    geo_code = sanitize_geo_code(update.effective_message.text)
    if not geo_code:
        available = ", ".join(p["geo_code"] for p in list_geo_profiles())
        await update.effective_message.reply_text(f"Выберите регион из списка: {available}")
        return WAITING_ADD_REQ_GEO
    context.user_data["selected_geo"] = geo_code
    requisites = get_active_requisites(geo_code)
    if not requisites:
        await update.effective_message.reply_text(
            "Реквизитов пока нет.\n\n"
            "Отправьте 4 строки в формате:\n"
            "Банк\nIBAN\nBIC/SWIFT или -\nПолучатель"
        )
        return WAITING_REQUISITES
    await update.effective_message.reply_text(
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


async def change_reqs_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "edit_requisites"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    requisites = get_active_requisites(selected_geo)
    if not requisites:
        await update.effective_message.reply_text(
            "Реквизитов пока нет.\n\n"
            "Отправьте 4 строки в формате:\n"
            "Банк\nIBAN\nBIC/SWIFT или -\nПолучатель"
        )
        return WAITING_REQUISITES
    await update.effective_message.reply_text(
        f"{build_geo_requisites_list_text(selected_geo)}\n\n"
        "Отправьте новые реквизиты четырьмя строками:\n"
        "Банк\n"
        "IBAN\n"
        "BIC / SWIFT (можно оставить пустым, поставив -)\n"
        "Получатель"
    )
    return WAITING_REQUISITES


async def change_reqs_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    incoming_text = update.effective_message.text
    if is_menu_button_text(incoming_text):
        await update.effective_message.reply_text(
            "Текущий ввод реквизитов отменен. Нажмите нужную кнопку еще раз.",
            reply_markup=main_keyboard(get_bot_user_role(update.effective_user.id if update.effective_user else None)),
        )
        return ConversationHandler.END

    lines = [line.strip() for line in update.effective_message.text.strip().splitlines() if line.strip()]
    if len(lines) < 4:
        await update.effective_message.reply_text("Нужно 4 строки: банк, IBAN, BIC / SWIFT и получатель.")
        return WAITING_REQUISITES

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
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


async def show_requisites_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    if not await admin_check(update, "delete_requisites"):
        return None
    user_id = update.effective_user.id if update.effective_user else None
    role = get_bot_user_role(user_id)
    is_processor = (
        bot_role_has_permission(role, "delete_requisites")
        and not bot_role_has_permission(role, "view_requisites_history")
    )
    if is_processor:
        available = [p["geo_code"] for p in list_geo_profiles()]
        await update.effective_message.reply_text(
            "Выберите GEO для удаления реквизита.\n"
            f"Доступно: {', '.join(available) if available else 'нет'}",
            reply_markup=geo_picker_keyboard(),
        )
        return WAITING_DELETE_REQ_GEO
    selected_geo = get_selected_geo(context, user_id)
    await update.effective_message.reply_text(
        f"{build_requisites_history_text(selected_geo, 'delete')}\n\n"
        "Последний комплект реквизитов удалить нельзя.",
        reply_markup=requisites_history_keyboard(selected_geo, "delete"),
    )
    return ConversationHandler.END


async def delete_req_geo_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    geo_code = sanitize_geo_code(update.effective_message.text)
    if not geo_code:
        available = ", ".join(p["geo_code"] for p in list_geo_profiles())
        await update.effective_message.reply_text(f"Выберите регион из списка: {available}")
        return WAITING_DELETE_REQ_GEO
    user_id = update.effective_user.id if update.effective_user else None
    await update.effective_message.reply_text(
        f"{build_requisites_history_text(geo_code, 'delete')}\n\n"
        "Последний комплект реквизитов удалить нельзя.",
        reply_markup=requisites_history_keyboard(geo_code, "delete"),
    )
    await update.effective_message.reply_text(
        "Нажмите кнопку под записью для удаления.",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def requisites_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    if not await admin_check(update):
        await query.answer()
        return
    user_id = update.effective_user.id if update.effective_user else None
    role = get_bot_user_role(user_id)
    has_view = bot_role_has_permission(role, "view_requisites_history")
    has_delete = bot_role_has_permission(role, "delete_requisites")
    if not has_view and not has_delete:
        await query.answer("Недостаточно прав")
        return
    payload = (query.data or "").split(":")
    if len(payload) < 4:
        await query.answer()
        return
    _, action, geo_code, history_id_raw = payload[0], payload[1], payload[2], payload[3]
    if payload[1] == "list":
        if not has_view:
            await query.answer("Недостаточно прав")
            return
        action = payload[2]
        geo_code = payload[3]
        await query.edit_message_text(
            build_requisites_history_text(geo_code, action),
            reply_markup=requisites_history_keyboard(geo_code, action),
        )
        await query.answer("Список обновлен")
        return
    history_id = parse_optional_int(history_id_raw)
    if history_id is None or history_id <= 0:
        await query.answer("Некорректный ID")
        return
    try:
        if action == "restore":
            if not has_view:
                await query.answer("Недостаточно прав")
                return
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
            if not has_delete:
                await query.answer("Недостаточно прав")
                return
            delete_geo_requisites_history_item(geo_code, history_id)
            log_bot_activity(user_id, "delete_requisites", geo_code=geo_code, payload=str(history_id))
            if not has_view:
                await query.edit_message_text(f"Реквизиты ID {history_id} удалены для {geo_code}.")
                await query.message.reply_text(
                    "Реквизиты удалены.",
                    reply_markup=main_keyboard(get_bot_user_role(user_id)),
                )
            else:
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
    choice = (update.effective_message.text or "").strip()
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
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
    lines = [line.strip() for line in update.effective_message.text.strip().splitlines()]
    if len(lines) < 2 or not lines[0]:
        await update.effective_message.reply_text("Нужно 2 строки: имя менеджера и Telegram-ссылка.")
        return WAITING_MANAGER

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
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


async def send_ready_payment_link(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    amount: float,
    user_id: int | None,
    selected_geo: str,
    currency_code: str | None = None,
    requisites_id: int | None = None,
    manager_id: int | None = None,
    manager_link: str | None = None,
    forced_language: str | None = None,
    clean_label: str = "",
    clean_comment: str = "",
    payment_comment: str = "",
) -> int:
    handler_contact = resolve_handler_contact(manager_id, manager_link)
    try:
        link_record = create_payment_link_record(
            amount=amount,
            geo_code=selected_geo,
            creator_user_id=user_id,
            creator_role=get_bot_user_role(user_id),
            currency_code=currency_code,
            label=clean_label,
            landing_comment=clean_comment,
            payment_comment=payment_comment,
            forced_language=forced_language,
            handler_user_id=manager_id,
        )
        link = build_payment_link_url(
            str(link_record.get("link_token") or ""),
            link_record.get("forced_language"),
        )
    except HTTPException as exc:
        await update.effective_message.reply_text(
            str(exc.detail),
            reply_markup=main_keyboard(get_bot_user_role(user_id)),
        )
        return ConversationHandler.END

    handler = handler_contact or {
        "manager_name": "не выбран",
        "manager_telegram_url": "",
    }

    context.user_data.pop("temp_amount", None)
    context.user_data.pop("temp_requisites_id", None)
    context.user_data.pop("temp_manager_id", None)
    context.user_data.pop("temp_manager_link", None)
    context.user_data.pop("temp_language", None)
    context.user_data.pop("temp_label", None)
    context.user_data.pop("temp_comment", None)
    context.user_data.pop("temp_payment_comment", None)

    log_bot_activity(user_id, "create_link", geo_code=selected_geo, payload=f"{amount:.2f}")
    language_label = LANDING_COMMENT_BUTTON_BY_CODE.get(forced_language or "", forced_language or "auto")
    await update.effective_message.reply_text(
        f"Ссылка готова.\n\n"
        f"Сумма: {amount:.2f} {sanitize_currency_code(currency_code) or DEFAULT_CURRENCY}\n"
        f"Реквизиты: зафиксирован текущий активный комплект GEO\n"
        f"Обработчик: {handler.get('manager_name') or 'не выбран'}\n"
        f"Комментарий для лендинга: {clean_comment or 'не указан'}\n"
        f"Комментарий к платежу: {payment_comment or 'не указан'}\n"
        f"Язык лендинга: {language_label}\n"
        f"Ссылка: {link}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return ConversationHandler.END


async def create_link_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update, "create_link"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else None
    context.user_data.pop("temp_amount", None)
    context.user_data.pop("temp_requisites_id", None)
    context.user_data.pop("temp_manager_id", None)
    context.user_data.pop("temp_manager_link", None)
    context.user_data.pop("temp_language", None)
    context.user_data.pop("temp_label", None)
    context.user_data.pop("temp_comment", None)
    context.user_data.pop("temp_payment_comment", None)
    context.user_data.pop("temp_currency", None)
    context.user_data.pop("temp_geo", None)
    await update.effective_message.reply_text("Введите сумму к оплате, например: 250")
    return WAITING_LINK_AMOUNT


async def create_link_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount = parse_payment_amount(update.effective_message.text)
    if amount is None:
        await update.effective_message.reply_text("Нужно ввести положительную сумму, например: 250")
        return WAITING_LINK_AMOUNT

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    context.user_data["temp_amount"] = amount
    context.user_data["temp_geo"] = selected_geo
    await update.effective_message.reply_text(
        build_link_currency_selection_text(),
        reply_markup=currency_picker_keyboard(),
    )
    return WAITING_LINK_CURRENCY


async def create_link_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_value = update.effective_message.text.strip().upper()
    currency_code = sanitize_currency_code(raw_value) or (DEFAULT_CURRENCY if raw_value in {"-", "DEFAULT"} else None)
    if currency_code is None:
        available = ", ".join(item["code"] for item in CURRENCY_OPTIONS)
        await update.effective_message.reply_text(f"Нужна одна из валют: {available}")
        return WAITING_LINK_CURRENCY

    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = get_selected_geo(context, user_id)
    context.user_data["temp_currency"] = currency_code

    await update.effective_message.reply_text(
        build_link_geo_selection_text(selected_geo),
        reply_markup=geo_picker_with_requisites_keyboard(),
    )
    return WAITING_LINK_REQUISITES


async def create_link_requisites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    geo_code = sanitize_geo_code(update.effective_message.text)
    if not geo_code or not geo_has_requisites(geo_code):
        available = ", ".join(list_geo_codes_with_requisites())
        await update.effective_message.reply_text(
            f"Нужно выбрать GEO с реквизитами. Доступно: {available}"
        )
        return WAITING_LINK_REQUISITES

    context.user_data["temp_geo"] = geo_code
    context.user_data["temp_label"] = ""
    active_requisites = get_active_requisites(geo_code)
    if not active_requisites:
        await update.effective_message.reply_text(
            f"Для GEO {geo_code} сейчас нет реквизитов."
        )
        return WAITING_LINK_REQUISITES
    context.user_data["temp_requisites_id"] = None
    await update.effective_message.reply_text(
        build_landing_comment_selection_text(),
        reply_markup=landing_comment_picker_keyboard(),
    )
    return WAITING_LINK_COMMENT


async def create_link_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_text = update.effective_message.text.strip()
    handler = get_handler_contact_by_username(raw_text)
    if handler is None:
        await update.effective_message.reply_text(
            "Нужно отправить @username обработчика из списка."
        )
        return WAITING_LINK_MANAGER

    context.user_data["temp_manager_id"] = handler.get("id")
    context.user_data["temp_manager_link"] = handler.get("manager_telegram_url")
    amount = float(context.user_data.get("temp_amount", 0))
    user_id = update.effective_user.id if update.effective_user else None
    selected_geo = str(context.user_data.get("temp_geo") or get_selected_geo(context, user_id))
    requisites_id = context.user_data.get("temp_requisites_id")
    manager_id = context.user_data.get("temp_manager_id")
    manager_link = normalize_manager_link(context.user_data.get("temp_manager_link"))
    clean_label = str(context.user_data.get("temp_label", ""))
    clean_comment = str(context.user_data.get("temp_comment", ""))
    payment_comment_text = str(context.user_data.get("temp_payment_comment", ""))
    currency_code = context.user_data.get("temp_currency")
    safe_language = sanitize_language_code(context.user_data.get("temp_language"))
    return await send_ready_payment_link(
        update,
        context,
        amount=amount,
        user_id=user_id,
        selected_geo=selected_geo,
        currency_code=currency_code,
        requisites_id=requisites_id,
        manager_id=manager_id,
        manager_link=manager_link,
        forced_language=safe_language,
        clean_label=clean_label,
        clean_comment=clean_comment,
        payment_comment=payment_comment_text,
    )


async def create_link_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    selected_option = LANDING_COMMENT_BY_BUTTON.get(update.effective_message.text.strip().casefold())
    if selected_option is None:
        await update.effective_message.reply_text(
            "Нужно выбрать кнопку с комментарием ниже.",
            reply_markup=landing_comment_picker_keyboard(),
        )
        return WAITING_LINK_COMMENT

    context.user_data["temp_comment"] = selected_option["comment"]
    context.user_data["temp_language"] = selected_option["code"]
    await update.effective_message.reply_text(
        build_payment_comment_prompt_text(),
    )
    return WAITING_LINK_PAYMENT_COMMENT


async def create_link_payment_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_text = (update.effective_message.text or "").strip()
    context.user_data["temp_payment_comment"] = raw_text if raw_text and raw_text != "-" else ""
    selected_geo = str(context.user_data.get("temp_geo") or "")
    user_id = update.effective_user.id if update.effective_user else None
    await update.effective_message.reply_text(
        f"Для ссылки будет зафиксирован текущий активный комплект реквизитов GEO {selected_geo}.\n"
        "Теперь выберите ответственного обработчика.\n\n"
        f"{build_link_manager_selection_text(selected_geo)}",
        reply_markup=main_keyboard(get_bot_user_role(user_id)),
    )
    return WAITING_LINK_MANAGER


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
    conv_handler_add_req = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{re.escape(ADD_REQUISITE_BTN)}$"), add_requisite_start)],
        states={
            WAITING_ADD_REQ_GEO: [MessageHandler(conversation_text_filter, add_req_geo_received)],
            WAITING_REQUISITES: [MessageHandler(conversation_text_filter, change_reqs_save)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            MessageHandler(filters.Regex(MENU_BUTTONS_PATTERN), cancel_cmd),
        ],
    )
    conv_handler_add_geo = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{re.escape(ADD_GEO_BTN)}$"), add_geo_start)],
        states={
            WAITING_NEW_GEO_DETAILS: [MessageHandler(conversation_text_filter, add_geo_save)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            MessageHandler(filters.Regex(MENU_BUTTONS_PATTERN), cancel_cmd),
        ],
    )
    conv_handler_delete_req = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{re.escape(DELETE_REQUISITE_BTN)}$"), show_requisites_delete_start)],
        states={
            WAITING_DELETE_REQ_GEO: [MessageHandler(conversation_text_filter, delete_req_geo_received)],
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
            WAITING_LINK_CURRENCY: [MessageHandler(conversation_text_filter, create_link_currency)],
            WAITING_LINK_REQUISITES: [MessageHandler(conversation_text_filter, create_link_requisites)],
            WAITING_LINK_COMMENT: [MessageHandler(conversation_text_filter, create_link_comment)],
            WAITING_LINK_PAYMENT_COMMENT: [MessageHandler(conversation_text_filter, create_link_payment_comment)],
            WAITING_LINK_MANAGER: [MessageHandler(conversation_text_filter, create_link_manager)],
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
    bot_app.add_handler(MessageHandler(filters.Regex("^🛠 Админка$"), show_admin_panel_cmd))
    bot_app.add_handler(CallbackQueryHandler(requisites_history_callback, pattern=r"^req:"))
    bot_app.add_handler(conv_handler_geo)
    bot_app.add_handler(conv_handler_req)
    bot_app.add_handler(conv_handler_add_req)
    bot_app.add_handler(conv_handler_add_geo)
    bot_app.add_handler(conv_handler_delete_req)
    bot_app.add_handler(conv_handler_link)
    bot_app.add_error_handler(telegram_error_handler)


# --- FASTAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_RUNTIME_SHUTDOWN, BOT_RUNTIME_TASK

    logger.info("Application startup initiated")
    logger.info("Runtime snapshot: %s", build_runtime_snapshot())

    if bot_app is not None:
        BOT_RUNTIME_SHUTDOWN = False
        BOT_RUNTIME_TASK = asyncio.create_task(supervise_bot_runtime())
    else:
        logger.warning("Telegram bot is disabled because BOT_TOKEN is not configured")

    yield

    logger.info("Application shutdown initiated")
    BOT_RUNTIME_SHUTDOWN = True
    if BOT_RUNTIME_TASK is not None:
        try:
            BOT_RUNTIME_TASK.cancel()
            await BOT_RUNTIME_TASK
        except asyncio.CancelledError:
            pass
        finally:
            BOT_RUNTIME_TASK = None

    if bot_app is not None:
        await stop_bot_runtime()
        logger.info("Telegram bot stopped successfully")


app = FastAPI(lifespan=lifespan)
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


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.perf_counter()
    path = request.url.path
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.exception("HTTP %s %s failed in %.2f ms", request.method, path, elapsed_ms)
        raise

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    if path != "/api/health" and not path.startswith("/static/"):
        log_method = logger.warning if response.status_code >= 400 else logger.info
        log_method("HTTP %s %s -> %s in %.2f ms", request.method, path, response.status_code, elapsed_ms)
    return response


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
        "bot": get_bot_status(),
        "database_exists": DB_FILE.exists(),
    }


@app.get("/api/landing-context")
async def landing_context(
    request: Request,
    link: str | None = None,
    payment: str | None = None,
    currency: str | None = None,
    geo: str | None = None,
    req: int | None = None,
    mgr: int | None = None,
    mgr_link: str | None = None,
    lang: str | None = None,
    label: str | None = None,
    comment: str | None = None,
):
    visitor, browser_language = await build_visitor_context(request)
    invalid_reason = ""
    payment_link_token = ""
    payment_link_status = ""

    if link:
        landing_refresh = request.headers.get("x-landing-refresh") == "1"
        link_context = resolve_payment_link_context(link, mark_opened=not landing_refresh)
        if link_context is None:
            resolved_geo = resolve_geo_code(geo, visitor.get("country_code"), browser_language)
            profile = get_geo_profile(resolved_geo)
            handler = {
                "id": None,
                "manager_name": "",
                "manager_telegram_url": "",
                "channel_id": None,
                "channel_name": "",
                "channel_logo_url": "",
            }
            requisites = None
            payment_amount = None
            payment_currency = DEFAULT_CURRENCY
            payment_label = ""
            landing_comment = ""
            payment_comment = ""
            mode = "invalid"
            invalid_reason = "link_missing"
            refresh_seconds = 0
            recommended_language = resolve_recommended_language(
                explicit_language=lang,
                browser_language=browser_language,
                country_code=visitor.get("country_code"),
                geo_default_language=profile.get("default_language"),
            )
        else:
            payment_link = link_context["record"]
            payment_link_token = str(payment_link.get("link_token") or "")
            payment_link_status = str(payment_link.get("status") or "")
            resolved_geo = str(payment_link.get("geo_code") or "ES")
            profile = get_geo_profile(resolved_geo)
            handler = link_context["handler"]
            requisites = link_context["requisites"]
            payment_amount = payment_link.get("payment_amount")
            payment_currency = sanitize_currency_code(payment_link.get("payment_currency")) or DEFAULT_CURRENCY
            payment_label = str(payment_link.get("payment_label") or "")
            landing_comment = str(payment_link.get("landing_comment") or "")
            payment_comment = str(payment_link.get("payment_comment") or "")
            recommended_language = resolve_recommended_language(
                explicit_language=payment_link.get("forced_language"),
                browser_language=browser_language,
                country_code=visitor.get("country_code"),
                geo_default_language=profile.get("default_language"),
            )
            expires_at = parse_iso_datetime(payment_link.get("expires_at"))
            refresh_seconds = max(0, int((expires_at - utc_now()).total_seconds())) if expires_at else 0
            mode = "expired" if payment_link_status == "expired" or refresh_seconds <= 0 else "live"
            if mode == "live":
                fresh_req = get_active_requisites(resolved_geo)
                if fresh_req is not None:
                    requisites = fresh_req
                else:
                    mode = "invalid"
                    invalid_reason = "requisites_missing"
                    refresh_seconds = 0
            if mode == "expired":
                invalid_reason = "link_expired"
                refresh_seconds = 0
                if get_active_requisites(resolved_geo) is None:
                    mode = "invalid"
                    invalid_reason = "requisites_missing"
    else:
        resolved_geo = resolve_geo_code(geo, visitor.get("country_code"), browser_language)
        profile = get_geo_profile(resolved_geo)
        handler = resolve_handler_contact(mgr, mgr_link) or {
            "id": None,
            "manager_name": "",
            "manager_telegram_url": "",
            "channel_id": None,
            "channel_name": "",
            "channel_logo_url": "",
        }
        payment_amount = parse_payment_amount(payment)
        payment_currency = sanitize_currency_code(currency) or DEFAULT_CURRENCY
        payment_label = sanitize_payment_label(label)
        landing_comment = ""
        payment_comment = clean_payment_comment(comment)
        mode = "live" if payment_amount is not None else ("preview" if ALLOW_PREVIEW_MODE else "invalid")
        requisites = resolve_requisites_for_geo(resolved_geo, req) if mode != "invalid" else None
        if mode != "invalid" and requisites is None:
            mode = "invalid"
            invalid_reason = "requisites_missing"
        if mode != "invalid" and not normalize_manager_link(handler.get("manager_telegram_url")):
            mode = "invalid"
            invalid_reason = "manager_missing"
            requisites = None
        refresh_seconds = max(60, int(profile["refresh_minutes"]) * 60) if mode != "invalid" else 0
        recommended_language = resolve_recommended_language(
            explicit_language=lang,
            browser_language=browser_language,
            country_code=visitor.get("country_code"),
            geo_default_language=profile.get("default_language"),
        )

    visit_token = record_visit(
        mode=mode,
        visitor=visitor,
        recommended_language=recommended_language,
        geo_code=resolved_geo,
        payment_amount=payment_amount,
        payment_currency=payment_currency,
        payment_label=payment_label or None,
        payment_comment=payment_comment or None,
        manager=handler,
        requisites=requisites,
        request=request,
        payment_link_token=payment_link_token or None,
        payment_link_status=payment_link_status or None,
    )

    return {
        "mode": mode,
        "invalid_reason": invalid_reason,
        "recommended_language": recommended_language,
        "available_languages": LANGUAGE_OPTIONS,
        "payment": {
            "amount": payment_amount if payment_amount is not None else (DEFAULT_PREVIEW_AMOUNT if mode == "preview" else None),
            "currency": payment_currency,
            "label": payment_label,
            "landing_comment": landing_comment,
            "comment": payment_comment,
        },
        "geo": {
            **profile,
            "refresh_seconds": refresh_seconds,
        },
        "handler": handler,
        "manager": handler,
        "requisites": requisites,
        "visit": {"token": visit_token},
        "visitor": visitor,
        "link": {
            "token": payment_link_token,
            "status": payment_link_status,
        },
        "timer": {
            "refresh_seconds": refresh_seconds,
            "expires_at": (utc_now() + timedelta(seconds=refresh_seconds)).isoformat() if refresh_seconds else None,
        },
    }


@app.post("/api/landing-client")
async def landing_client(payload: LandingClientPayload):
    raise HTTPException(status_code=410, detail="Механика сохранения клиента удалена")


@app.get("/api/admin/session")
async def admin_session(request: Request):
    cleanup_sessions()
    token = request.cookies.get(SESSION_COOKIE_NAME)
    session = ADMIN_SESSIONS.get(token) if token else None
    return {
        "authenticated": bool(session),
        "configured": bool(ADMIN_PASSWORD),
        "user": {
            "user_id": session.get("user_id"),
            "username": session.get("username"),
            "full_name": session.get("full_name"),
            "role": session.get("role"),
        } if session else None,
        "permissions": web_permissions_for_role(session.get("role")) if session else [],
    }


@app.post("/api/admin/login")
async def admin_login(payload: AdminLoginPayload, request: Request):
    ensure_admin_request_origin(request)
    if not ADMIN_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="Вход отключен: задайте ADMIN_PASSWORD в переменных окружения.",
        )
    client_ip = extract_client_ip(request)
    ensure_login_not_rate_limited(client_ip)
    login_value = (payload.username or "").strip()
    if not login_value or not secrets.compare_digest(payload.password, ADMIN_PASSWORD):
        register_failed_login(client_ip)
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    session_user = resolve_web_login_user(login_value)
    if session_user is None:
        register_failed_login(client_ip)
        raise HTTPException(status_code=401, detail="Пользователь не найден или у него нет роли")

    response = JSONResponse({"authenticated": True})
    session_token = create_admin_session(session_user)
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
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        ADMIN_SESSIONS.pop(token, None)
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE_NAME, samesite="strict")
    return response


@app.get("/api/admin/dashboard")
async def admin_dashboard(request: Request):
    session = ensure_admin_session(request)
    return build_admin_dashboard_payload(session)


@app.post("/api/admin/links")
async def admin_create_payment_link(payload: CreatePaymentLinkPayload, request: Request):
    session = ensure_admin_permission(request, "create_link")
    amount = parse_payment_amount(str(payload.amount))
    if amount is None:
        raise HTTPException(status_code=400, detail="Укажите корректную сумму")
    link_record = create_payment_link_record(
        amount=amount,
        geo_code=payload.geo_code,
        creator_user_id=session.get("user_id"),
        creator_role=session.get("role"),
        currency_code=payload.currency_code,
        label=payload.label,
        landing_comment=payload.comment,
        payment_comment=payload.payment_comment,
        forced_language=payload.language_code,
        handler_user_id=payload.handler_user_id,
    )
    return {
        "ok": True,
        "link": build_payment_link_url(
            str(link_record.get("link_token") or ""),
            link_record.get("forced_language"),
        ),
        "payment_link": link_record,
    }


@app.post("/api/admin/geos/{geo_code}")
async def admin_update_geo(geo_code: str, payload: GeoConfigPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "manage_access")
    snapshot = save_geo_configuration(geo_code, payload)
    return {"ok": True, "geo": snapshot}


@app.post("/api/admin/requisites/{geo_code}")
async def admin_save_requisites(geo_code: str, payload: RequisitesPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "edit_requisites")
    target_geo = sanitize_geo_code(payload.geo_code, allow_unknown=True) if payload.geo_code else None
    active_requisites = update_geo_requisites(
        target_geo or geo_code,
        payload.bank_name,
        payload.card_number,
        payload.bic_swift,
        payload.receiver_name,
    )
    return {"ok": True, "active_requisites": active_requisites}


@app.post("/api/admin/managers")
async def admin_save_manager(payload: ManagerPayload, request: Request):
    raise HTTPException(status_code=410, detail="Модель managers удалена. Используйте роли Telegram-бота")


@app.post("/api/admin/bot-users")
async def admin_save_bot_user(payload: BotUserPayload, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "manage_access")
    if payload.user_id <= 0:
        raise HTTPException(status_code=400, detail="Нужен корректный Telegram ID")
    bot_user = save_bot_user_role(0, payload.user_id, payload.role, payload.channel_id)
    return {"ok": True, "bot_user": bot_user}


@app.delete("/api/admin/bot-users/{user_id}")
async def admin_delete_bot_user(user_id: int, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "manage_access")
    success, message = remove_bot_admin(user_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message}


@app.post("/api/admin/channels")
async def admin_save_channel(
    request: Request,
    channel_name: str = Form(...),
    channel_id: int | None = Form(default=None),
    remove_logo: bool = Form(default=False),
    logo: UploadFile | None = File(default=None),
):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "manage_access")
    channel = await save_channel(
        channel_name=channel_name,
        channel_id=channel_id,
        logo_upload=logo,
        remove_logo=remove_logo,
    )
    return {"ok": True, "channel": channel}


@app.delete("/api/admin/channels/{channel_id}")
async def admin_delete_channel(channel_id: int, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "manage_access")
    return {"ok": True, **delete_channel(channel_id)}


@app.post("/api/admin/requisites/{geo_code}/history/{history_id}/activate")
async def admin_activate_requisites_history(geo_code: str, history_id: int, request: Request):
    raise HTTPException(status_code=410, detail="Восстановление из истории отключено. Используется только очередь реквизитов")


@app.delete("/api/admin/requisites/{geo_code}/history/{history_id}")
async def admin_delete_requisites_history(geo_code: str, history_id: int, request: Request):
    ensure_admin_request_origin(request)
    ensure_admin_permission(request, "delete_requisites")
    result = delete_geo_requisites_history_item(geo_code, history_id)
    return {"ok": True, **result}


STATIC_DIR.mkdir(exist_ok=True)
CHANNEL_LOGO_DIR.mkdir(parents=True, exist_ok=True)
# Логотипы: если volume — монтируем отдельно; путь /static/uploads/channel-logos/ должен совпадать
app.mount("/static/uploads/channel-logos", StaticFiles(directory=str(CHANNEL_LOGO_DIR)), name="channel_logos")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    import uvicorn

    port = parse_optional_int(os.getenv("PORT", ""), 8000) or 8000
    logger.info("Starting uvicorn on 0.0.0.0:%s", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
