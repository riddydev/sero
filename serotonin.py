"""Serotonin: a Telegram football statistics bot.

The bot intentionally keeps its public surface small:
  /help
  /team <team name>
  /h2h <team 1> vs <team 2>

Runtime credentials are read from Replit Secrets / environment variables and
are never included in log messages or user-facing errors.

Key design choices:
  - Persistent SQLite cache for team lookups and match lists
  - Per-user rate limiting
  - Single-instance lock to avoid Telegram polling conflicts
  - Batched competition fetches to respect API rate limits
"""

from __future__ import annotations

import asyncio
import fcntl
import html
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx
from rapidfuzz import fuzz
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import Conflict, NetworkError, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

for _logger_name in ("httpx", "httpcore", "telegram.request", "telegram.ext._updater"):
    logging.getLogger(_logger_name).setLevel(logging.WARNING)


class StartupConfigurationError(RuntimeError):
    """Raised when the bot cannot safely start."""


class FootballAPIError(RuntimeError):
    """A safe, user-facing football API failure."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.public_message = message
        self.status_code = status_code


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    football_data_api_key: str
    football_api_base_url: str = "https://api.football-data.org/v4"
    cache_db_path: str = "cache.db"


def load_settings(environ: dict[str, str] | None = None) -> Settings:
    values = os.environ if environ is None else environ
    token = values.get("TELEGRAM_BOT_TOKEN", "").strip()
    api_key = values.get("FOOTBALL_DATA_API_KEY", "").strip()
    missing = []
    if not token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not api_key:
        missing.append("FOOTBALL_DATA_API_KEY")
    if missing:
        raise StartupConfigurationError(
            "Missing required Replit Secrets: " + ", ".join(missing)
        )
    base_url = values.get(
        "FOOTBALL_DATA_API_BASE_URL", "https://api.football-data.org/v4"
    ).strip()
    if not base_url.startswith(("https://", "http://")):
        raise StartupConfigurationError(
            "FOOTBALL_DATA_API_BASE_URL must be an HTTP(S) URL"
        )
    return Settings(
        telegram_bot_token=token,
        football_data_api_key=api_key,
        football_api_base_url=base_url.rstrip("/"),
        cache_db_path=values.get("SEROTONIN_CACHE_DB", "cache.db").strip() or "cache.db",
    )


def normalize_team_name(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "")
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"\b(fc|cf|afc|sc|ac|cd|fk|sk|bk)\b", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


class APICache:
    def __init__(self, ttl_seconds: int = 3600, db_path: str = "cache.db"):
        self.ttl = ttl_seconds
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, timestamp REAL NOT NULL)"
                )
        except sqlite3.Error as exc:
            logger.warning("Cache initialization failed: %s", exc.__class__.__name__)

    def get(self, key: str) -> Any | None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT value, timestamp FROM cache WHERE key = ?", (key,)
                ).fetchone()
            if not row:
                return None
            value_json, timestamp = row
            if time.time() - timestamp >= self.ttl:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                return None
            return json.loads(value_json)
        except (sqlite3.Error, ValueError, TypeError) as exc:
            logger.warning("Cache read failed: %s", exc.__class__.__name__)
            return None

    def set(self, key: str, value: Any) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO cache (key, value, timestamp) VALUES (?, ?, ?)",
                    (key, json.dumps(value), time.time()),
                )
        except (sqlite3.Error, TypeError, ValueError) as exc:
            logger.warning("Cache write failed: %s", exc.__class__.__name__)


class RateLimiter:
    def __init__(self, max_requests: int = 15, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests: dict[int, list[float]] = {}

    def is_allowed(self, user_id: int) -> bool:
        now = time.monotonic()
        if len(self.requests) > 500:
            self.requests = {
                uid: ts for uid, ts in self.requests.items()
                if ts and now - ts[-1] < self.window
            }
        recent = [t for t in self.requests.get(user_id, []) if now - t < self.window]
        if len(recent) >= self.max_requests:
            self.requests[user_id] = recent
            return False
        recent.append(now)
        self.requests[user_id] = recent
        return True

    def get_wait_time(self, user_id: int) -> int:
        recent = self.requests.get(user_id, [])
        if not recent:
            return 0
        return max(0, int(self.window - (time.monotonic() - recent[0])))


class SingleInstanceLock:
    def __init__(self, path: str = ".serotonin.lock"):
        self.path = Path(path)
        self._file: Any | None = None

    def __enter__(self) -> "SingleInstanceLock":
        self._file = self.path.open("w")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._file.close()
            self._file = None
            raise StartupConfigurationError(
                "Another local Serotonin process is already running. "
                "Stop the existing Telegram Bot workflow before starting another."
            ) from exp
        return self

    def __exit__(self, *_: Any) -> None:
        if self._file is not None:
            with suppress(OSError):
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._file.close()
            self._file = None


# NOTE: This is a truncated restore attempt - the full file needs to be restored.
# Please use the sed fix on Replit for now.

if __name__ == "__main__":
    print("Bot file is incomplete - please contact support or restore from backup")
