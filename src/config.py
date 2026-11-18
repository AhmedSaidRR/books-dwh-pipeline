"""Central configuration for the books data warehouse pipeline.

Everything is driven by environment variables (see ``.env.example``) so the
exact same code runs locally, inside Airflow and inside CI.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:  # python-dotenv is optional at runtime (Docker injects real env vars)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - defensive only
    pass

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_BASE_URL = "https://books.toscrape.com/"
DEFAULT_USER_AGENT = (
    "books-dwh-pipeline/1.0 (educational data engineering project; "
    "contact: your-email@example.com)"
)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


@dataclass(frozen=True)
class DatabaseSettings:
    """Connection settings for the PostgreSQL warehouse."""

    host: str = "localhost"
    port: int = 5433
    dbname: str = "warehouse"
    user: str = "warehouse"
    password: str = "warehouse"

    @classmethod
    def from_env(cls) -> DatabaseSettings:
        return cls(
            host=_env("POSTGRES_HOST", "localhost") or "localhost",
            port=int(_env("POSTGRES_PORT", "5433") or 5433),
            dbname=_env("POSTGRES_DB", "warehouse") or "warehouse",
            user=_env("POSTGRES_USER", "warehouse") or "warehouse",
            password=_env("POSTGRES_PASSWORD", "warehouse") or "warehouse",
        )

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class ScraperSettings:
    """HTTP / crawling settings for books.toscrape.com."""

    base_url: str = DEFAULT_BASE_URL
    request_delay: float = 0.1
    timeout: int = 20
    max_retries: int = 3
    user_agent: str = DEFAULT_USER_AGENT
    max_categories: int | None = None

    @classmethod
    def from_env(cls) -> ScraperSettings:
        raw_categories = _env("SCRAPER_MAX_CATEGORIES")
        return cls(
            base_url=_env("SCRAPER_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL,
            request_delay=float(_env("SCRAPER_REQUEST_DELAY", "0.1") or 0.1),
            timeout=int(_env("SCRAPER_TIMEOUT", "20") or 20),
            max_retries=int(_env("SCRAPER_MAX_RETRIES", "3") or 3),
            user_agent=_env("SCRAPER_USER_AGENT", DEFAULT_USER_AGENT) or DEFAULT_USER_AGENT,
            max_categories=int(raw_categories) if raw_categories else None,
        )


@dataclass(frozen=True)
class Settings:
    """Top level settings container."""

    scraper: ScraperSettings
    database: DatabaseSettings
    data_dir: Path = DEFAULT_DATA_DIR

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            scraper=ScraperSettings.from_env(),
            database=DatabaseSettings.from_env(),
            data_dir=Path(_env("DATA_DIR", str(DEFAULT_DATA_DIR)) or DEFAULT_DATA_DIR),
        )
