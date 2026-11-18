"""Load step: upsert a daily snapshot into the PostgreSQL star schema.

The load is idempotent: ``fact_book_snapshot`` has a unique constraint on
``(date_key, book_sk)`` and every insert uses ``ON CONFLICT``. Re-running a
failed DAG run therefore repairs the data instead of duplicating it.
"""

from __future__ import annotations

import calendar
import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg import Connection

from .config import DatabaseSettings, Settings

LOGGER = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "sql" / "init_warehouse.sql"


class LoadError(RuntimeError):
    """Raised when the snapshot cannot be written to the warehouse."""


# --------------------------------------------------------------------------- SQL
INSERT_DATE = """
INSERT INTO dim_date (
    date_key, full_date, day_of_month, day_name, month_number,
    month_name, quarter, year, is_weekend
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (date_key) DO NOTHING
"""

INSERT_CATEGORY = """
INSERT INTO dim_category (category_name)
VALUES (%s)
ON CONFLICT (category_name) DO NOTHING
"""

UPSERT_BOOK = """
INSERT INTO dim_book (book_key, title, category_key, book_url, image_url, last_seen)
SELECT %s, %s, c.category_key, %s, %s, NOW()
FROM dim_category c
WHERE c.category_name = %s
ON CONFLICT (book_key) DO UPDATE SET
    title        = EXCLUDED.title,
    category_key = EXCLUDED.category_key,
    book_url     = EXCLUDED.book_url,
    image_url    = EXCLUDED.image_url,
    last_seen    = NOW(),
    is_active    = TRUE
"""

UPSERT_FACT = """
INSERT INTO fact_book_snapshot (
    date_key, book_sk, price_gbp, price_tier, rating, in_stock, run_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT fact_book_snapshot_unique DO UPDATE SET
    price_gbp  = EXCLUDED.price_gbp,
    price_tier = EXCLUDED.price_tier,
    rating     = EXCLUDED.rating,
    in_stock   = EXCLUDED.in_stock,
    run_id     = EXCLUDED.run_id,
    loaded_at  = NOW()
"""

UPSERT_RUN = """
INSERT INTO etl_run_log (
    run_id, snapshot_date, status, rows_extracted, rows_loaded, finished_at
)
VALUES (%s, %s, %s, %s, %s, NOW())
ON CONFLICT (run_id) DO UPDATE SET
    snapshot_date  = EXCLUDED.snapshot_date,
    status         = EXCLUDED.status,
    rows_extracted = EXCLUDED.rows_extracted,
    rows_loaded    = EXCLUDED.rows_loaded,
    finished_at    = NOW(),
    error_message  = NULL
"""


# ---------------------------------------------------------------------- helpers
def connect(settings: Settings | DatabaseSettings | None = None) -> Connection:
    """Open a connection to the warehouse (not to Airflow's metadata DB)."""
    if settings is None:
        db = Settings.from_env().database
    elif isinstance(settings, Settings):
        db = settings.database
    else:
        db = settings
    return psycopg.connect(db.dsn())


def apply_schema(conn: Connection) -> None:
    """Create the star schema if it does not exist (idempotent)."""
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    with conn.transaction():
        conn.execute(sql)


def date_key(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def build_dim_date_rows(dates: Iterable[date]) -> list[tuple]:
    rows: list[tuple] = []
    for value in sorted(set(dates)):
        rows.append(
            (
                date_key(value),
                value,
                value.day,
                calendar.day_name[value.weekday()],
                value.month,
                calendar.month_name[value.month],
                (value.month - 1) // 3 + 1,
                value.year,
                value.weekday() >= 5,
            )
        )
    return rows


def _to_date(value: Any) -> date:
    return pd.Timestamp(value).date()


# ------------------------------------------------------------------------- load
def load_snapshot(
    conn: Connection,
    df: pd.DataFrame,
    run_id: str,
    rows_extracted: int | None = None,
) -> dict[str, Any]:
    """Upsert one daily snapshot and return a small summary."""
    if df.empty:
        raise LoadError("refusing to load an empty snapshot")

    snapshot_dates = {
        _to_date(value) for value in pd.Series(df["snapshot_date"]).dropna().unique()
    }
    if len(snapshot_dates) != 1:
        raise LoadError(f"expected exactly one snapshot_date, got {sorted(snapshot_dates)}")
    snapshot_date = snapshot_dates.pop()

    required = {"book_key", "title", "category", "price_gbp", "price_tier", "rating", "in_stock"}
    missing = required.difference(df.columns)
    if missing:
        raise LoadError(f"cannot load, missing columns: {sorted(missing)}")

    categories = sorted({str(value) for value in df["category"].dropna().unique()})
    book_rows = [
        (
            str(row.book_key),
            str(row.title),
            str(row.book_url) if isinstance(row.book_url, str) else None,
            str(row.image_url) if isinstance(row.image_url, str) else None,
            str(row.category),
        )
        for row in df.itertuples(index=False)
    ]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(INSERT_DATE, build_dim_date_rows([snapshot_date]))
            cur.executemany(INSERT_CATEGORY, [(name,) for name in categories])
            cur.executemany(UPSERT_BOOK, book_rows)

            cur.execute(
                "SELECT book_key, book_sk FROM dim_book WHERE book_key = ANY(%s)",
                ([row[0] for row in book_rows],),
            )
            book_keys = dict(cur.fetchall())
            missing_keys = [row[0] for row in book_rows if row[0] not in book_keys]
            if missing_keys:
                raise LoadError(f"dim_book lookup failed for {missing_keys[:5]}")

            fact_rows = [
                (
                    date_key(snapshot_date),
                    book_keys[str(row.book_key)],
                    float(row.price_gbp),
                    str(row.price_tier),
                    int(row.rating) if not pd.isna(row.rating) else None,
                    bool(row.in_stock),
                    run_id,
                )
                for row in df.itertuples(index=False)
            ]
            cur.executemany(UPSERT_FACT, fact_rows)

            cur.execute(
                UPSERT_RUN,
                (
                    run_id,
                    snapshot_date,
                    "success",
                    rows_extracted if rows_extracted is not None else len(df),
                    len(fact_rows),
                ),
            )

    summary = {
        "run_id": run_id,
        "snapshot_date": snapshot_date.isoformat(),
        "rows_loaded": len(fact_rows),
        "books": len(book_rows),
        "categories": len(categories),
    }
    LOGGER.info(
        "Loaded %s rows for %s (%s books, %s categories)",
        summary["rows_loaded"],
        summary["snapshot_date"],
        summary["books"],
        summary["categories"],
    )
    return summary


def record_failed_run(
    settings: Settings | DatabaseSettings | None,
    run_id: str,
    snapshot_date: date | str | None,
    error: str,
) -> None:
    """Best-effort write of a failed run to ``etl_run_log``."""
    try:
        with connect(settings) as conn:
            apply_schema(conn)
            conn.execute(
                """
                INSERT INTO etl_run_log (run_id, snapshot_date, status, error_message, finished_at)
                VALUES (%s, %s, 'failed', %s, NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'failed',
                    error_message = EXCLUDED.error_message,
                    finished_at = NOW()
                """,
                (run_id, snapshot_date, error[:2000]),
            )
            conn.commit()
    except Exception:  # pragma: no cover - logging must never mask the real error
        LOGGER.exception("Could not record the failed run %s", run_id)
