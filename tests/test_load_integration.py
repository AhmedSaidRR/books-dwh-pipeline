"""Integration test: real PostgreSQL, real star schema.

Skipped unless ``RUN_DB_TESTS=1`` (CI starts a postgres service container for
this job; locally you can point POSTGRES_* at the compose warehouse).
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime

import pytest

from src.config import Settings
from src.load import apply_schema, connect, date_key, load_snapshot
from src.quality import DataQualityValidator
from src.transform import transform_records

pytestmark = pytest.mark.integration

SNAPSHOT = date(2099, 12, 31)
KEY = date_key(SNAPSHOT)
RUN_ID = "itest__2099-12-31"
BOOKS = ["itest-book-1", "itest-book-2"]
CATEGORY = "IntegrationTest"


def _records(price_offset: float = 0.0) -> list[dict]:
    scraped_at = datetime(2099, 12, 31, 6, 0, tzinfo=UTC).isoformat()
    return [
        {
            "book_key": book,
            "title": book.replace("-", " ").title(),
            "category": CATEGORY,
            "category_slug": "integrationtest",
            "price_gbp": 20.0 + index + price_offset,
            "rating": 4,
            "in_stock": True,
            "book_url": f"https://books.toscrape.com/catalogue/{book}/index.html",
            "image_url": "https://books.toscrape.com/media/cache/x.jpg",
            "scraped_at": scraped_at,
        }
        for index, book in enumerate(BOOKS)
    ]


@pytest.fixture()
def conn():
    if os.getenv("RUN_DB_TESTS") != "1":
        pytest.skip("set RUN_DB_TESTS=1 to run integration tests")

    settings = Settings.from_env()
    try:
        connection = connect(settings)
    except Exception as error:  # noqa: BLE001 - environment dependent
        pytest.skip(f"warehouse is not reachable: {error}")

    apply_schema(connection)
    connection.commit()

    yield connection

    with connection.cursor() as cur:
        cur.execute("DELETE FROM fact_book_snapshot WHERE date_key = %s", (KEY,))
        cur.execute("DELETE FROM dim_book WHERE book_key = ANY(%s)", (BOOKS,))
        cur.execute("DELETE FROM dim_category WHERE category_name = %s", (CATEGORY,))
        cur.execute("DELETE FROM dim_date WHERE date_key = %s", (KEY,))
        cur.execute("DELETE FROM etl_run_log WHERE run_id = %s", (RUN_ID,))
    connection.commit()
    connection.close()


def test_load_is_idempotent_and_queryable(conn) -> None:
    df = transform_records(_records(), snapshot_date=SNAPSHOT)
    DataQualityValidator(min_rows=1, min_rows_per_category=0).validate(df, SNAPSHOT)

    first = load_snapshot(conn, df, run_id=RUN_ID, rows_extracted=len(df))
    conn.commit()
    assert first["rows_loaded"] == 2

    # Re-running the same logical date must update, not duplicate.
    updated = transform_records(_records(price_offset=5.0), snapshot_date=SNAPSHOT)
    load_snapshot(conn, updated, run_id=RUN_ID, rows_extracted=len(updated))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), MIN(price_gbp), MAX(price_gbp) FROM fact_book_snapshot WHERE date_key = %s",
            (KEY,),
        )
        count, min_price, max_price = cur.fetchone()
    assert count == 2
    assert float(min_price) == 25.0
    assert float(max_price) == 26.0

    query = """
        SELECT b.title, f.price_gbp, d.full_date, c.category_name
        FROM fact_book_snapshot f
        JOIN dim_book b ON b.book_sk = f.book_sk
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_category c ON c.category_key = b.category_key
        WHERE f.date_key = %s
        ORDER BY b.title
    """
    with conn.cursor() as cur:
        cur.execute(query, (KEY,))
        rows = cur.fetchall()

    assert [row[3] for row in rows] == [CATEGORY, CATEGORY]
    assert [str(row[2]) for row in rows] == [str(SNAPSHOT), str(SNAPSHOT)]
