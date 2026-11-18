"""Tests for the pure helpers of the load step plus the schema contract."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from src.load import SCHEMA_FILE, build_dim_date_rows, date_key


def test_date_key_format() -> None:
    assert date_key(date(2026, 11, 18)) == 20261118
    assert date_key(date(2025, 1, 2)) == 20250102


def test_build_dim_date_rows() -> None:
    rows = build_dim_date_rows([date(2026, 11, 18), date(2026, 11, 18), date(2026, 11, 21)])

    assert [row[0] for row in rows] == [20261118, 20261121]
    wednesday = rows[0]
    assert wednesday[1] == date(2026, 11, 18)
    assert wednesday[3] == "Wednesday"
    assert wednesday[6] == 4  # quarter
    assert wednesday[7] == 2026
    assert wednesday[8] is False  # not a weekend

    saturday = rows[1]
    assert saturday[8] is True


def test_schema_file_contains_the_star_schema() -> None:
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    for table in (
        "dim_date",
        "dim_category",
        "dim_book",
        "fact_book_snapshot",
        "etl_run_log",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql

    # The idempotency guarantee relies on this constraint.
    assert "CONSTRAINT fact_book_snapshot_unique UNIQUE (date_key, book_sk)" in sql
    assert "CHECK (rating BETWEEN 1 AND 5)" in sql


def test_schema_file_exists_next_to_the_project_root() -> None:
    assert Path(SCHEMA_FILE).is_file()
