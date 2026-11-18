"""Unit tests for the transform step."""

from __future__ import annotations

import pandas as pd
import pytest

from src.transform import COLUMNS, TransformError, transform_records


def test_transform_returns_typed_columns(raw_records) -> None:
    df = transform_records(raw_records, snapshot_date="2026-11-18")

    assert list(df.columns) == COLUMNS
    assert len(df) == 5
    assert str(df["price_gbp"].dtype) == "float64"
    assert str(df["rating"].dtype) == "Int64"
    assert df["snapshot_date"].nunique() == 1
    assert df["snapshot_date"].iloc[0].isoformat() == "2026-11-18"
    assert df["in_stock"].dtype == bool


def test_transform_derives_price_tiers(raw_records) -> None:
    df = transform_records(raw_records)
    tiers = dict(zip(df["book_key"], df["price_tier"], strict=True))
    assert tiers["book-001"] == "budget"      # 11.0
    assert tiers["book-002"] == "budget"      # 19.99 (boundary, exclusive)
    assert tiers["book-003"] == "standard"    # 35.50
    assert tiers["book-004"] == "premium"     # 40.00 (boundary, inclusive)
    assert tiers["book-005"] == "budget"      # 7.25


def test_transform_deduplicates_on_book_key(make_record) -> None:
    records = [
        make_record(1, title="Old title", price_gbp=10.0),
        make_record(1, title="New title", price_gbp=12.0),
    ]
    df = transform_records(records, snapshot_date="2026-11-18")

    assert len(df) == 1
    assert df["title"].iloc[0] == "New title"
    assert df["price_gbp"].iloc[0] == 12.0


def test_transform_cleans_titles_and_strings(make_record) -> None:
    df = transform_records(
        [make_record(1, title="  A   Light\n in the Attic  ")],
        snapshot_date="2026-11-18",
    )
    assert df["title"].iloc[0] == "A Light in the Attic"


def test_transform_falls_back_to_scraped_at(make_record) -> None:
    df = transform_records([make_record(1, scraped_at="2026-11-17T23:30:00+00:00")])
    assert df["snapshot_date"].iloc[0].isoformat() == "2026-11-17"


def test_transform_keeps_bad_values_for_the_quality_gate(make_record) -> None:
    df = transform_records(
        [make_record(1, price_gbp="not-a-price", rating=9)],
        snapshot_date="2026-11-18",
    )
    assert pd.isna(df["price_gbp"].iloc[0])
    assert df["rating"].iloc[0] == 9


def test_transform_rejects_empty_input() -> None:
    with pytest.raises(TransformError):
        transform_records([])


def test_transform_rejects_missing_columns(make_record) -> None:
    record = make_record(1)
    del record["category"]
    with pytest.raises(TransformError, match="missing required columns"):
        transform_records([record])
