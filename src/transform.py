"""Transform step: raw scraper records -> a clean, typed DataFrame."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

COLUMNS: list[str] = [
    "book_key",
    "title",
    "category",
    "price_gbp",
    "price_tier",
    "rating",
    "in_stock",
    "book_url",
    "image_url",
    "snapshot_date",
    "scraped_at",
]

PRICE_COLUMNS = ["price_gbp"]
INT_COLUMNS = ["rating"]
BOOL_COLUMNS = ["in_stock"]
STRING_COLUMNS = ["book_key", "title", "category", "book_url", "image_url", "scraped_at"]

# Business rules for the price tier (GBP).
PRICE_BINS = [0.0, 20.0, 40.0, float("inf")]
PRICE_LABELS = ["budget", "standard", "premium"]


class TransformError(ValueError):
    """Raised when the raw payload cannot be turned into a valid dataset."""


def _clean_text(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def transform_records(
    records: Iterable[dict[str, Any]],
    snapshot_date: str | date | None = None,
) -> pd.DataFrame:
    """Validate, clean and enrich the scraped records.

    Steps:
    1. Build a DataFrame and keep only the expected columns.
    2. Coerce every column to its warehouse type (a bad price becomes NaN so
       the quality gate can catch it instead of crashing the transform).
    3. Clean text, derive ``price_tier``, normalise booleans.
    4. De-duplicate on the natural key and sort for determinism.
    """
    rows = list(records)
    if not rows:
        raise TransformError("no records to transform (the extract step returned nothing)")

    df = pd.DataFrame.from_records(rows)
    derived = {"price_tier", "snapshot_date"}
    missing = [column for column in COLUMNS if column not in df.columns and column not in derived]
    if missing:
        raise TransformError(f"raw data is missing required columns: {missing}")

    df = df.copy()

    # ---- typing -----------------------------------------------------------
    for column in PRICE_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").round(2)
    for column in INT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    if "in_stock" in df.columns:
        # Accept True/False, "True"/"False", "In stock" and "1"/"0".
        df["in_stock"] = df["in_stock"].map(
            lambda value: str(value).strip().lower() in {"true", "1", "in stock", "yes"}
            if not isinstance(value, bool)
            else value
        )

    # ---- text cleaning ----------------------------------------------------
    for column in STRING_COLUMNS:
        if column in df.columns:
            df[column] = _clean_text(df[column])
    df["title"] = df["title"].astype("string")

    # ---- business enrichment ---------------------------------------------
    df["price_tier"] = pd.cut(
        df["price_gbp"],
        bins=PRICE_BINS,
        labels=PRICE_LABELS,
        right=False,
    ).astype("string")

    if snapshot_date is None:
        # Fall back to the date the scrape happened (UTC), not the wall clock,
        # so re-processing a raw file always produces the same snapshot.
        snapshot_date = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True).dt.date.max()
    if snapshot_date is None or pd.isna(snapshot_date):
        raise TransformError("cannot determine the snapshot date")
    df["snapshot_date"] = pd.Timestamp(snapshot_date).date()

    # ---- quality-oriented cleanup ----------------------------------------
    before = len(df)
    df = df.drop_duplicates(subset=["book_key"], keep="last")
    if len(df) != before:
        LOGGER.warning("Dropped %s duplicate book_key rows", before - len(df))

    df = df.sort_values("book_key", kind="stable").reset_index(drop=True)

    for column in ("book_key", "title", "category", "price_gbp", "rating", "in_stock"):
        if df[column].isna().any():
            LOGGER.warning(
                "Column %s contains %s nulls - the quality gate will decide",
                column,
                int(df[column].isna().sum()),
            )

    return df[COLUMNS]
