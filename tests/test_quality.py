"""Unit tests for the quality gate.

The last test is the interview demo: a rating of 9 must make the pipeline
refuse to load.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.quality import DataQualityError, DataQualityValidator
from src.transform import transform_records


@pytest.fixture()
def df(raw_records) -> pd.DataFrame:
    return transform_records(raw_records, snapshot_date="2026-11-18")


@pytest.fixture()
def validator() -> DataQualityValidator:
    # Small minimums so the fixtures stay tiny.
    return DataQualityValidator(min_rows=1, min_rows_per_category=0)


def test_valid_data_passes(df, validator) -> None:
    report = validator.validate(df, expected_snapshot_date="2026-11-18")

    assert report.passed
    assert report.row_count == 5
    assert report.snapshot_date.isoformat() == "2026-11-18"
    assert report.failures() == []


def test_report_is_json_friendly(df, validator) -> None:
    report = validator.run(df, expected_snapshot_date="2026-11-18").to_dict()
    assert report["passed"] is True
    assert report["row_count"] == 5
    assert all({"name", "passed", "severity", "details"} <= check.keys() for check in report["checks"])


def test_rating_of_nine_fails_the_run(df, validator) -> None:
    df.loc[df.index[0], "rating"] = 9

    report = validator.run(df)
    failed = [result.name for result in report.failures()]
    assert "rating_between_1_and_5" in failed

    with pytest.raises(DataQualityError) as error:
        validator.validate(df)
    assert "rating_between_1_and_5" in str(error.value)
    assert error.value.report.row_count == 5


@pytest.mark.parametrize("bad_rating", [0, 6, 9, -1])
def test_out_of_range_ratings_are_all_rejected(df, validator, bad_rating: int) -> None:
    df.loc[df.index[0], "rating"] = bad_rating
    report = validator.run(df)
    assert "rating_between_1_and_5" in [result.name for result in report.failures()]


def test_duplicate_book_keys_fail(df, validator) -> None:
    df.loc[df.index[1], "book_key"] = df.loc[df.index[0], "book_key"]
    report = validator.run(df)
    failed = [result.name for result in report.failures()]
    assert "book_key_unique" in failed


def test_null_title_fails(df, validator) -> None:
    df.loc[df.index[0], "title"] = None
    report = validator.run(df)
    assert "no_nulls_in_key_columns" in [result.name for result in report.failures()]


@pytest.mark.parametrize("bad_price", [0.0, -5.0, 1500.0])
def test_impossible_prices_fail(df, validator, bad_price: float) -> None:
    df.loc[df.index[0], "price_gbp"] = bad_price
    report = validator.run(df)
    assert "price_within_bounds" in [result.name for result in report.failures()]


def test_wrong_snapshot_date_fails(df, validator) -> None:
    report = validator.run(df, expected_snapshot_date="2026-11-19")
    assert "snapshot_date_matches_run" in [result.name for result in report.failures()]


def test_missing_column_short_circuits(df, validator) -> None:
    report = validator.run(df.drop(columns=["price_gbp"]))
    assert [result.name for result in report.failures()] == ["required_columns"]
    assert report.row_count == 5


def test_too_few_rows_fail(df, raw_records) -> None:
    strict = DataQualityValidator(min_rows=100, min_rows_per_category=50)
    report = strict.run(df)
    assert "row_volume" in [result.name for result in report.failures()]


def test_placeholder_category_is_only_a_warning(make_record, validator) -> None:
    df = transform_records(
        [make_record(1), make_record(2, category="Default")],
        snapshot_date="2026-11-18",
    )
    report = validator.run(df)

    assert report.passed  # a warning must not block the load
    assert "category_not_placeholder" in [result.name for result in report.warnings]
    assert "category_not_placeholder" in report.to_dict()["warnings"]
