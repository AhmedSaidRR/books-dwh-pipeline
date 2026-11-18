"""Data quality gates.

The pipeline refuses to load data that breaks the contract of the warehouse.
Every rule returns a :class:`CheckResult`; the run fails only when a
*critical* rule fails, which makes the Airflow task fail loudly (and keeps the
warehouse free of bad data) while warnings are only logged.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "book_key",
    "title",
    "category",
    "price_gbp",
    "price_tier",
    "rating",
    "in_stock",
    "snapshot_date",
]

VALID_PRICE_TIERS = {"budget", "standard", "premium"}

# books.toscrape.com is a dump of a real store, so the source contains two
# placeholder categories ("Default" and "Add a comment"). They are real rows,
# not a scraping bug: flag them as a warning so the data team can decide,
# instead of failing a perfectly good daily load.
PLACEHOLDER_CATEGORIES = {"default", "add a comment"}


class DataQualityError(RuntimeError):
    """Raised when one or more critical checks fail."""

    def __init__(self, message: str, report: QualityReport) -> None:
        super().__init__(message)
        self.report = report


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    severity: str = "critical"  # critical | warning
    details: str = ""


@dataclass
class QualityReport:
    results: list[CheckResult] = field(default_factory=list)
    row_count: int = 0
    snapshot_date: date | None = None

    @property
    def passed(self) -> bool:
        return not self.failures()

    def failures(self, severity: str = "critical") -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == severity]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "warning"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "row_count": self.row_count,
            "snapshot_date": self.snapshot_date.isoformat() if self.snapshot_date else None,
            "checks": [asdict(result) for result in self.results],
            "failed_checks": [result.name for result in self.failures()],
            "warnings": [result.name for result in self.warnings],
        }

    def summary(self) -> str:
        lines = [
            f"Quality report: {self.row_count} rows, snapshot={self.snapshot_date}, "
            f"{'PASSED' if self.passed else 'FAILED'}"
        ]
        for result in self.results:
            status = "PASS" if result.passed else ("FAIL" if result.severity == "critical" else "WARN")
            lines.append(f"  [{status}] {result.name}: {result.details}")
        return "\n".join(lines)


class DataQualityValidator:
    """Runs the data contract rules against the transformed DataFrame."""

    def __init__(
        self,
        min_rows: int = 10,
        min_rows_per_category: int = 8,
        max_price: float = 1000.0,
    ) -> None:
        self.min_rows = min_rows
        self.min_rows_per_category = min_rows_per_category
        self.max_price = max_price

    # ------------------------------------------------------------------ API
    def run(
        self,
        df: pd.DataFrame,
        expected_snapshot_date: str | date | None = None,
    ) -> QualityReport:
        report = QualityReport(
            row_count=len(df),
            snapshot_date=self._snapshot_date(df),
        )
        report.results.extend(self._check_schema(df))
        if any(not r.passed for r in report.results if r.name == "required_columns"):
            # Row-level rules would raise confusing KeyErrors without columns.
            return report

        report.results.extend(
            [
                self._check_row_volume(df),
                self._check_primary_key(df),
                self._check_nulls(df),
                self._check_rating_range(df),
                self._check_price_range(df),
                self._check_price_tier(df),
                self._check_stock_flag(df),
                self._check_categories(df),
                self._check_placeholder_categories(df),
                self._check_snapshot_date(df, expected_snapshot_date),
            ]
        )
        return report

    def validate(
        self,
        df: pd.DataFrame,
        expected_snapshot_date: str | date | None = None,
    ) -> QualityReport:
        report = self.run(df, expected_snapshot_date)
        LOGGER.info("\n%s", report.summary())
        if not report.passed:
            failed = ", ".join(result.name for result in report.failures())
            raise DataQualityError(f"data quality checks failed: {failed}", report)
        return report

    # --------------------------------------------------------------- checks
    @staticmethod
    def _snapshot_date(df: pd.DataFrame) -> date | None:
        if "snapshot_date" not in df.columns or df["snapshot_date"].dropna().empty:
            return None
        values = pd.to_datetime(df["snapshot_date"].dropna(), errors="coerce").dt.date.unique()
        return values[0] if len(values) == 1 else None

    @staticmethod
    def _check_schema(df: pd.DataFrame) -> list[CheckResult]:
        missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
        return [
            CheckResult(
                name="required_columns",
                passed=not missing,
                details="all required columns present" if not missing else f"missing: {missing}",
            )
        ]

    def _check_row_volume(self, df: pd.DataFrame) -> CheckResult:
        rows = len(df)
        categories = int(df["category"].nunique(dropna=True))
        expected = max(self.min_rows, self.min_rows_per_category * max(categories, 1))
        return CheckResult(
            name="row_volume",
            passed=rows >= expected,
            details=f"{rows} rows from {categories} categories (minimum expected {expected})",
        )

    @staticmethod
    def _check_primary_key(df: pd.DataFrame) -> CheckResult:
        duplicated = df["book_key"].duplicated(keep=False)
        duplicates = sorted(df.loc[duplicated, "book_key"].dropna().unique().tolist())
        return CheckResult(
            name="book_key_unique",
            passed=not duplicates,
            details=(
                "book_key is unique"
                if not duplicates
                else f"{len(duplicates)} duplicated keys, e.g. {duplicates[:5]}"
            ),
        )

    @staticmethod
    def _check_nulls(df: pd.DataFrame) -> CheckResult:
        key_columns = ["book_key", "title", "category", "price_gbp", "rating", "in_stock", "snapshot_date"]
        null_counts = {column: int(df[column].isna().sum()) for column in key_columns}
        offenders = {column: count for column, count in null_counts.items() if count}
        return CheckResult(
            name="no_nulls_in_key_columns",
            passed=not offenders,
            details="no nulls in key columns" if not offenders else f"nulls found: {offenders}",
        )

    @staticmethod
    def _check_rating_range(df: pd.DataFrame) -> CheckResult:
        ratings = pd.to_numeric(df["rating"], errors="coerce")
        invalid = df.loc[ratings.notna() & ~ratings.between(1, 5), "book_key"]
        return CheckResult(
            name="rating_between_1_and_5",
            passed=invalid.empty,
            details=(
                f"{len(invalid)} out-of-range ratings, e.g. "
                f"{invalid.head(5).tolist()} = {ratings[invalid.index].head(5).tolist()}"
                if not invalid.empty
                else "all ratings are within 1..5"
            ),
        )

    def _check_price_range(self, df: pd.DataFrame) -> CheckResult:
        prices = pd.to_numeric(df["price_gbp"], errors="coerce")
        invalid = df.loc[prices.notna() & ((prices <= 0) | (prices >= self.max_price)), "book_key"]
        return CheckResult(
            name="price_within_bounds",
            passed=invalid.empty,
            details=(
                f"{len(invalid)} prices outside (0, {self.max_price}), e.g. {invalid.head(5).tolist()}"
                if not invalid.empty
                else f"all prices are inside (0, {self.max_price})"
            ),
        )

    @staticmethod
    def _check_price_tier(df: pd.DataFrame) -> CheckResult:
        invalid = (~df["price_tier"].isin(VALID_PRICE_TIERS)) | df["price_tier"].isna()
        return CheckResult(
            name="price_tier_valid",
            passed=not invalid.any(),
            details=(
                f"{int(invalid.sum())} rows have an unknown price tier"
                if invalid.any()
                else f"all tiers are in {sorted(VALID_PRICE_TIERS)}"
            ),
        )

    @staticmethod
    def _check_stock_flag(df: pd.DataFrame) -> CheckResult:
        invalid = ~df["in_stock"].isin([True, False]) | df["in_stock"].isna()
        return CheckResult(
            name="in_stock_is_boolean",
            passed=not invalid.any(),
            details=(
                f"{int(invalid.sum())} rows are not boolean"
                if invalid.any()
                else "in_stock is boolean for every row"
            ),
        )

    @staticmethod
    def _check_categories(df: pd.DataFrame) -> CheckResult:
        empty = df["category"].isna() | (df["category"].astype("string").str.len() == 0)
        categories = int(df["category"].nunique(dropna=True))
        return CheckResult(
            name="categories_present",
            passed=not empty.any() and categories >= 1,
            details=(
                f"{int(empty.sum())} rows without a category"
                if empty.any()
                else f"{categories} distinct categories"
            ),
        )

    @staticmethod
    def _check_placeholder_categories(df: pd.DataFrame) -> CheckResult:
        categories = df["category"].astype("string").str.lower().dropna().unique()
        found = sorted(category for category in categories if category in PLACEHOLDER_CATEGORIES)
        return CheckResult(
            name="category_not_placeholder",
            passed=not found,
            severity="warning",
            details=(
                f"placeholder categories in the source data: {found}"
                if found
                else "no placeholder categories"
            ),
        )

    @staticmethod
    def _check_snapshot_date(df: pd.DataFrame, expected: str | date | None) -> CheckResult:
        values = pd.to_datetime(df["snapshot_date"], errors="coerce").dt.date.dropna().unique()
        if expected is None:
            passed = len(values) == 1
            details = f"single snapshot date: {values.tolist()}" if passed else (
                f"expected exactly one snapshot date, found {values.tolist()}"
            )
            return CheckResult(name="snapshot_date_consistent", passed=passed, details=details)

        expected_date = pd.Timestamp(expected).date()
        passed = len(values) == 1 and values[0] == expected_date
        return CheckResult(
            name="snapshot_date_matches_run",
            passed=passed,
            details=f"expected {expected_date}, found {values.tolist()}",
        )
