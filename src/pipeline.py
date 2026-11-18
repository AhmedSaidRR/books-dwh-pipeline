"""Local entrypoint: extract -> transform -> quality -> load.

Airflow calls the same functions one task at a time; this module is what you
run while developing (``python -m src.pipeline --max-categories 2``) and what
you can use to replay an existing raw file without hitting the network.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .config import Settings
from .load import apply_schema, connect, load_snapshot, record_failed_run
from .quality import DataQualityValidator, QualityReport
from .scraper import BooksScraper
from .transform import transform_records

LOGGER = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    run_id: str
    snapshot_date: str
    rows_extracted: int
    rows_transformed: int
    rows_loaded: int
    raw_path: str | None
    processed_path: str | None
    duration_seconds: float
    quality: dict[str, Any] | None = None


def save_raw(records: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_raw(path: Path) -> list[dict[str, Any]]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def run_pipeline(
    settings: Settings | None = None,
    snapshot_date: str | date | None = None,
    max_categories: int | None = None,
    skip_load: bool = False,
    raw_file: str | Path | None = None,
    run_id: str | None = None,
) -> PipelineResult:
    settings = settings or Settings.from_env()
    started = time.monotonic()

    # 1. EXTRACT -----------------------------------------------------------
    if raw_file is not None:
        records = load_raw(Path(raw_file))
        raw_path: Path | None = Path(raw_file)
        LOGGER.info("Loaded %s raw records from %s", len(records), raw_path)
    else:
        records = BooksScraper(settings.scraper).scrape(max_categories=max_categories)
        raw_path = None
    if not records:
        raise RuntimeError("extract returned zero records - aborting")

    # 2. TRANSFORM ---------------------------------------------------------
    df = transform_records(records, snapshot_date=snapshot_date)
    run_date: date = df["snapshot_date"].iloc[0]
    run_id = run_id or f"manual__{run_date.isoformat()}"

    raw_path = raw_path or save_raw(records, settings.data_dir / "raw" / f"books_{run_date}.json")
    processed_path = settings.data_dir / "processed" / f"books_{run_date}.parquet"
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(processed_path, index=False)
    LOGGER.info("Transformed %s rows -> %s", len(df), processed_path)

    # 3. QUALITY -----------------------------------------------------------
    validator = DataQualityValidator()
    report: QualityReport | None = None
    try:
        report = validator.validate(df, expected_snapshot_date=run_date)
        report_dict = report.to_dict()
    except Exception as error:
        if not skip_load:
            record_failed_run(settings, run_id, run_date, str(error))
        raise

    # 4. LOAD --------------------------------------------------------------
    rows_loaded = 0
    if skip_load:
        LOGGER.info("--skip-load set: skipping the warehouse write")
    else:
        with connect(settings) as conn:
            apply_schema(conn)
            summary = load_snapshot(
                conn,
                df,
                run_id=run_id,
                rows_extracted=len(records),
            )
            conn.commit()
            rows_loaded = int(summary["rows_loaded"])

    return PipelineResult(
        run_id=run_id,
        snapshot_date=run_date.isoformat(),
        rows_extracted=len(records),
        rows_transformed=len(df),
        rows_loaded=rows_loaded,
        raw_path=str(raw_path) if raw_path else None,
        processed_path=str(processed_path),
        duration_seconds=round(time.monotonic() - started, 2),
        quality=report_dict if report else None,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the books warehouse pipeline locally.")
    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Logical date of the snapshot (YYYY-MM-DD). Defaults to today (UTC).",
    )
    parser.add_argument(
        "--max-categories",
        type=int,
        default=None,
        help="Scrape only the first N categories (useful for a quick local run).",
    )
    parser.add_argument(
        "--raw-file",
        default=None,
        help="Replay an existing raw JSON file instead of scraping.",
    )
    parser.add_argument("--skip-load", action="store_true", help="Do everything except the DB load.")
    parser.add_argument("--run-id", default=None, help="Override the run id stored in the warehouse.")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = build_parser().parse_args(argv)
    try:
        result = run_pipeline(
            snapshot_date=args.snapshot_date,
            max_categories=args.max_categories,
            skip_load=args.skip_load,
            raw_file=args.raw_file,
            run_id=args.run_id,
        )
    except Exception as error:  # noqa: BLE001 - CLI boundary
        LOGGER.error("Pipeline failed: %s", error)
        return 1

    LOGGER.info("Pipeline finished in %ss", result.duration_seconds)
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
