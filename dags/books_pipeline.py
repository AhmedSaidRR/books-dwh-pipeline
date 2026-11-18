"""Daily DAG: books.toscrape.com -> PostgreSQL star schema.

Four tasks mirror the four modules in ``src/``:

    extract (scraper.py) -> transform (transform.py)
        -> quality (quality.py) -> load (load.py)

Each task is retried independently. The quality task has ``retries=0`` on
purpose: retrying a deterministic data bug only wastes time, the task should
fail fast and block the load.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

LOGGER = logging.getLogger(__name__)

DAG_DOC = """
### Books price warehouse

Crawls every category of [books.toscrape.com](https://books.toscrape.com),
cleans the data with pandas, validates it and loads one row per book per day
into the PostgreSQL star schema (`dim_date`, `dim_book`, `dim_category`,
`fact_book_snapshot`).

**Failure semantics**

* `extract` retries 3x with exponential backoff (the network is flaky).
* `quality` never retries: a deterministic bad row must block the load.
* `load` is idempotent (`ON CONFLICT (date_key, book_sk) DO UPDATE`), so a
  cleared task re-run repairs the snapshot instead of duplicating it.

**Manual test of the quality gate**: set the `quality_min_rows` style checks
aside and inject a rating of `9` into the processed parquet file before the
quality task runs - `rating_between_1_and_5` will fail the task.
"""


def _record_failure(context: dict) -> None:
    """Write failed runs to `etl_run_log` (best effort, never masks errors)."""
    try:
        from src.config import Settings
        from src.load import record_failed_run

        record_failed_run(
            Settings.from_env(),
            run_id=context.get("run_id") or "unknown",
            snapshot_date=context.get("ds"),
            error=str(context.get("exception")),
        )
    except Exception:  # pragma: no cover - observability only
        LOGGER.exception("Could not persist the failed run")


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": _record_failure,
    "email_on_failure": False,
}


@dag(
    dag_id="books_dwh_pipeline",
    description="Scrape books.toscrape.com, validate it and load a daily star-schema snapshot",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args=DEFAULT_ARGS,
    tags=["books", "scraping", "postgres", "dwh", "data-quality"],
    doc_md=DAG_DOC,
)
def books_dwh_pipeline():
    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=25),
    )
    def extract() -> dict:
        """Scrape the catalogue and persist the raw payload as JSON."""
        from src.config import Settings
        from src.pipeline import save_raw
        from src.scraper import BooksScraper

        context = get_current_context()
        snapshot_date = context["ds"]
        settings = Settings.from_env()

        LOGGER.info("Scraping %s for snapshot %s", settings.scraper.base_url, snapshot_date)
        records = BooksScraper(settings.scraper).scrape()
        if not records:
            raise ValueError("scraper returned zero books")

        raw_path = save_raw(
            records,
            Path(settings.data_dir) / "raw" / f"books_{snapshot_date}.json",
        )
        LOGGER.info("Extracted %s books -> %s", len(records), raw_path)
        return {
            "raw_path": str(raw_path),
            "snapshot_date": snapshot_date,
            "rows_extracted": len(records),
        }

    @task(execution_timeout=timedelta(minutes=10))
    def transform(payload: dict) -> dict:
        """Clean, type and enrich the raw payload; write a parquet file."""

        from src.config import Settings
        from src.pipeline import load_raw
        from src.transform import transform_records

        snapshot_date = payload["snapshot_date"]
        df = transform_records(load_raw(Path(payload["raw_path"])), snapshot_date=snapshot_date)

        processed_path = (
            Path(Settings.from_env().data_dir)
            / "processed"
            / f"books_{snapshot_date}.parquet"
        )
        processed_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(processed_path, index=False)

        LOGGER.info("Transformed %s rows -> %s", len(df), processed_path)
        return {
            **payload,
            "processed_path": str(processed_path),
            "rows_transformed": int(len(df)),
            # Cast to plain Python types: XCom serialises with json, and
            # numpy.int64 keys/values are not JSON serialisable.
            "price_tier_counts": {
                str(tier): int(count) for tier, count in df["price_tier"].value_counts().items()
            },
        }

    @task(retries=0, execution_timeout=timedelta(minutes=5))
    def quality(payload: dict) -> dict:
        """Run the data contract; a critical failure fails the DAG run here."""
        import pandas as pd

        from src.quality import DataQualityValidator

        df = pd.read_parquet(payload["processed_path"])
        report = DataQualityValidator().validate(
            df, expected_snapshot_date=payload["snapshot_date"]
        )
        LOGGER.info("Quality gate passed for %s rows", report.row_count)
        return {**payload, "quality_report": report.to_dict()}

    @task(retries=1, execution_timeout=timedelta(minutes=10))
    def load(payload: dict) -> dict:
        """Idempotent upsert of the snapshot into the star schema."""
        import pandas as pd

        from src.config import Settings
        from src.load import apply_schema, connect, load_snapshot

        context = get_current_context()
        settings = Settings.from_env()
        df = pd.read_parquet(payload["processed_path"])

        with connect(settings) as conn:
            apply_schema(conn)
            summary = load_snapshot(
                conn,
                df,
                run_id=context["run_id"],
                rows_extracted=payload["rows_extracted"],
            )
            conn.commit()

        LOGGER.info("Warehouse load summary: %s", summary)
        return summary

    extracted = extract()
    transformed = transform(extracted)
    validated = quality(transformed)
    load(validated)


books_dwh_pipeline()
