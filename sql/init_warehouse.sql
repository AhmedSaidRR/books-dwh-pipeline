-- ---------------------------------------------------------------------------
-- Warehouse schema: a small star schema for daily book price snapshots.
--
--        dim_date          dim_category
--            |                  |
--            +--> fact_book_snapshot <--+
--                       ^               |
--                  dim_book ------------+
--
-- Run automatically by the `warehouse` container on first start, and also by
-- src/load.py (`apply_schema`) so the pipeline is self-healing.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key       INTEGER      PRIMARY KEY,              -- 20261118
    full_date      DATE         NOT NULL UNIQUE,
    day_of_month   SMALLINT     NOT NULL,
    day_name       TEXT         NOT NULL,
    month_number   SMALLINT     NOT NULL,
    month_name     TEXT         NOT NULL,
    quarter        SMALLINT     NOT NULL,
    year           SMALLINT     NOT NULL,
    is_weekend     BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key   SERIAL       PRIMARY KEY,
    category_name  TEXT         NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_book (
    book_sk        SERIAL       PRIMARY KEY,              -- surrogate key
    book_key       TEXT         NOT NULL UNIQUE,           -- natural key (URL slug)
    title          TEXT         NOT NULL,
    category_key   INTEGER      NOT NULL REFERENCES dim_category (category_key),
    book_url       TEXT,
    image_url      TEXT,
    first_seen     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_seen      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS fact_book_snapshot (
    fact_id        BIGSERIAL    PRIMARY KEY,
    date_key       INTEGER      NOT NULL REFERENCES dim_date (date_key),
    book_sk        INTEGER      NOT NULL REFERENCES dim_book (book_sk),
    price_gbp      NUMERIC(8, 2) NOT NULL CHECK (price_gbp > 0),
    price_tier     TEXT         NOT NULL,
    rating         SMALLINT     CHECK (rating BETWEEN 1 AND 5),
    in_stock       BOOLEAN      NOT NULL,
    run_id         TEXT,
    loaded_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- One snapshot per book per day: re-running a DAG run is idempotent.
    CONSTRAINT fact_book_snapshot_unique UNIQUE (date_key, book_sk)
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          TEXT        PRIMARY KEY,
    snapshot_date   DATE,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL,                 -- running | success | failed
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_book_snapshot_date
    ON fact_book_snapshot (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_book_snapshot_book
    ON fact_book_snapshot (book_sk);
CREATE INDEX IF NOT EXISTS idx_dim_book_category
    ON dim_book (category_key);
