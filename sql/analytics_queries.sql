-- ---------------------------------------------------------------------------
-- Analytics queries for the books star schema.
--
-- Run them with:
--   docker compose exec warehouse psql -U warehouse -d warehouse
-- ---------------------------------------------------------------------------

-- 1. Latest snapshot at a glance -------------------------------------------
SELECT
    d.full_date,
    COUNT(*)                              AS books,
    COUNT(*) FILTER (WHERE f.in_stock)    AS in_stock,
    ROUND(AVG(f.price_gbp), 2)            AS avg_price_gbp,
    ROUND(MIN(f.price_gbp), 2)            AS min_price_gbp,
    ROUND(MAX(f.price_gbp), 2)            AS max_price_gbp
FROM fact_book_snapshot f
JOIN dim_date d ON d.date_key = f.date_key
WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date)
GROUP BY d.full_date;

-- 2. Price change versus the previous snapshot (LAG per book) ---------------
WITH prices AS (
    SELECT
        b.book_key,
        b.title,
        c.category_name,
        d.full_date,
        f.price_gbp,
        LAG(f.price_gbp) OVER (
            PARTITION BY b.book_sk
            ORDER BY d.full_date
        ) AS previous_price
    FROM fact_book_snapshot f
    JOIN dim_book b     ON b.book_sk = f.book_sk
    JOIN dim_category c ON c.category_key = b.category_key
    JOIN dim_date d     ON d.date_key = f.date_key
)
SELECT
    title,
    category_name,
    full_date,
    previous_price,
    price_gbp,
    ROUND(price_gbp - previous_price, 2)                          AS price_change,
    ROUND(100.0 * (price_gbp - previous_price) / previous_price, 1) AS pct_change
FROM prices
WHERE previous_price IS NOT NULL
ORDER BY pct_change ASC
LIMIT 20;

-- 3. Top 3 most expensive books per category (RANK) -------------------------
WITH ranked AS (
    SELECT
        c.category_name,
        b.title,
        f.price_gbp,
        RANK() OVER (
            PARTITION BY c.category_name
            ORDER BY f.price_gbp DESC
        ) AS price_rank
    FROM fact_book_snapshot f
    JOIN dim_book b     ON b.book_sk = f.book_sk
    JOIN dim_category c ON c.category_key = b.category_key
    JOIN dim_date d     ON d.date_key = f.date_key
    WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date)
)
SELECT category_name, price_rank, title, price_gbp
FROM ranked
WHERE price_rank <= 3
ORDER BY category_name, price_rank;

-- 4. Category health on the latest snapshot ---------------------------------
SELECT
    c.category_name,
    COUNT(*)                                      AS books,
    COUNT(*) FILTER (WHERE f.in_stock)            AS in_stock,
    ROUND(AVG(f.price_gbp), 2)                    AS avg_price_gbp,
    COUNT(*) FILTER (WHERE f.in_stock) * 100.0 / COUNT(*) AS in_stock_pct
FROM fact_book_snapshot f
JOIN dim_book b     ON b.book_sk = f.book_sk
JOIN dim_category c ON c.category_key = b.category_key
JOIN dim_date d     ON d.date_key = f.date_key
WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date)
GROUP BY c.category_name
ORDER BY avg_price_gbp DESC;

-- 5. Pipeline observability -------------------------------------------------
SELECT
    r.run_id,
    r.snapshot_date,
    r.status,
    r.rows_extracted,
    r.rows_loaded,
    r.finished_at - r.started_at AS duration
FROM etl_run_log r
ORDER BY r.started_at DESC
LIMIT 10;

-- 6. Data quality sentinel: any snapshot that changed size dramatically? ----
SELECT
    d.full_date,
    COUNT(*) AS books,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY d.full_date) AS delta_vs_previous_day
FROM fact_book_snapshot f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.full_date
ORDER BY d.full_date;
