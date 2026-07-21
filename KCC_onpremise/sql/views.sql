CREATE OR REPLACE VIEW vw_reconciliation AS
SELECT
    (SELECT COALESCE(SUM(raw_count),0)   FROM pipeline_run_log) AS raw_total,
    (SELECT COUNT(*)                     FROM fact_sales)       AS clean_loaded,
    (SELECT COALESCE(SUM(error_count),0) FROM pipeline_run_log) AS error_total,
    (SELECT COALESCE(SUM(valid_count),0) FROM pipeline_run_log)
        - (SELECT COUNT(*)               FROM fact_sales)       AS duplicates_removed,
    (SELECT COALESCE(SUM(late_record_count),0) FROM pipeline_run_log) AS late_total;

CREATE OR REPLACE VIEW vw_file_reconciliation AS
SELECT source_file, raw_count, valid_count, error_count,
       duplicate_count AS within_file_duplicates, late_record_count
FROM pipeline_run_log
ORDER BY source_file;

CREATE OR REPLACE VIEW vw_store_daily_summary AS
SELECT
    store_id,
    business_date,
    SUM(price * quantity)          AS total_revenue,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    SUM(quantity)                  AS total_units
FROM fact_sales
GROUP BY store_id, business_date
ORDER BY store_id, business_date;
