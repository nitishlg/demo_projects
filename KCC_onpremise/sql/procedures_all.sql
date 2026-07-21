CREATE OR REPLACE FUNCTION f_try_ts(p_text TEXT)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_text IS NULL OR btrim(p_text) = '' THEN
        RETURN NULL;
    END IF;
    RETURN btrim(p_text)::timestamptz;
EXCEPTION WHEN others THEN
    RETURN NULL;
END; $$;

CREATE OR REPLACE FUNCTION f_try_numeric(p_text TEXT)
RETURNS NUMERIC LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_text IS NULL OR btrim(p_text) = '' THEN
        RETURN NULL;
    END IF;
    RETURN btrim(p_text)::numeric;
EXCEPTION WHEN others THEN
    RETURN NULL;
END; $$;

CREATE OR REPLACE FUNCTION f_try_int(p_text TEXT)
RETURNS INTEGER LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_text IS NULL OR btrim(p_text) = '' THEN
        RETURN NULL;
    END IF;
    RETURN btrim(p_text)::integer;
EXCEPTION WHEN others THEN
    RETURN NULL;
END; $$;

CREATE OR REPLACE PROCEDURE sp_flag_malformed(p_file VARCHAR, p_expected_cols INT)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO error_quarantine (source_file, line_no, raw_data, error_reason)
    SELECT
        s.source_file,
        s.line_no,
        s.raw_line,
        'COLUMN_COUNT_MISMATCH(' || cardinality(string_to_array(s.raw_line, ',')) ||
        ' cols, expected ' || p_expected_cols || ')'
    FROM stg_raw_lines s
    WHERE s.source_file = p_file
      AND cardinality(string_to_array(s.raw_line, ',')) <> p_expected_cols;
END; $$;

CREATE OR REPLACE PROCEDURE sp_parse_validate_load(
    p_file          VARCHAR,
    p_file_date     DATE,
    p_expected_cols INT
)
LANGUAGE plpgsql AS $$
BEGIN
    CREATE TEMP TABLE tmp_parsed ON COMMIT DROP AS
    SELECT
        s.line_no,
        s.raw_line,
        f[1] AS transaction_id,
        f[2] AS ts_text,
        f[3] AS store_id,
        f[4] AS sku,
        f[5] AS sku_name,
        f[6] AS quantity_text,
        f[7] AS price_text
    FROM stg_raw_lines s
    CROSS JOIN LATERAL (SELECT string_to_array(s.raw_line, ',') AS f) x
    WHERE s.source_file = p_file
      AND cardinality(f) = p_expected_cols;

    CREATE TEMP TABLE tmp_checked ON COMMIT DROP AS
    SELECT
        p.*,
        CASE
            WHEN COALESCE(btrim(transaction_id),'') = ''      THEN 'NULL_TRANSACTION_ID'
            WHEN f_try_ts(ts_text) IS NULL                    THEN 'INVALID_TIMESTAMP'
            WHEN length(btrim(sku)) > 50                      THEN 'SKU_TOO_LONG'
            WHEN COALESCE(btrim(sku_name),'') = ''            THEN 'NULL_SKU_NAME'
            WHEN f_try_int(quantity_text) IS NULL
              OR f_try_int(quantity_text) <= 0                THEN 'INVALID_QUANTITY'
            WHEN f_try_numeric(price_text) IS NULL
              OR f_try_numeric(price_text) <= 0               THEN 'INVALID_PRICE'
            ELSE NULL
        END AS error_reason
    FROM tmp_parsed p;

    INSERT INTO error_quarantine (source_file, line_no, raw_data, error_reason)
    SELECT p_file, line_no, raw_line, error_reason
    FROM tmp_checked
    WHERE error_reason IS NOT NULL;

    CREATE TEMP TABLE tmp_valid ON COMMIT DROP AS
    SELECT
        btrim(transaction_id)                        AS transaction_id,
        f_try_ts(ts_text)                            AS timestamp,
        (f_try_ts(ts_text) AT TIME ZONE 'UTC')::date AS business_date,
        btrim(store_id)                              AS store_id,
        btrim(sku)                                   AS sku,
        upper(btrim(sku_name))                       AS sku_name,
        f_try_int(quantity_text)                     AS quantity,
        round(f_try_numeric(price_text), 2)          AS price,
        p_file                                       AS source_file
    FROM tmp_checked
    WHERE error_reason IS NULL;

    CREATE TEMP TABLE tmp_deduped ON COMMIT DROP AS
    SELECT * FROM (
        SELECT v.*,
               ROW_NUMBER() OVER (PARTITION BY transaction_id
                                  ORDER BY timestamp DESC) AS rn
        FROM tmp_valid v
    ) z
    WHERE rn = 1;

    INSERT INTO fact_sales (
        transaction_id, timestamp, business_date, store_id,
        sku, sku_name, quantity, price, processed_at, source_file
    )
    SELECT transaction_id, timestamp, business_date, store_id,
           sku, sku_name, quantity, price, NOW(), source_file
    FROM tmp_deduped
    ON CONFLICT (transaction_id) DO UPDATE SET
        timestamp     = EXCLUDED.timestamp,
        business_date = EXCLUDED.business_date,
        store_id      = EXCLUDED.store_id,
        sku           = EXCLUDED.sku,
        sku_name      = EXCLUDED.sku_name,
        quantity      = EXCLUDED.quantity,
        price         = EXCLUDED.price,
        processed_at  = NOW(),
        source_file   = EXCLUDED.source_file
    WHERE EXCLUDED.timestamp > fact_sales.timestamp;
END; $$;

CREATE OR REPLACE PROCEDURE sp_process_file(
    p_file          VARCHAR,
    p_file_date     DATE,
    p_expected_cols INT DEFAULT 7
)
LANGUAGE plpgsql AS $$
DECLARE
    v_raw   BIGINT;
    v_err   BIGINT;
    v_valid BIGINT;
    v_dupe  BIGINT;
    v_late  BIGINT;
BEGIN
    SELECT count(*) INTO v_raw
    FROM stg_raw_lines WHERE source_file = p_file;

    CALL sp_flag_malformed(p_file, p_expected_cols);
    CALL sp_parse_validate_load(p_file, p_file_date, p_expected_cols);

    SELECT count(*) INTO v_err
    FROM error_quarantine WHERE source_file = p_file;

    v_valid := v_raw - v_err;

    WITH file_valid AS (
        SELECT string_to_array(s.raw_line, ',') AS f
        FROM stg_raw_lines s
        WHERE s.source_file = p_file
          AND cardinality(string_to_array(s.raw_line, ',')) = p_expected_cols
          AND NOT EXISTS (SELECT 1 FROM error_quarantine q
                          WHERE q.source_file = s.source_file
                            AND q.line_no    = s.line_no)
    )
    SELECT
        count(*) - count(DISTINCT btrim(f[1])),
        count(*) FILTER (WHERE (f_try_ts(f[2]) AT TIME ZONE 'UTC')::date <> p_file_date)
    INTO v_dupe, v_late
    FROM file_valid;

    INSERT INTO pipeline_run_log (
        source_file, raw_count, valid_count, error_count,
        duplicate_count, late_record_count
    )
    VALUES (p_file, v_raw, v_valid, v_err, v_dupe, v_late);
END; $$;
