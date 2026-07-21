
CREATE TABLE IF NOT EXISTS file_schema_master (
    pattern        VARCHAR PRIMARY KEY,
    expected_cols  TEXT NOT NULL,
    description    VARCHAR
);

INSERT INTO file_schema_master (pattern, expected_cols, description)
VALUES (
    '^sales_day_\d+\.csv$',
    'transaction_id,timestamp,store_id,sku,sku_name,quantity,price',
    'Daily POS sales export'
)
ON CONFLICT (pattern) DO NOTHING;


CREATE TABLE IF NOT EXISTS file_ingestion_log (
    file_name      VARCHAR PRIMARY KEY,
    file_path      VARCHAR NOT NULL,
    file_size      BIGINT  NOT NULL,
    row_count      BIGINT  NOT NULL DEFAULT 0,     -- data lines (excludes header)
    status         VARCHAR NOT NULL
                   CHECK (status IN ('PROCESSING','SUCCESS','FAILED')),
    error_message  VARCHAR,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingested_at    TIMESTAMPTZ
);


CREATE TABLE IF NOT EXISTS stg_raw_lines (
    source_file  VARCHAR NOT NULL,
    line_no      INTEGER NOT NULL,
    raw_line     TEXT    NOT NULL,
    PRIMARY KEY (source_file, line_no)
);


CREATE TABLE IF NOT EXISTS fact_sales (
    transaction_id VARCHAR PRIMARY KEY,
    timestamp      TIMESTAMPTZ NOT NULL,
    business_date  DATE NOT NULL,
    store_id       VARCHAR NOT NULL,
    sku            VARCHAR(50) NOT NULL,
    sku_name       VARCHAR NOT NULL,
    quantity       INTEGER NOT NULL,
    price          NUMERIC(10,2) NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file    VARCHAR NOT NULL
);


CREATE TABLE IF NOT EXISTS error_quarantine (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file    VARCHAR NOT NULL,
    line_no        INTEGER,
    raw_data       TEXT    NOT NULL,              -- the original line, verbatim
    error_reason   VARCHAR NOT NULL,              -- e.g. INVALID_PRICE, COLUMN_COUNT_MISMATCH
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file        VARCHAR NOT NULL,
    raw_count          BIGINT NOT NULL DEFAULT 0,  -- physical lines (excl. header)
    valid_count        BIGINT NOT NULL DEFAULT 0,  -- passed all validation (pre-dedup)
    error_count        BIGINT NOT NULL DEFAULT 0,  -- quarantined (malformed + rule fails)
    duplicate_count    BIGINT NOT NULL DEFAULT 0,  -- WITHIN-file duplicates only
    late_record_count  BIGINT NOT NULL DEFAULT 0,  -- valid rows whose event date <> file date
    run_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_fact_business_date ON fact_sales(business_date);
CREATE INDEX IF NOT EXISTS idx_fact_store_date    ON fact_sales(store_id, business_date);
CREATE INDEX IF NOT EXISTS idx_quar_file          ON error_quarantine(source_file);
