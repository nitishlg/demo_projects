# POS ETL Pipeline

## Overview

This project implements a production-grade ETL pipeline for retail Point-of-Sale (POS) transaction data.

The pipeline:

* Generates 7 days of POS transaction data (~100,000 records)
* Simulates real-world data quality issues
* Validates and cleans incoming data
* Routes invalid records to a quarantine table
* Deduplicates transactions using the latest timestamp
* Loads clean data into PostgreSQL
* Produces store-level reporting and reconciliation metrics

---

## Tech Stack

* Python 3.8+
* PostgreSQL
* Docker & Docker Compose
* DBeaver (optional for database inspection)

---

## Project Structure

```text
pos-etl/
├── data/
│   └── raw/
├── sql/
│   ├── schema.sql
│   └── views.sql
├── generate_data.py
├── etl_pipeline.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Setup

### Start PostgreSQL

```bash
docker compose -f docker-compose.postgresql.yml build --no-cache 
docker compose -f docker-compose.postgresql.yml up -d postgres 
docker compose -f docker-compose.postgresql.yml run --rm generate-data 
docker compose -f docker-compose.postgresql.yml run --rm etl
```

### Verify Database

```bash
docker ps
```

---

## Generate Test Data

Generate 7 CSV files containing approximately 100,000 records:

```bash
python generate_data.py
```

Files are created in:

```text
data/raw/
```

---

## Run ETL Pipeline

```bash
python etl_pipeline.py
```

The pipeline will:

1. Discover CSV files automatically
2. Validate records
3. Quarantine invalid rows
4. Deduplicate transactions
5. Load clean data into PostgreSQL

---

## Database Tables

### fact_sales

Stores validated transaction records.

### error_quarantine

Stores invalid records and validation errors.

### file_ingestion_log

Tracks processed files and supports idempotent re-runs.

### pipeline_run_log

Stores reconciliation and processing metrics.

---

## Validation Rules

| Field          | Rule            |
| -------------- | --------------- |
| transaction_id | Required        |
| price          | Numeric and > 0 |
| quantity       | Integer and > 0 |
| sku            | Max length 50   |
| sku_name       | Required        |
| timestamp      | Valid datetime  |

Invalid records are stored in `error_quarantine`.

---

## Deduplication

Duplicate transactions are identified using:

```text
transaction_id
```

If duplicates exist, the record with the latest timestamp is retained.

---

## Reporting

### Store Performance View

```sql
SELECT *
FROM vw_store_daily_summary;
```

### Reconciliation Report

```sql
SELECT *
FROM pipeline_run_log
ORDER BY source_file;
```

---

## Assumptions

* All timestamps are converted to UTC.
* Business date is derived from transaction timestamp.
* Duplicate transaction IDs keep the latest transaction timestamp.
* Late-arriving records are processed using event time, not file arrival time.

---

## Performance

Target:

```text
100,000 rows processed in under 30 seconds
```

Optimizations:

* Streaming CSV processing
* Batch database inserts
* Indexed lookup fields
* Idempotent file tracking
* SQL-based deduplication

---

## Future Improvements

* Airflow orchestration
* Data quality dashboard
* Automated alerting
* Partitioned fact tables
* Cloud object storage integration
