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

### Docker commands to build, generate csv , run ETL

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
| transaction_id | Not nullable        |
| price          | Numeric and > 0 |
| quantity       | Integer and > 0 |
| sku            | Max length 50   |
| sku_name       | Not nullable        |
| timestamp      | Must parse as valid datetime  |

Invalid records are stored in `error_quarantine`.

---

## Deduplication

Duplicate transactions are identified using:

```text
transaction_id
```

1. Identify duplicate transaction_id values within and across files
2. Keep the record with the latest timestamp; discard the rest
3. Log how many duplicates were removed per run

