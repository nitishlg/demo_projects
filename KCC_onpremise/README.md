# POS Sales ETL Pipeline

A simple on-prem pipeline that loads daily store sales CSV files into
PostgreSQL. Python lands the raw data; PostgreSQL procedures clean,
validate, deduplicate, and load it.

## Requirements
- Python 3.8+
- PostgreSQL 

## Setup
Run the SQL files once, in this order:

```powershell
"C:\Program Files\PostgreSQL\18\bin\psql.exe"  -U postgres -d postgres -f D:\Aai.com\kcc\pos_etl_full_postgresql_repo\demo_projects\KCC_onpremise\sql\schema_all.sql
"C:\Program Files\PostgreSQL\18\bin\psql.exe"  -U postgres -d postgres -f D:\Aai.com\kcc\pos_etl_full_postgresql_repo\demo_projects\KCC_onpremise\sql\procedures_all.sql
"C:\Program Files\PostgreSQL\18\bin\psql.exe"  -U postgres -d postgres -f D:\Aai.com\kcc\pos_etl_full_postgresql_repo\demo_projects\KCC_onpremise\sql\views.sql
```

 
 

## How to run
```bash
python generate_data_simple.py   # 1. make 7 CSV files in data/raw/
python loader_simple.py          # 2. load them into the database
```

The loader does everything: it reads each file, calls the database
procedures, and loads the results. There is no separate step after it.

## See the results
```sql
SELECT * FROM vw_reconciliation;        -- total recs : raw / clean / errors / duplicates
SELECT * FROM vw_file_reconciliation;   -- per-file breakdown
SELECT * FROM vw_store_daily_summary;   -- revenue per store per day
SELECT error_reason, COUNT(*) FROM error_quarantine GROUP BY 1;
```

## What it handles
- Bad rows (wrong price, long SKU, missing fields) -> quarantined, not dropped
- Duplicate transactions -> keeps the latest one
- Late files -> a sale is counted on its real date, not the file's date
- Re-running -> already-loaded files are skipped (safe to run again)

## Result
~100,000 rows load in under 30 seconds.
Totals reconcile: raw = clean + errors + duplicates.

## Tables
| Table | What it holds |
|-------|---------------|
| `file_schema_master` | which files to accept and their columns |
| `file_ingestion_log` | which files were loaded (for idempotency) |
| `stg_raw_lines` | raw lines, straight from the CSV |
| `fact_sales` | clean, deduplicated sales |
| `error_quarantine` | rejected rows and the reason |
| `pipeline_run_log` | per-file counts |
