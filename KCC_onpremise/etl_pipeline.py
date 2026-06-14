"""
DB_PORT=5432
DB_NAME=posdb
DB_USER=posuser
DB_PASSWORD=pospassword
"""

from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch


REQUIRED_COLUMNS = [
    "transaction_id",
    "timestamp",
    "store_id",
    "sku",
    "sku_name",
    "quantity",
    "price",
]


@dataclass
class CleanRecord:
    transaction_id: str
    transaction_ts: datetime
    business_date: str
    store_id: str
    sku: str
    sku_name: str
    quantity: int
    price: Decimal
    source_file: str
    raw_data: str


@dataclass
class QuarantineRecord:
    raw_data: str
    error_reason: str
    source_file: str


def setup_logging() -> None:
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/etl_pipeline.log", encoding="utf-8"),
        ],
    )


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "posdb"),
        user=os.getenv("DB_USER", "posuser"),
        password=os.getenv("DB_PASSWORD", "pospassword"),
    )


def init_db(conn) -> None:
    schema_sql = """
    CREATE TABLE IF NOT EXISTS file_ingestion_log (
        file_name      VARCHAR PRIMARY KEY,
        file_path      VARCHAR NOT NULL,
        file_size      BIGINT NOT NULL,
        row_count      BIGINT NOT NULL,
        status         VARCHAR NOT NULL CHECK (status IN ('PROCESSING', 'SUCCESS', 'FAILED')),
        error_message  VARCHAR,
        ingested_at    TIMESTAMPTZ,
        started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
        id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        raw_data        JSONB NOT NULL,
        error_reason    VARCHAR NOT NULL,
        source_file     VARCHAR NOT NULL,
        quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        run_id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        run_started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        run_completed_at      TIMESTAMPTZ,
        source_file           VARCHAR NOT NULL,
        raw_count             BIGINT NOT NULL DEFAULT 0,
        clean_count           BIGINT NOT NULL DEFAULT 0,
        error_count           BIGINT NOT NULL DEFAULT 0,
        duplicate_count       BIGINT NOT NULL DEFAULT 0,
        late_record_count     BIGINT NOT NULL DEFAULT 0,
        status                VARCHAR NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
        error_message         VARCHAR
    );

    CREATE INDEX IF NOT EXISTS idx_fact_sales_timestamp ON fact_sales(timestamp);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_business_date ON fact_sales(business_date);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_store_date ON fact_sales(store_id, business_date);
    CREATE INDEX IF NOT EXISTS idx_quarantine_source_file ON error_quarantine(source_file);
    CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_source_file ON pipeline_run_log(source_file);
    """

    view_sql = """
    CREATE OR REPLACE VIEW vw_store_daily_summary AS
    WITH sales AS (
        SELECT
            store_id,
            business_date,
            SUM(price * quantity) AS total_revenue,
            COUNT(DISTINCT transaction_id) AS total_transactions
        FROM fact_sales
        GROUP BY store_id, business_date
    ),
    errors AS (
        SELECT
            raw_data ->> 'store_id' AS store_id,
            DATE((raw_data ->> 'timestamp')::timestamptz) AS business_date,
            COUNT(*) AS total_errors
        FROM error_quarantine
        WHERE raw_data ? 'store_id'
          AND raw_data ? 'timestamp'
          AND raw_data ->> 'timestamp' IS NOT NULL
          AND raw_data ->> 'timestamp' <> ''
        GROUP BY
            raw_data ->> 'store_id',
            DATE((raw_data ->> 'timestamp')::timestamptz)
    )
    SELECT
        s.store_id,
        s.business_date,
        ROUND(s.total_revenue, 2) AS total_revenue,
        s.total_transactions,
        COALESCE(e.total_errors, 0) AS total_errors
    FROM sales s
    LEFT JOIN errors e
        ON e.store_id = s.store_id
       AND e.business_date = s.business_date;
    """

    with conn.cursor() as cur:
        cur.execute(schema_sql)
        cur.execute(view_sql)

    conn.commit()


def discover_csv_files(raw_dir: Path) -> List[Path]:
    if not raw_dir.exists():
        logging.warning("Raw directory does not exist: %s", raw_dir)
        return []
    return sorted(raw_dir.glob("*.csv"))


def file_already_ingested(conn, file_path: Path) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM file_ingestion_log
            WHERE file_name = %s
              AND file_size = %s
              AND status = 'SUCCESS'
            """,
            (file_path.name, file_path.stat().st_size),
        )
        return cur.fetchone() is not None


def mark_file_processing(conn, file_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO file_ingestion_log (
                file_name,
                file_path,
                file_size,
                row_count,
                status,
                started_at
            )
            VALUES (%s, %s, %s, 0, 'PROCESSING', NOW())
            ON CONFLICT (file_name) DO UPDATE SET
                file_path = EXCLUDED.file_path,
                file_size = EXCLUDED.file_size,
                row_count = 0,
                status = 'PROCESSING',
                error_message = NULL,
                started_at = NOW(),
                ingested_at = NULL
            """,
            (file_path.name, str(file_path), file_path.stat().st_size),
        )
    conn.commit()


def mark_file_success(conn, file_path: Path, row_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE file_ingestion_log
            SET row_count = %s,
                status = 'SUCCESS',
                error_message = NULL,
                ingested_at = NOW()
            WHERE file_name = %s
            """,
            (row_count, file_path.name),
        )
    conn.commit()


def mark_file_failed(conn, file_path: Path, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE file_ingestion_log
            SET status = 'FAILED',
                error_message = %s,
                ingested_at = NOW()
            WHERE file_name = %s
            """,
            (error_message[:1000], file_path.name),
        )
    conn.commit()


def raw_row_json(row: Dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, default=str, sort_keys=True)


def normalize_raw_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value.strip() if isinstance(value, str) else value
        for key, value in row.items()
    }


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None or str(value).strip() == "":
        return None

    text = str(value).strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_quantity(value: Any) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None

    try:
        quantity = int(str(value).strip())
    except ValueError:
        return None

    return quantity if quantity > 0 else None


def parse_price(value: Any) -> Optional[Decimal]:
    if value is None or str(value).strip() == "":
        return None

    try:
        price = Decimal(str(value).strip())
    except InvalidOperation:
        return None

    if price <= Decimal("0"):
        return None

    return price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def validate_and_transform(
    row: Dict[str, Any],
    source_file: str,
) -> Tuple[Optional[CleanRecord], Optional[QuarantineRecord]]:
    original_json = raw_row_json(row)
    row = normalize_raw_row(row)

    transaction_id = str(row.get("transaction_id") or "").strip()
    if not transaction_id:
        return None, QuarantineRecord(original_json, "NULL_TRANSACTION_ID", source_file)

    parsed_ts = parse_timestamp(row.get("timestamp"))
    if parsed_ts is None:
        return None, QuarantineRecord(original_json, "INVALID_TIMESTAMP", source_file)

    sku = str(row.get("sku") or "").strip()
    if len(sku) > 50:
        return None, QuarantineRecord(original_json, "SKU_TOO_LONG", source_file)

    sku_name = str(row.get("sku_name") or "").strip()
    if not sku_name:
        return None, QuarantineRecord(original_json, "NULL_SKU_NAME", source_file)

    quantity = parse_quantity(row.get("quantity"))
    if quantity is None:
        return None, QuarantineRecord(original_json, "INVALID_QUANTITY", source_file)

    price = parse_price(row.get("price"))
    if price is None:
        return None, QuarantineRecord(original_json, "INVALID_PRICE", source_file)

    store_id = str(row.get("store_id") or "").strip()

    return CleanRecord(
        transaction_id=transaction_id,
        transaction_ts=parsed_ts,
        business_date=parsed_ts.date().isoformat(),
        store_id=store_id,
        sku=sku,
        sku_name=sku_name.upper(),
        quantity=quantity,
        price=price,
        source_file=source_file,
        raw_data=original_json,
    ), None


def read_and_validate_file(file_path: Path) -> Tuple[List[CleanRecord], List[QuarantineRecord], int]:
    clean_records: List[CleanRecord] = []
    quarantine_records: List[QuarantineRecord] = []
    row_count = 0

    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        missing_columns = sorted(set(REQUIRED_COLUMNS) - set(reader.fieldnames or []))
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        for row in reader:
            row_count += 1
            clean, quarantine = validate_and_transform(row, file_path.name)

            if quarantine is not None:
                quarantine_records.append(quarantine)
            elif clean is not None:
                clean_records.append(clean)

    return clean_records, quarantine_records, row_count


def infer_file_business_date(file_name: str) -> Optional[str]:
    stem = Path(file_name).stem

    if not stem.startswith("sales_day_"):
        return None

    try:
        day_number = int(stem.replace("sales_day_", ""))
    except ValueError:
        return None

    expected_date = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_number - 1)
    return expected_date.date().isoformat()


def count_late_records(records: Iterable[CleanRecord]) -> int:
    late_count = 0

    for record in records:
        expected_business_date = infer_file_business_date(record.source_file)

        if expected_business_date is not None and record.business_date != expected_business_date:
            late_count += 1

    return late_count


def dedupe_by_file(records: List[CleanRecord]) -> Tuple[List[CleanRecord], Dict[str, int]]:
    latest_by_id: Dict[str, CleanRecord] = {}
    duplicate_counts_by_file: Dict[str, int] = {}

    for record in records:
        existing = latest_by_id.get(record.transaction_id)

        if existing is None:
            latest_by_id[record.transaction_id] = record
            continue

        if record.transaction_ts > existing.transaction_ts:
            duplicate_counts_by_file[existing.source_file] = duplicate_counts_by_file.get(existing.source_file, 0) + 1
            latest_by_id[record.transaction_id] = record
        else:
            duplicate_counts_by_file[record.source_file] = duplicate_counts_by_file.get(record.source_file, 0) + 1

    return list(latest_by_id.values()), duplicate_counts_by_file


def insert_quarantine(conn, quarantine_records: List[QuarantineRecord]) -> None:
    if not quarantine_records:
        return

    rows = [
        (record.raw_data, record.error_reason, record.source_file)
        for record in quarantine_records
    ]

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO error_quarantine (
                raw_data,
                error_reason,
                source_file,
                quarantined_at
            )
            VALUES (%s::jsonb, %s, %s, NOW())
            """,
            rows,
            page_size=1000,
        )


def upsert_fact_sales(conn, records: List[CleanRecord]) -> Tuple[int, int]:
    applied_count = 0
    older_duplicate_count = 0

    with conn.cursor() as cur:
        for record in records:
            cur.execute(
                """
                INSERT INTO fact_sales (
                    transaction_id,
                    timestamp,
                    business_date,
                    store_id,
                    sku,
                    sku_name,
                    quantity,
                    price,
                    processed_at,
                    source_file
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    business_date = EXCLUDED.business_date,
                    store_id = EXCLUDED.store_id,
                    sku = EXCLUDED.sku,
                    sku_name = EXCLUDED.sku_name,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    processed_at = NOW(),
                    source_file = EXCLUDED.source_file
                WHERE EXCLUDED.timestamp > fact_sales.timestamp
                """,
                (
                    record.transaction_id,
                    record.transaction_ts,
                    record.business_date,
                    record.store_id,
                    record.sku,
                    record.sku_name,
                    record.quantity,
                    record.price,
                    record.source_file,
                ),
            )

            if cur.rowcount == 1:
                applied_count += 1
            else:
                older_duplicate_count += 1

    return applied_count, older_duplicate_count


def insert_run_log(
    conn,
    source_file: str,
    raw_count: int,
    clean_count: int,
    error_count: int,
    duplicate_count: int,
    late_record_count: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_run_log (
                run_started_at,
                run_completed_at,
                source_file,
                raw_count,
                clean_count,
                error_count,
                duplicate_count,
                late_record_count,
                status,
                error_message
            )
            VALUES (
                NOW(),
                NOW(),
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                source_file,
                raw_count,
                clean_count,
                error_count,
                duplicate_count,
                late_record_count,
                status,
                error_message,
            ),
        )


def print_summary(conn, stats: Dict[str, int]) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fact_sales")
        fact_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM error_quarantine")
        quarantine_count = cur.fetchone()[0]

    print("\\nPostgreSQL ETL run summary")
    print("--------------------------")
    print(f"Discovered files:       {stats['discovered_files']:,}")
    print(f"Processed files:        {stats['processed_files']:,}")
    print(f"Skipped files:          {stats['skipped_files']:,}")
    print(f"Failed files:           {stats['failed_files']:,}")
    print(f"Rows read this run:     {stats['rows_read']:,}")
    print(f"Quarantined this run:   {stats['quarantined_rows']:,}")
    print(f"Duplicates removed:     {stats['duplicates_removed']:,}")
    print(f"Inserted/updated sales: {stats['inserted_or_updated_sales']:,}")
    print(f"Total fact_sales rows:  {fact_count:,}")
    print(f"Total quarantine rows:  {quarantine_count:,}")


def run_pipeline(conn, raw_dir: Path) -> Dict[str, int]:
    stats = {
        "discovered_files": 0,
        "processed_files": 0,
        "skipped_files": 0,
        "failed_files": 0,
        "rows_read": 0,
        "quarantined_rows": 0,
        "duplicates_removed": 0,
        "inserted_or_updated_sales": 0,
    }

    files = discover_csv_files(raw_dir)
    stats["discovered_files"] = len(files)

    logging.info("Discovered %s CSV files in %s", len(files), raw_dir)

    all_valid_records: List[CleanRecord] = []
    file_metrics: Dict[str, Dict[str, int]] = {}

    for file_path in files:
        if file_already_ingested(conn, file_path):
            stats["skipped_files"] += 1
            logging.info("SKIPPED already ingested file: %s", file_path.name)
            continue

        logging.info("PROCESSING file: %s", file_path.name)
        mark_file_processing(conn, file_path)

        try:
            clean_records, quarantine_records, row_count = read_and_validate_file(file_path)

            insert_quarantine(conn, quarantine_records)
            all_valid_records.extend(clean_records)

            file_metrics[file_path.name] = {
                "raw_count": row_count,
                "clean_count": 0,
                "error_count": len(quarantine_records),
                "duplicate_count": 0,
                "late_record_count": count_late_records(clean_records),
            }

            stats["processed_files"] += 1
            stats["rows_read"] += row_count
            stats["quarantined_rows"] += len(quarantine_records)

            mark_file_success(conn, file_path, row_count)
            conn.commit()

            logging.info(
                "PROCESSED %s rows=%s valid=%s quarantined=%s",
                file_path.name,
                row_count,
                len(clean_records),
                len(quarantine_records),
            )

        except Exception as exc:
            conn.rollback()
            stats["failed_files"] += 1
            logging.exception("FAILED file %s", file_path.name)

            try:
                mark_file_failed(conn, file_path, str(exc))
                insert_run_log(
                    conn=conn,
                    source_file=file_path.name,
                    raw_count=0,
                    clean_count=0,
                    error_count=0,
                    duplicate_count=0,
                    late_record_count=0,
                    status="FAILED",
                    error_message=str(exc),
                )
                conn.commit()
            except Exception:
                conn.rollback()

    deduped_records, duplicate_counts_by_file = dedupe_by_file(all_valid_records)

    for source_file, duplicate_count in duplicate_counts_by_file.items():
        if source_file in file_metrics:
            file_metrics[source_file]["duplicate_count"] = duplicate_count

    for record in deduped_records:
        if record.source_file in file_metrics:
            file_metrics[record.source_file]["clean_count"] += 1

    applied_count, db_older_duplicates = upsert_fact_sales(conn, deduped_records)

    stats["duplicates_removed"] = sum(duplicate_counts_by_file.values()) + db_older_duplicates
    stats["inserted_or_updated_sales"] = applied_count

    for source_file, metrics in file_metrics.items():
        insert_run_log(
            conn=conn,
            source_file=source_file,
            raw_count=metrics["raw_count"],
            clean_count=metrics["clean_count"],
            error_count=metrics["error_count"],
            duplicate_count=metrics["duplicate_count"],
            late_record_count=metrics["late_record_count"],
            status="SUCCESS",
        )

    conn.commit()

    return stats


def main() -> None:
    setup_logging()
    raw_dir = Path(os.getenv("RAW_DIR", "data/raw"))

    conn = get_connection()

    try:
        init_db(conn)
        stats = run_pipeline(conn, raw_dir)
        print_summary(conn, stats)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
