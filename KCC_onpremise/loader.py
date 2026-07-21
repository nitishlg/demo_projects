import os ,sys
import re
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

DB = {
    "host": os.getenv("PG_DB_HOST", "localhost"),
    "port": os.getenv("PG_DB_PORT", "5432"),
    "dbname": os.getenv("PG_DB_NAME", "postgres"),
    "user": os.getenv("PG_DB_USER", "postgres"),
    "password": os.getenv("PG_DB_PASSWORD", "Qwer@1234"),
}

RAW_DIR = os.getenv("RAW_DIR", "data/raw")
START_DATE = datetime(2026, 1, 1)


def get_file_rules(cur):
    cur.execute("SELECT pattern, expected_cols FROM file_schema_master")
    rules = []
    for pattern, cols_text in cur.fetchall():
        columns = cols_text.split(",")
        rules.append((pattern, columns))
    return rules


def find_expected_cols(file_name, rules):
    for pattern, columns in rules:
        if re.match(pattern, file_name):
            return columns
    return None


def file_business_date(file_name):
    match = re.match(r"^sales_day_(\d+)\.csv$", file_name)
    if not match:
        return None
    day_number = int(match.group(1))
    return (START_DATE + timedelta(days=day_number - 1)).date()


def already_loaded(cur, file_name, file_size):
    cur.execute(
        "SELECT 1 FROM file_ingestion_log "
        "WHERE file_name = %s AND file_size = %s AND status = 'SUCCESS'",
        (file_name, file_size),
    )
    return cur.fetchone() is not None


def set_status(conn, file_name, file_path, file_size, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO file_ingestion_log "
        "  (file_name, file_path, file_size, row_count, status, error_message, "
        "   started_at, ingested_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) "
        "ON CONFLICT (file_name) DO UPDATE SET "
        "  file_size = EXCLUDED.file_size, row_count = EXCLUDED.row_count, "
        "  status = EXCLUDED.status, error_message = EXCLUDED.error_message, "
        "  ingested_at = NOW()",
        (file_name, file_path, file_size, rows, status, error),
    )
    conn.commit()


def read_header(path):
    with open(path, newline="") as f:
        first_line = f.readline().strip()
    return first_line.split(",")


def load_raw_lines(cur, file_name, path):
    rows = []
    with open(path) as f:
        next(f)  # skip the header line
        line_no = 0
        for line in f:
            line = line.strip()
            if line != "":
                line_no += 1
                rows.append((file_name, line_no, line))
    cur.execute("DELETE FROM stg_raw_lines WHERE source_file = %s", (file_name,))
    execute_values(
        cur,
        "INSERT INTO stg_raw_lines (source_file, line_no, raw_line) VALUES %s",
        rows,
    )
    return len(rows)


def process_file(conn, path, rules):
    file_name = os.path.basename(path)
    file_size = os.path.getsize(path)
    cur = conn.cursor()


    if already_loaded(cur, file_name, file_size):
        print("skip (already loaded):", file_name)
        return


    expected_cols = find_expected_cols(file_name, rules)
    if expected_cols is None:
        print("skip (unknown file type):", file_name)
        return

    set_status(conn, file_name, path, file_size, "PROCESSING")
    try:
        header = read_header(path)
        if header != expected_cols:
            raise ValueError("bad header: got %s, expected %s" % (header, expected_cols))

        # 4) load raw lines, then hand off to the database procedures
        row_count = load_raw_lines(cur, file_name, path)
        conn.commit()
        cur.execute(
            "CALL sp_process_file(%s, %s, %s)",
            (file_name, file_business_date(file_name), len(expected_cols)),
        )
        conn.commit()

        set_status(conn, file_name, path, file_size, "SUCCESS", rows=row_count)
        print("done:", file_name, "(", row_count, "rows )")
    except Exception as error:
        conn.rollback()
        set_status(conn, file_name, path, file_size, "FAILED", error=str(error)[:300])
        print("FAILED:", file_name, "-", error)


def main():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    rules = get_file_rules(cur)
    print(os.listdir(RAW_DIR))
    for name in sorted(os.listdir(RAW_DIR)):
        if name.endswith(".csv"):
            process_file(conn, os.path.join(RAW_DIR, name), rules)

    conn.close()



if __name__ == "__main__":
    main()
    # ok = test_connection()
    # sys.exit(0 if ok else 1)