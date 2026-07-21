
import os
import subprocess
import sys

import psycopg2

DB = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", "5432"),
    "dbname":   os.getenv("DB_NAME", "postgres"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Qwer@1234"),
}

SQL_FILES = [
    "sql/schema_all.sql",
    "sql/procedures_all.sql",
    "sql/views.sql",
]


def run_sql_file(conn, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("   ran", path)


def run_python(script):
    print("   running", script)
    subprocess.run([sys.executable, script], check=True)


def main():
    print("[1/4] Creating tables, procedures, views ...")
    conn = psycopg2.connect(**DB)
    for path in SQL_FILES:
        run_sql_file(conn, path)

    print("[2/4] Generating CSV data ...")
    run_python("generate_data.py")

    print("[3/4] Loading data ...")
    run_python("loader.py")

    print("[4/4] Results")

    cur = conn.cursor()
    cur.execute(
        "SELECT source_file, raw_count, valid_count, error_count, "
        "       duplicate_count, late_record_count "
        "FROM pipeline_run_log ORDER BY source_file"
    )
    print("   per-file results (written by sp_process_file):")
    print("   %-18s %8s %8s %8s %8s %8s" %
          ("file", "raw", "valid", "error", "dup", "late"))
    for r in cur.fetchall():
        print("   %-18s %8s %8s %8s %8s %8s" % r)

    # Overall reconciliation (reads the fact table + the run log).
    cur.execute("SELECT * FROM vw_reconciliation")
    columns = [d[0] for d in cur.description]
    row = cur.fetchone()
    print("\n   reconciliation totals:")
    for name, value in zip(columns, row):
        print("   %-20s %s" % (name, value))

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()