
from __future__ import annotations

import csv
import random
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

RAW_DIR = Path("data/raw")
TOTAL_ROWS = 100_000
DAYS = 7
ERROR_RATE = 0.05
ERROR_ROWS = int(TOTAL_ROWS * ERROR_RATE)
SEED = 42
BASE_DATE = datetime(2026, 1, 1)

STORE_IDS = [f"STR-{i:03d}" for i in range(1, 31)]
PRODUCTS: List[Tuple[str, str, float]] = [
    ("SKU-10001", "Cotton T-Shirt", 14.99),
    ("SKU-10002", "Denim Jeans", 49.99),
    ("SKU-10003", "Running Shoes", 89.99),
    ("SKU-10004", "Winter Jacket", 129.99),
    ("SKU-10005", "Leather Belt", 24.99),
    ("SKU-10006", "Sports Socks", 7.99),
    ("SKU-10007", "Kitchen Towel Set", 11.99),
    ("SKU-10008", "Ceramic Mug", 9.99),
    ("SKU-10009", "Bluetooth Speaker", 39.99),
    ("SKU-10010", "Backpack", 59.99),
    ("SKU-10011", "Bed Sheet Set", 69.99),
    ("SKU-10012", "Desk Lamp", 34.99),
    ("SKU-10013", "Water Bottle", 19.99),
    ("SKU-10014", "Throw Pillow", 22.99),
    ("SKU-10015", "Scented Candle", 12.99),
]

@dataclass(frozen=True)
class ErrorPlan:
    day: int
    row_index: int
    error_type: str

def distribute_rows(total_rows: int, days: int) -> List[int]:
    base = total_rows // days
    rows = [base] * days
    for i in range(total_rows % days):
        rows[i] += 1
    return rows

def random_timestamp_for_day(day: int) -> datetime:
    start = BASE_DATE + timedelta(days=day - 1)
    return start + timedelta(seconds=random.randint(0, 86_399))

def make_valid_row(day: int) -> Dict[str, object]:
    sku, sku_name, base_price = random.choice(PRODUCTS)
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": random_timestamp_for_day(day).isoformat(timespec="seconds"),
        "store_id": random.choice(STORE_IDS),
        "sku": sku,
        "sku_name": sku_name,
        "quantity": random.randint(1, 8),
        "price": f"{base_price * random.uniform(0.85, 1.20):.2f}",
    }

def make_late_row(target_file_day: int) -> Dict[str, object]:
    source_day = random.choice([1, 2]) if target_file_day == 3 else random.choice([4, 5])
    return make_valid_row(source_day)

def build_error_plan(rows_per_day: List[int]) -> Dict[int, Dict[int, str]]:
    all_positions = [(day, idx) for day, cnt in enumerate(rows_per_day, start=1) for idx in range(cnt)]
    chosen = random.sample(all_positions, ERROR_ROWS)
    error_types = (
        ["data_type_mismatch"] * 1250
        + ["schema_violation"] * 1250
        + ["null_required_field"] * 1250
        + ["duplicate_transaction"] * 1250
    )
    random.shuffle(error_types)
    grouped = {day: {} for day in range(1, DAYS + 1)}
    for (day, idx), et in zip(chosen, error_types):
        grouped[day][idx] = et
    return grouped

def apply_non_duplicate_error(row: Dict[str, object], error_type: str) -> Dict[str, object]:
    bad = deepcopy(row)
    if error_type == "data_type_mismatch":
        bad["price"] = "12.O0"
    elif error_type == "schema_violation":
        bad["sku"] = "A" * 55
    elif error_type == "null_required_field":
        if random.random() < 0.5:
            bad["transaction_id"] = ""
        else:
            bad["sku_name"] = ""
    return bad

def apply_duplicate_error(row: Dict[str, object], prior_valid_rows: List[Dict[str, object]]) -> Dict[str, object]:
    dup = deepcopy(row)
    if prior_valid_rows:
        source = random.choice(prior_valid_rows)
        dup["transaction_id"] = source["transaction_id"]
        source_ts = datetime.fromisoformat(str(source["timestamp"]))
        dup["timestamp"] = (source_ts + timedelta(minutes=random.randint(1, 720))).isoformat(timespec="seconds")
        dup["quantity"] = int(source["quantity"]) + random.randint(1, 3)
        dup["price"] = f"{float(source['price']) + random.uniform(1.00, 10.00):.2f}"
        dup["store_id"] = random.choice([s for s in STORE_IDS if s != source["store_id"]])
    return dup

def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    fieldnames = ["transaction_id", "timestamp", "store_id", "sku", "sku_name", "quantity", "price"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main() -> None:
    random.seed(SEED)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    rows_per_day = distribute_rows(TOTAL_ROWS, DAYS)
    error_plan = build_error_plan(rows_per_day)
    late_indices = {
        3: set(random.sample(range(rows_per_day[2]), random.randint(50, 100))),
        6: set(random.sample(range(rows_per_day[5]), random.randint(50, 100))),
    }
    prior_valid_rows: List[Dict[str, object]] = []
    total_errors = 0
    late_count = 0

    for day, row_count in enumerate(rows_per_day, start=1):
        rows = []
        for idx in range(row_count):
            if day in late_indices and idx in late_indices[day]:
                row = make_late_row(day)
                late_count += 1
            else:
                row = make_valid_row(day)

            et = error_plan[day].get(idx)
            if et:
                total_errors += 1
                row = apply_duplicate_error(row, prior_valid_rows) if et == "duplicate_transaction" else apply_non_duplicate_error(row, et)
            else:
                prior_valid_rows.append(deepcopy(row))
            rows.append(row)

        out = RAW_DIR / f"sales_day_{day}.csv"
        write_csv(out, rows)
        print(f"Wrote {out} with {len(rows):,} rows")

    print("\nGeneration complete")
    print(f"Total rows: {TOTAL_ROWS:,}")
    print(f"Injected error rows: {total_errors:,} ({total_errors / TOTAL_ROWS:.2%})")
    print(f"Late-arriving rows: {late_count:,}")

if __name__ == "__main__":
    main()
