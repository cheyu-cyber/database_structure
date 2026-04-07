"""
data_loader.py — standalone admin tool to load CSV data into a new database.

All database access goes through Validator (the sole DB gateway).

Usage:
    python data_loader.py <db_name> <file.csv>

Example:
    python data_loader.py sales sales_data.csv

Flow:
  1. Read CSV, infer column types from data
  2. Send CREATE_TABLE to Validator /execute
  3. Send INSERT per row to Validator /execute
"""

import sys
import os
import csv

sys.path.append(os.path.dirname(__file__))
import httpx
import config

VALIDATOR_URL = f"{config.URLS['validator']}/execute"


def infer_type(value: str) -> str:
    """Infer SQLite type from a sample value: INTEGER > REAL > TEXT."""
    try:
        int(value)
        return "INTEGER"
    except ValueError:
        pass
    try:
        float(value)
        return "REAL"
    except ValueError:
        pass
    return "TEXT"


def infer_schema(reader, header: list[str]) -> tuple[dict[str, str], list[dict]]:
    """
    Read all rows, infer column types from first non-empty value per column.
    Returns (columns_dict, all_rows).
    """
    rows = list(reader)
    col_types = {col: "TEXT" for col in header}

    for row in rows:
        for col in header:
            val = row.get(col, "").strip()
            if val and col_types[col] == "TEXT":
                col_types[col] = infer_type(val)

    # Second pass: widen types if needed (e.g., a column has both int and float)
    for row in rows:
        for col in header:
            val = row.get(col, "").strip()
            if not val:
                continue
            inferred = infer_type(val)
            if col_types[col] == "INTEGER" and inferred == "REAL":
                col_types[col] = "REAL"
            elif col_types[col] == "INTEGER" and inferred == "TEXT":
                col_types[col] = "TEXT"
            elif col_types[col] == "REAL" and inferred == "TEXT":
                col_types[col] = "TEXT"

    return col_types, rows


def load(db_name: str, csv_path: str):
    table_name = os.path.splitext(os.path.basename(csv_path))[0]

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        if not header:
            print("Error: CSV has no header row")
            sys.exit(1)

        columns, rows = infer_schema(reader, header)

    print(f"Database:  {db_name}")
    print(f"Table:     {table_name}")
    print(f"Columns:   {columns}")
    print(f"Rows:      {len(rows)}")
    print()

    # Schema is empty for a new db — CREATE_TABLE validation expects table to NOT exist
    schema = {}

    # 1. CREATE TABLE
    print("Creating table...", end=" ")
    resp = httpx.post(VALIDATOR_URL, json={
        "type": "schema_op",
        "action": "CREATE_TABLE",
        "target": table_name,
        "payload": {"columns": columns},
        "schema": schema,
        "db": db_name
    }, timeout=10.0)
    result = resp.json()
    if not result.get("ok"):
        print(f"FAILED: {result.get('reason') or result.get('error')}")
        sys.exit(1)
    print("OK")

    # 2. INSERT rows
    success = 0
    errors = 0
    for i, row in enumerate(rows):
        values = {col: row.get(col, "") for col in header}
        # Need updated schema for inserts (table now exists)
        # Fetch once after table creation
        if i == 0:
            schema_resp = httpx.get(
                f"{config.URLS['validator']}/schema",
                params={"db": db_name},
                timeout=5.0
            )
            schema = schema_resp.json().get("schema", {})

        resp = httpx.post(VALIDATOR_URL, json={
            "type": "query",
            "action": "INSERT",
            "target": table_name,
            "payload": {"values": values},
            "schema": schema,
            "db": db_name
        }, timeout=10.0)
        result = resp.json()
        if result.get("ok"):
            success += 1
        else:
            errors += 1
            if errors <= 3:
                print(f"  Row {i+1} error: {result.get('reason') or result.get('error')}")

    print(f"\nDone: {success} inserted, {errors} errors")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python data_loader.py <db_name> <file.csv>")
        sys.exit(1)

    db_name = sys.argv[1]
    csv_path = sys.argv[2]

    if not os.path.exists(csv_path):
        print(f"Error: file not found: {csv_path}")
        sys.exit(1)

    load(db_name, csv_path)
