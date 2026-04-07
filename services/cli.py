"""
CLI — the only human-facing interface.
Sends all input as HTTP POST to Query Service.
Knows nothing about schema, validation, or the database.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import httpx
import json
import config

QUERY_URL = f"{config.URLS['query']}/query"


def print_result(result: dict):
    if result.get("ok"):
        rows = result.get("rows")
        if rows is not None:
            if rows:
                for row in rows:
                    print(" ", row)
            else:
                print("  (no rows)")
        else:
            print("  OK")
    else:
        reason = result.get("reason") or result.get("error") or "Unknown error"
        print(f"  Error: {reason}")


def run():
    current_db = config.DEFAULT_DB

    print("SQL System CLI")
    print("Commands: select <table> | insert <table> col=val ... |")
    print("          create table <table> col:TYPE ... | alter <table> add col:TYPE |")
    print("          drop <table> | use <db_name> | or just type naturally")
    print("Type 'exit' to quit.\n")

    while True:
        try:
            user_input = input(f"{current_db}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye")
            break

        if not user_input:
            continue
        if user_input.lower() == "exit":
            print("Bye")
            break

        # Switch database
        parts = user_input.split()
        if parts[0].lower() == "use" and len(parts) >= 2:
            current_db = parts[1]
            print(f"  Switched to database: {current_db}")
            continue

        try:
            resp = httpx.post(
                QUERY_URL,
                json={"input": user_input, "db": current_db},
                timeout=30.0
            )
            result = resp.json()
        except httpx.ConnectError:
            print("  Error: Query Service not reachable. Is run.py running?")
            continue
        except Exception as e:
            print(f"  Error: {e}")
            continue

        print_result(result)


if __name__ == "__main__":
    run()
