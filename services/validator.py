"""
Validator Service — port 8003

Last line of defense AND sole database gateway.
Every request — query or schema op — must pass through POST /execute.
Validates first, then executes against the database if valid.
After schema-changing ops, pushes updated schema to Schema Manager.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
import services.database as database
import config

app = FastAPI(title="Validator")


class ExecuteRequest(BaseModel):
    type: str        # "query" | "schema_op"
    action: str      # "SELECT" | "INSERT" | "CREATE_TABLE" | "ALTER" | "DROP"
    target: str      # table name
    payload: dict = {}
    schema: dict     # current schema snapshot from Schema Manager
    db: str = config.DEFAULT_DB  # which database to operate on


# --- Validate + Execute (single entry point) ---

@app.post("/execute")
async def execute(req: ExecuteRequest):
    """Validate the request, execute if valid, return result."""
    validation = _validate(req)
    if not validation["valid"]:
        return {"ok": False, "reason": validation["reason"]}

    result = _run(req)

    # After schema ops, push updated schema to Schema Manager
    if req.type == "schema_op" and result["ok"]:
        schema = _read_schema(req.db)
        await _push_schema_update(req.db, schema)

    return result


# --- Schema reader (used by Schema Manager on startup) ---

@app.get("/schema")
async def get_schema(db: str = config.DEFAULT_DB):
    """Read current schema directly from the database."""
    return {"ok": True, "schema": _read_schema(db)}


# --- Validation logic ---

def _validate(req: ExecuteRequest) -> dict:
    action = req.action
    target = req.target
    payload = req.payload
    schema = req.schema

    if action == "CREATE_TABLE":
        return _validate_create(target, payload, schema)
    elif action == "SELECT":
        return _validate_select(target, schema)
    elif action == "INSERT":
        return _validate_insert(target, payload, schema)
    elif action == "ALTER":
        return _validate_alter(target, schema)
    elif action == "DROP":
        return _validate_drop(target, schema)
    else:
        return {"valid": False, "reason": f"Unknown action: '{action}'"}


def _validate_create(table, payload, schema):
    if table in schema:
        return {"valid": False, "reason": f"Table '{table}' already exists"}
    if not payload.get("columns"):
        return {"valid": False, "reason": "CREATE_TABLE requires at least one column"}
    return {"valid": True}


def _validate_select(table, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    return {"valid": True}


def _validate_insert(table, payload, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    known = {c["name"] for c in schema[table]}
    incoming = set(payload.get("values", {}).keys())
    unknown = incoming - known
    if unknown:
        return {"valid": False, "reason": f"Unknown columns: {unknown}"}
    return {"valid": True}


def _validate_alter(table, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    return {"valid": True}


def _validate_drop(table, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    return {"valid": True}


# --- Execution logic ---

def _run(req: ExecuteRequest) -> dict:
    action = req.action
    target = req.target
    payload = req.payload
    db = req.db

    if action == "SELECT":
        return database.execute(f"SELECT * FROM {target}", db=db)

    elif action == "INSERT":
        values = payload.get("values", {})
        cols = ", ".join(values.keys())
        placeholders = ", ".join("?" for _ in values)
        return database.execute(
            f"INSERT INTO {target} ({cols}) VALUES ({placeholders})",
            tuple(values.values()),
            db=db
        )

    elif action == "CREATE_TABLE":
        col_defs = ", ".join(f"{col} {dtype}" for col, dtype in payload["columns"].items())
        return database.execute(f"CREATE TABLE IF NOT EXISTS {target} ({col_defs})", db=db)

    elif action == "ALTER":
        errors = []
        for col, dtype in payload.get("add_columns", {}).items():
            r = database.execute(f"ALTER TABLE {target} ADD COLUMN {col} {dtype}", db=db)
            if not r["ok"]:
                errors.append(r["error"])
        if errors:
            return {"ok": False, "errors": errors}
        return {"ok": True}

    elif action == "DROP":
        return database.execute(f"DROP TABLE IF EXISTS {target}", db=db)

    return {"ok": False, "error": f"No executor for action: {action}"}


# --- Helpers ---

def _read_schema(db: str = config.DEFAULT_DB) -> dict:
    """Read full schema from SQLite."""
    result = database.execute("SELECT name FROM sqlite_master WHERE type='table'", db=db)
    if not result["ok"]:
        return {}
    schema = {}
    for row in result["rows"]:
        table = row["name"]
        cols = database.execute(f"PRAGMA table_info({table})", db=db)
        if cols["ok"]:
            schema[table] = [
                {"name": c["name"], "type": c["type"]}
                for c in cols["rows"]
            ]
    return schema


async def _push_schema_update(db: str, schema: dict):
    """Push updated schema to Schema Manager after a schema op."""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{config.URLS['schema']}/schema-update",
                json={"db": db, "schema": schema},
                timeout=3.0
            )
    except Exception:
        pass  # Schema Manager unreachable, non-fatal


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["validator"])
