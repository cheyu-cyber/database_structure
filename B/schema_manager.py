"""
Schema Manager Service — port 8001

Stateful: owns and caches the current schema.
Pub/Sub:  services register a webhook URL. On any schema change,
          Schema Manager POSTs the updated schema to all subscribers.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import httpx
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
import database
import config

app = FastAPI(title="Schema Manager")

# --- State ---
_subscribers: list[str] = []   # registered webhook URLs
_schema_cache: dict = {}        # in-memory floor plan


# --- Models ---
class CreateTableRequest(BaseModel):
    table: str
    columns: dict   # { "col_name": "TYPE", ... }

class AlterTableRequest(BaseModel):
    table: str
    add_columns: dict

class SubscribeRequest(BaseModel):
    webhook_url: str


# --- Pub/Sub ---
async def _notify_subscribers(schema: dict):
    """Fire-and-forget: POST updated schema to all registered subscribers."""
    async with httpx.AsyncClient() as client:
        for url in _subscribers:
            try:
                await client.post(url, json=schema, timeout=3.0)
            except Exception:
                pass  # subscriber unreachable, skip


def _refresh_cache():
    """Re-read schema from SQLite into memory."""
    global _schema_cache
    result = database.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    if not result["ok"]:
        return
    schema = {}
    for row in result["rows"]:
        table = row["name"]
        cols = database.execute(f"PRAGMA table_info({table})")
        if cols["ok"]:
            schema[table] = [
                {"name": c["name"], "type": c["type"]}
                for c in cols["rows"]
            ]
    _schema_cache = schema


# --- Endpoints ---
@app.on_event("startup")
async def startup():
    _refresh_cache()


@app.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    """Register a webhook URL to receive schema change notifications."""
    if req.webhook_url not in _subscribers:
        _subscribers.append(req.webhook_url)
    return {"ok": True, "subscribers": len(_subscribers)}


@app.get("/schema")
async def get_schema():
    """Return the current schema (floor plan)."""
    return {"ok": True, "schema": _schema_cache}


@app.post("/schema/create")
async def create_table(req: CreateTableRequest):
    col_defs = ", ".join(f"{col} {dtype}" for col, dtype in req.columns.items())
    sql = f"CREATE TABLE IF NOT EXISTS {req.table} ({col_defs})"
    result = database.execute(sql)
    if result["ok"]:
        _refresh_cache()
        asyncio.create_task(_notify_subscribers(_schema_cache))
    return result


@app.post("/schema/alter")
async def alter_table(req: AlterTableRequest):
    errors = []
    for col, dtype in req.add_columns.items():
        r = database.execute(f"ALTER TABLE {req.table} ADD COLUMN {col} {dtype}")
        if not r["ok"]:
            errors.append(r["error"])
    if errors:
        return {"ok": False, "errors": errors}
    _refresh_cache()
    asyncio.create_task(_notify_subscribers(_schema_cache))
    return {"ok": True}


@app.delete("/schema/{table}")
async def drop_table(table: str):
    result = database.execute(f"DROP TABLE IF EXISTS {table}")
    if result["ok"]:
        _refresh_cache()
        asyncio.create_task(_notify_subscribers(_schema_cache))
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["schema"])
