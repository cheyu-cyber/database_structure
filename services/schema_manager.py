"""
Schema Manager Service — port 8001

Pure cache + pub/sub. Does NOT access the database directly.
Validator is the sole database gateway — it pushes schema updates here
after any schema-changing operation.

Services register a webhook URL. On any schema change,
Schema Manager POSTs the updated schema to all subscribers.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import httpx
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
import config

app = FastAPI(title="Schema Manager")

# --- State ---
_subscribers: list[str] = []          # registered webhook URLs
_schema_cache: dict[str, dict] = {}   # {db_name: {table: [cols]}}


# --- Models ---
class SubscribeRequest(BaseModel):
    webhook_url: str


# --- Pub/Sub ---
async def _notify_subscribers(db: str, schema: dict):
    """Fire-and-forget: POST updated schema to all registered subscribers."""
    async with httpx.AsyncClient() as client:
        for url in _subscribers:
            try:
                await client.post(url, json={"db": db, "schema": schema}, timeout=3.0)
            except Exception:
                pass  # subscriber unreachable, skip


# --- Endpoints ---
@app.on_event("startup")
async def startup():
    """Fetch initial schema for default db from Validator (the sole DB gateway)."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{config.URLS['validator']}/schema",
                params={"db": config.DEFAULT_DB},
                timeout=5.0
            )
            _schema_cache[config.DEFAULT_DB] = r.json().get("schema", {})
    except Exception:
        pass  # Validator not ready yet; will be populated on first schema update


@app.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    """Register a webhook URL to receive schema change notifications."""
    if req.webhook_url not in _subscribers:
        _subscribers.append(req.webhook_url)
    return {"ok": True, "subscribers": len(_subscribers)}


@app.get("/schema")
async def get_schema(db: str = config.DEFAULT_DB):
    """Return the cached schema for a specific database."""
    return {"ok": True, "schema": _schema_cache.get(db, {})}


@app.post("/schema-update")
async def schema_update(payload: dict):
    """
    Webhook endpoint. Validator calls this after any schema-changing op.
    Payload: {"db": "...", "schema": {...}}
    Updates local cache and fans out to all subscribers.
    """
    db = payload.get("db", config.DEFAULT_DB)
    schema = payload.get("schema", {})
    _schema_cache[db] = schema
    asyncio.create_task(_notify_subscribers(db, schema))
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["schema"])
