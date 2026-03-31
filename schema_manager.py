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
_subscribers: list[str] = []   # registered webhook URLs
_schema_cache: dict = {}        # in-memory floor plan


# --- Models ---
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


# --- Endpoints ---
@app.on_event("startup")
async def startup():
    """Fetch initial schema from Validator (the sole DB gateway)."""
    global _schema_cache
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{config.URLS['validator']}/schema", timeout=5.0)
            _schema_cache = r.json().get("schema", {})
    except Exception:
        pass  # Validator not ready yet; will be populated on first schema update


@app.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    """Register a webhook URL to receive schema change notifications."""
    if req.webhook_url not in _subscribers:
        _subscribers.append(req.webhook_url)
    return {"ok": True, "subscribers": len(_subscribers)}


@app.get("/schema")
async def get_schema():
    """Return the current cached schema."""
    return {"ok": True, "schema": _schema_cache}


@app.post("/schema-update")
async def schema_update(updated_schema: dict):
    """
    Webhook endpoint. Validator calls this after any schema-changing op.
    Updates local cache and fans out to all subscribers.
    """
    global _schema_cache
    _schema_cache = updated_schema
    asyncio.create_task(_notify_subscribers(_schema_cache))
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["schema"])
