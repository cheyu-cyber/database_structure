"""
Query Service — port 8000

The single REST entry point. POST /query is the only door in.

On startup:
  - registers with Schema Manager to receive schema updates (pub/sub)

On each request:
  1. Check local schema cache
  2. Try to parse the input directly
  3. If unclear → ask LLM Service (async, non-blocking)
  4. Send to Validator — must pass before anything else happens
  5. Execute against the database (via Schema Manager for schema ops,
     or directly via SQLite for data queries)
  6. Return result or reason to caller
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
import config

app = FastAPI(title="Query Service")

# --- Local schema cache, kept fresh via pub/sub ---
_schema_cache: dict[str, dict] = {}   # {db_name: {table: [cols]}}


# --- Models ---
class QueryRequest(BaseModel):
    input: str          # raw natural language or structured command
    db: str = config.DEFAULT_DB


# --- Pub/Sub receiver ---
@app.post("/schema-update")
async def schema_update(payload: dict):
    """
    Webhook endpoint. Schema Manager calls this whenever schema changes.
    Payload: {"db": "...", "schema": {...}}
    """
    db = payload.get("db", config.DEFAULT_DB)
    _schema_cache[db] = payload.get("schema", {})
    return {"ok": True}


# --- Startup: fetch schema + register as subscriber ---
@app.on_event("startup")
async def startup():
    async with httpx.AsyncClient() as client:
        # 1. Get current schema for default db
        try:
            r = await client.get(
                f"{config.URLS['schema']}/schema",
                params={"db": config.DEFAULT_DB}
            )
            _schema_cache[config.DEFAULT_DB] = r.json().get("schema", {})
        except Exception:
            pass

        # 2. Register our webhook so we get notified on changes
        try:
            await client.post(
                f"{config.URLS['schema']}/subscribe",
                json={"webhook_url": f"{config.URLS['query']}/schema-update"}
            )
        except Exception:
            pass


# --- Main entry point ---
@app.post("/query")
async def handle_query(req: QueryRequest):
    """
    Single entry point. All user input comes through here.
    """
    db = req.db
    schema = _schema_cache.get(db, {})

    request_envelope = _parse(req.input)

    # If we couldn't parse it, ask the LLM (async, non-blocking)
    if request_envelope is None:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{config.URLS['llm']}/suggest",
                json={"user_input": req.input, "schema": schema},
                timeout=30.0
            )
            result = r.json()
            if not result["ok"]:
                return {"ok": False, "reason": "Could not understand input"}
            request_envelope = result["suggestion"]

    # Attach current schema snapshot and db name for Validator
    request_envelope["schema"] = schema
    request_envelope["db"] = db

    # Send to Validator — validates and executes in one step
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{config.URLS['validator']}/execute",
            json=request_envelope,
            timeout=30.0
        )
        return r.json()


# --- Parser: structured CLI commands ---
def _parse(user_input: str) -> dict | None:
    """
    Parse structured commands. Returns envelope or None (falls back to LLM).

    Supported syntax:
      select <table>
      insert <table> <col>=<val> <col>=<val> ...
      create table <table> <col>:<TYPE> ...
      alter <table> add <col>:<TYPE> ...
      drop <table>
    """
    parts = user_input.strip().split()
    if not parts:
        return None

    cmd = parts[0].lower()

    if cmd == "select" and len(parts) >= 2:
        return {"type": "query", "action": "SELECT", "target": parts[1], "payload": {}}

    if cmd == "insert" and len(parts) >= 3:
        values = {}
        for token in parts[2:]:
            if "=" in token:
                k, v = token.split("=", 1)
                values[k] = v
        return {"type": "query", "action": "INSERT", "target": parts[1], "payload": {"values": values}}

    if cmd == "create" and len(parts) >= 4 and parts[1].lower() == "table":
        columns = {}
        for token in parts[3:]:
            if ":" in token:
                col, dtype = token.split(":", 1)
                columns[col] = dtype
        return {"type": "schema_op", "action": "CREATE_TABLE", "target": parts[2], "payload": {"columns": columns}}

    if cmd == "alter" and len(parts) >= 5 and parts[2].lower() == "add":
        add_columns = {}
        for token in parts[3:]:
            if ":" in token:
                col, dtype = token.split(":", 1)
                add_columns[col] = dtype
        return {"type": "schema_op", "action": "ALTER", "target": parts[1], "payload": {"add_columns": add_columns}}

    if cmd == "drop" and len(parts) >= 2:
        return {"type": "schema_op", "action": "DROP", "target": parts[1], "payload": {}}

    return None  # fall through to LLM


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["query"])
