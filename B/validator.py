"""
Validator Service — port 8003

Last line of defense. Every request — query or schema op —
must pass through POST /validate before touching the database.
Returns {"valid": true} or {"valid": false, "reason": "..."}.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from fastapi import FastAPI
from pydantic import BaseModel
import config

app = FastAPI(title="Validator")


class ValidateRequest(BaseModel):
    type: str        # "query" | "schema_op"
    action: str      # "SELECT" | "INSERT" | "CREATE_TABLE" | "ALTER" | "DROP"
    target: str      # table name
    payload: dict = {}
    schema: dict     # current schema snapshot from Schema Manager


@app.post("/validate")
async def validate(req: ValidateRequest):
    action = req.action
    target = req.target
    payload = req.payload
    schema = req.schema

    if action == "CREATE_TABLE":
        return _create(target, payload, schema)
    elif action == "SELECT":
        return _select(target, schema)
    elif action == "INSERT":
        return _insert(target, payload, schema)
    elif action == "ALTER":
        return _alter(target, payload, schema)
    elif action == "DROP":
        return _drop(target, schema)
    else:
        return {"valid": False, "reason": f"Unknown action: '{action}'"}


def _create(table, payload, schema):
    if table in schema:
        return {"valid": False, "reason": f"Table '{table}' already exists"}
    if not payload.get("columns"):
        return {"valid": False, "reason": "CREATE_TABLE requires at least one column"}
    return {"valid": True}


def _select(table, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    return {"valid": True}


def _insert(table, payload, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    known = {c["name"] for c in schema[table]}
    incoming = set(payload.get("values", {}).keys())
    unknown = incoming - known
    if unknown:
        return {"valid": False, "reason": f"Unknown columns: {unknown}"}
    return {"valid": True}


def _alter(table, payload, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    return {"valid": True}


def _drop(table, schema):
    if table not in schema:
        return {"valid": False, "reason": f"Table '{table}' does not exist"}
    return {"valid": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["validator"])
