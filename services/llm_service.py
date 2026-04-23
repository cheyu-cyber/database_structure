"""
LLM Service - port 8002

Async: the LLM call does not block the CLI.
Called only when Query Service cannot parse user input directly.
Returns a suggested request envelope.

The Anthropic client is built lazily so the service still boots
when no key is configured (e.g. in CI).  /suggest then returns
a clean error instead of crashing at import time.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import json
from fastapi import FastAPI
from pydantic import BaseModel
from anthropic import Anthropic
import config

app = FastAPI(title="LLM Service")

_client: Anthropic | None = None


def get_client() -> Anthropic:
    """Build (and cache) the Anthropic client. Tests patch this."""
    global _client
    if _client is None:
        if not config.LLM_KEY:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set. Provide it via env or "
                "Anthropic_API_KEY.json next to config.py."
            )
        _client = Anthropic(api_key=config.LLM_KEY)
    return _client


class SuggestRequest(BaseModel):
    user_input: str
    schema: dict


@app.get("/health")
async def health():
    return {"ok": True, "service": "llm", "key_configured": bool(config.LLM_KEY)}


@app.post("/suggest")
async def suggest(req: SuggestRequest):
    """
    Sends user input + schema to Claude via the Anthropic SDK,
    returns a suggested request envelope.
    """
    schema_str = json.dumps(req.schema, indent=2)

    prompt = f"""You are a SQL assistant. Given the schema and user input, return ONLY a JSON request envelope.

Schema:
{schema_str}

User input: {req.user_input}

Return only valid JSON with this exact shape:
{{
  "type": "query" | "schema_op",
  "action": "SELECT" | "INSERT" | "CREATE_TABLE" | "ALTER" | "DROP",
  "target": "table_name",
  "payload": {{}}
}}"""

    try:
        client = get_client()
        resp = client.messages.create(
            model=config.LLM_MODEL,
            max_tokens=config.LLM_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text
        suggestion = json.loads(text.strip())
        return {"ok": True, "suggestion": suggestion}
    except Exception as e:
        return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["llm"])
