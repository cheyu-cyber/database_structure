"""
LLM Service — port 8002

Async: the LLM call does not block the CLI.
Called only when Query Service cannot parse user input directly.
Returns a suggested request envelope.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import json
import httpx
from fastapi import FastAPI
from pydantic import BaseModel
import config

app = FastAPI(title="LLM Service")


class SuggestRequest(BaseModel):
    user_input: str
    schema: dict


@app.post("/suggest")
async def suggest(req: SuggestRequest):
    """
    Async: sends user input + schema to Claude, returns a request envelope.
    Non-blocking — Query Service awaits this without freezing the event loop.
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

    body = {
        "model": config.LLM_MODEL,
        "max_tokens": config.LLM_MAX_TOKENS,
        "messages": [{"role": "user", "content": prompt}]
    }

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                json=body,
                headers={
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                },
                timeout=30.0
            )
            data = resp.json()
            text = data["content"][0]["text"]
            suggestion = json.loads(text.strip())
            return {"ok": True, "suggestion": suggestion}
        except Exception as e:
            return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORTS["llm"])
