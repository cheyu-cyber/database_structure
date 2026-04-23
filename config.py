"""
Project-wide config.

No secrets live in this file. The Anthropic API key is loaded from one of:
  1. ANTHROPIC_API_KEY environment variable (preferred for Docker / .env)
  2. Anthropic_API_KEY.json next to this file (preferred for local dev)

Service hostnames are env-overridable so the same code runs locally
(localhost) and in docker-compose (service names as hostnames).
"""

import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))

# --- Storage ---
DB_DIR = os.getenv("DB_DIR", os.path.join(_HERE, "databases"))
DEFAULT_DB = os.getenv("DEFAULT_DB", "data")


# --- LLM key + model loading ---
def _load_llm_settings() -> dict:
    """Load LLM settings from env first, then JSON, never from .py."""
    settings = {
        "key": os.getenv("ANTHROPIC_API_KEY", ""),
        "model": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        "max_tokens": int(os.getenv("ANTHROPIC_MAX_TOKENS", "512")),
    }

    json_path = os.getenv(
        "ANTHROPIC_KEY_FILE",
        os.path.join(_HERE, "Anthropic_API_KEY.json"),
    )
    if os.path.isfile(json_path):
        try:
            with open(json_path) as f:
                data = json.load(f)
            if not settings["key"]:
                settings["key"] = data.get("key", "")
            if "model" in data and not os.getenv("ANTHROPIC_MODEL"):
                settings["model"] = data["model"]
            if "max_tokens" in data and not os.getenv("ANTHROPIC_MAX_TOKENS"):
                settings["max_tokens"] = int(data["max_tokens"])
        except (OSError, ValueError, json.JSONDecodeError):
            pass

    return settings


_LLM = _load_llm_settings()
LLM_KEY = _LLM["key"]
LLM_MODEL = _LLM["model"]
LLM_MAX_TOKENS = _LLM["max_tokens"]


# --- Ports + URLs ---
PORTS = {
    "query":     int(os.getenv("QUERY_PORT", "8000")),
    "schema":    int(os.getenv("SCHEMA_PORT", "8001")),
    "llm":       int(os.getenv("LLM_PORT", "8002")),
    "validator": int(os.getenv("VALIDATOR_PORT", "8003")),
}

HOSTS = {
    "query":     os.getenv("QUERY_HOST", "localhost"),
    "schema":    os.getenv("SCHEMA_HOST", "localhost"),
    "llm":       os.getenv("LLM_HOST", "localhost"),
    "validator": os.getenv("VALIDATOR_HOST", "localhost"),
}

URLS = {k: f"http://{HOSTS[k]}:{PORTS[k]}" for k in PORTS}
