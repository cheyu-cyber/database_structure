DB_PATH = "data.db"

LLM_MODEL = "claude-sonnet-4-20250514"
LLM_MAX_TOKENS = 512

PORTS = {
    "query":     8000,
    "schema":    8001,
    "llm":       8002,
    "validator": 8003,
}

URLS = {k: f"http://localhost:{v}" for k, v in PORTS.items()}
