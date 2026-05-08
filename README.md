# database_structure

A small SQL system with one rule: **every query — typed or natural-language — passes through the same validated door.**

Four microservices, one CLI, ~600 lines of Python, designed around separation of concerns and defense-in-depth.

## 🎬 Video walkthrough

https://github.com/cheyu-cyber/database_structure/raw/main/video/database_structure_explainer_final.mp4

> If the player above doesn't load, [download / open the mp4 directly](video/database_structure_explainer_final.mp4).

## Architecture

| Service        | Port  | Role                                                  |
| -------------- | ----- | ----------------------------------------------------- |
| CLI            | —     | The only human-facing surface                         |
| Query Service  | 8000  | Single REST entry point; parses, routes, orchestrates |
| Schema Manager | 8001  | Pure cache + pub/sub broker (does **not** touch DB)   |
| LLM Service    | 8002  | Async fallback when input can't be parsed             |
| Validator      | 8003  | **Sole DB gateway** — validates, then executes        |

Validator is the only process in the system that imports the SQLite driver. Every read, every write, every schema change passes through one validated chokepoint.

## Request lifecycle

```
CLI ──POST /query──▶ Query Service
                        │
                        ├─ parse OK ───────────────────────────┐
                        │                                       │
                        └─ parse fails ──▶ LLM Service ─────────┤
                                                                ▼
                                            envelope ──▶ Validator /execute
                                                                │
                                                                ▼
                                                            SQLite
                                                                │
                                                  schema changed?
                                                                │
                                                                ▼
                                            Schema Manager ──webhook fan-out──▶ subscribers
```

## Validation layers (outside → in)

1. **Envelope schema** — Pydantic rejects malformed shape (422)
2. **Action whitelist** — 5 allowed actions; no raw SQL accepted
3. **Schema-aware checks** — table exists · columns known · no collisions
4. **Parameterized SQL** — `?`-placeholders for values, no string concatenation
5. **Sole DB gateway** — only `validator.py` imports `database.py`

## Run it

```bash
pip install -r requirements.txt
python run.py            # local dev (multiprocess)
# or
docker compose up        # production
```

## Tests

```bash
pytest
```

Each service has its own test file under `tests/`.
