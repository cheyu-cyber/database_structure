"""
run.py — starts all services as separate processes, then launches the CLI.

Start order matters:
  1. Schema Manager  (others depend on it at startup)
  2. Validator
  3. LLM Service
  4. Query Service   (subscribes to Schema Manager on startup)
  5. CLI             (talks to Query Service)
"""

import sys
import os
import time
import multiprocessing
import uvicorn

# Add root to path for all child processes
ROOT = os.path.dirname(__file__)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "services"))


def run_service(module_path: str, app_import: str, port: int):
    """Run a FastAPI app in a child process."""
    sys.path.insert(0, ROOT)
    sys.path.insert(0, os.path.join(ROOT, "services"))
    import importlib.util
    spec = importlib.util.spec_from_file_location("svc", module_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    app = getattr(mod, "app")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")


def start_service(name: str, filename: str, port: int) -> multiprocessing.Process:
    path = os.path.join(ROOT, "services", filename)
    p = multiprocessing.Process(
        target=run_service,
        args=(path, "app", port),
        name=name,
        daemon=True
    )
    p.start()
    return p


def wait_for_service(url: str, name: str, retries: int = 20):
    import httpx
    for i in range(retries):
        try:
            httpx.get(url, timeout=1.0)
            print(f"  [{name}] ready")
            return True
        except Exception:
            time.sleep(0.5)
    print(f"  [{name}] did not start in time")
    return False


if __name__ == "__main__":
    import config

    print("Starting services...")

    processes = []
    processes.append(start_service("schema_manager", "schema_manager.py", config.PORTS["schema"]))
    time.sleep(0.5)
    processes.append(start_service("validator",      "validator.py",      config.PORTS["validator"]))
    processes.append(start_service("llm_service",    "llm_service.py",    config.PORTS["llm"]))
    time.sleep(0.5)
    processes.append(start_service("query_service",  "query_service.py",  config.PORTS["query"]))

    # Wait for all services to be ready
    wait_for_service(f"{config.URLS['schema']}/schema",    "Schema Manager")
    wait_for_service(f"{config.URLS['validator']}/docs",   "Validator")
    wait_for_service(f"{config.URLS['llm']}/docs",         "LLM Service")
    wait_for_service(f"{config.URLS['query']}/docs",       "Query Service")

    print("\nAll services up. Starting CLI...\n")

    # CLI runs in the main process
    import cli
    cli.run()

    # On CLI exit, terminate all services
    for p in processes:
        p.terminate()
