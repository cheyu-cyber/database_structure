"""
Shared fixtures for the per-service test suite.

Path layout matches `run.py`:
  - `database_structure/` is on sys.path so `import config` works.
  - `database_structure/services/` is on sys.path so `import database`,
    `import validator`, `import llm_service`, ... work.

No test in this suite makes a real network call: every cross-service HTTP
hop is intercepted with respx, and the Anthropic SDK is patched at the
`llm_service.get_client` boundary.
"""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICES = os.path.join(ROOT, "services")
for p in (ROOT, SERVICES):
    if p not in sys.path:
        sys.path.insert(0, p)


@pytest.fixture(autouse=True)
def tmp_db_dir(monkeypatch, tmp_path):
    """Redirect every database operation to a fresh temp directory."""
    import config
    monkeypatch.setattr(config, "DB_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def fake_anthropic_key(monkeypatch):
    """Pretend a key is configured without using a real one."""
    import config
    monkeypatch.setattr(config, "LLM_KEY", "sk-ant-test-key")
    return "sk-ant-test-key"
