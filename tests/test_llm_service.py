"""Unit tests for services/llm_service.py.

The Anthropic SDK import is preserved (`from anthropic import Anthropic`),
but no test ever hits the real API: `get_client` is patched to return a
MagicMock whose `.messages.create` returns a fake response.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


def _fake_anthropic_response(text: str):
    """Mimic the shape of `client.messages.create(...)`."""
    block = MagicMock()
    block.text = text
    resp = MagicMock()
    resp.content = [block]
    return resp


@pytest.fixture
def client(fake_anthropic_key):
    import llm_service
    llm_service._client = None  # force re-build
    return TestClient(llm_service.app, raise_server_exceptions=False), llm_service


# ─────────────────────── /health ───────────────────────

class TestHealth:
    def test_reports_key_configured(self, client):
        test_client, _ = client
        body = test_client.get("/health").json()
        assert body == {"ok": True, "service": "llm", "key_configured": True}

    def test_reports_key_missing(self, monkeypatch):
        import config
        monkeypatch.setattr(config, "LLM_KEY", "")
        import llm_service
        llm_service._client = None
        body = TestClient(llm_service.app).get("/health").json()
        assert body["key_configured"] is False


# ─────────────────────── get_client ───────────────────────

class TestGetClient:
    def test_raises_when_no_key(self, monkeypatch):
        import config
        import llm_service
        monkeypatch.setattr(config, "LLM_KEY", "")
        llm_service._client = None
        with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY is not set"):
            llm_service.get_client()

    def test_caches_client(self, fake_anthropic_key):
        import llm_service
        llm_service._client = None
        with patch("llm_service.Anthropic") as ctor:
            ctor.return_value = MagicMock()
            a = llm_service.get_client()
            b = llm_service.get_client()
        assert a is b
        ctor.assert_called_once_with(api_key="sk-ant-test-key")


# ─────────────────────── /suggest ───────────────────────

class TestSuggest:
    def test_success_returns_parsed_envelope(self, client):
        test_client, llm_service = client
        envelope = {"type": "query", "action": "SELECT", "target": "users", "payload": {}}
        fake = MagicMock()
        fake.messages.create.return_value = _fake_anthropic_response(json.dumps(envelope))
        with patch.object(llm_service, "get_client", return_value=fake):
            resp = test_client.post("/suggest", json={
                "user_input": "show all users",
                "schema": {"users": [{"name": "id", "type": "INTEGER"}]},
            })
        body = resp.json()
        assert body == {"ok": True, "suggestion": envelope}

    def test_anthropic_raises_returns_clean_error(self, client):
        test_client, llm_service = client
        fake = MagicMock()
        fake.messages.create.side_effect = Exception("API down")
        with patch.object(llm_service, "get_client", return_value=fake):
            resp = test_client.post("/suggest", json={
                "user_input": "x", "schema": {},
            })
        body = resp.json()
        assert body["ok"] is False
        assert "API down" in body["error"]

    def test_invalid_json_response_returns_error(self, client):
        test_client, llm_service = client
        fake = MagicMock()
        fake.messages.create.return_value = _fake_anthropic_response("not json")
        with patch.object(llm_service, "get_client", return_value=fake):
            resp = test_client.post("/suggest", json={
                "user_input": "x", "schema": {},
            })
        assert resp.json()["ok"] is False

    def test_missing_key_returns_error(self, monkeypatch):
        """End-to-end: with no key the endpoint surfaces a helpful message."""
        import config
        monkeypatch.setattr(config, "LLM_KEY", "")
        import llm_service
        llm_service._client = None
        body = TestClient(llm_service.app).post("/suggest", json={
            "user_input": "x", "schema": {},
        }).json()
        assert body["ok"] is False
        assert "ANTHROPIC_API_KEY is not set" in body["error"]

    def test_prompt_includes_schema_and_user_input(self, client):
        """The prompt sent to Claude should contain the schema and user input verbatim."""
        test_client, llm_service = client
        envelope = {"type": "query", "action": "SELECT", "target": "t", "payload": {}}
        fake = MagicMock()
        fake.messages.create.return_value = _fake_anthropic_response(json.dumps(envelope))
        with patch.object(llm_service, "get_client", return_value=fake):
            test_client.post("/suggest", json={
                "user_input": "MAGIC_INPUT_TOKEN",
                "schema": {"MAGIC_TABLE_TOKEN": []},
            })
        sent_prompt = fake.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "MAGIC_INPUT_TOKEN" in sent_prompt
        assert "MAGIC_TABLE_TOKEN" in sent_prompt
