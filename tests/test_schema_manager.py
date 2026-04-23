"""Unit tests for services/schema_manager.py."""

import httpx
import pytest
import respx
from fastapi.testclient import TestClient


# ─────────────────────── HTTP endpoints ───────────────────────

class TestEndpoints:
    @pytest.fixture
    def client(self):
        from schema_manager import app, _schema_cache, _subscribers
        _schema_cache.clear()
        _subscribers.clear()
        return TestClient(app, raise_server_exceptions=False)

    def test_get_schema_empty(self, client):
        resp = client.get("/schema", params={"db": "testdb"})
        assert resp.status_code == 200
        assert resp.json() == {"ok": True, "schema": {}}

    def test_get_schema_default_db(self, client):
        assert client.get("/schema").json()["ok"] is True

    def test_subscribe(self, client):
        body = client.post("/subscribe", json={"webhook_url": "http://localhost:9999/hook"}).json()
        assert body == {"ok": True, "subscribers": 1}

    def test_subscribe_dedup(self, client):
        client.post("/subscribe", json={"webhook_url": "http://x/hook"})
        body = client.post("/subscribe", json={"webhook_url": "http://x/hook"}).json()
        assert body["subscribers"] == 1

    def test_subscribe_multiple(self, client):
        client.post("/subscribe", json={"webhook_url": "http://a"})
        body = client.post("/subscribe", json={"webhook_url": "http://b"}).json()
        assert body["subscribers"] == 2

    def test_schema_update_caches(self, client):
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        with respx.mock(assert_all_called=False):
            client.post("/schema-update", json={"db": "testdb", "schema": schema})
        assert client.get("/schema", params={"db": "testdb"}).json()["schema"] == schema

    def test_schema_update_default_db(self, client):
        import config
        with respx.mock(assert_all_called=False):
            client.post("/schema-update", json={"schema": {"x": []}})
        assert client.get("/schema", params={"db": config.DEFAULT_DB}).json()["schema"] == {"x": []}


# ─────────────────────── _notify_subscribers (async) ───────────────────────

class TestNotifySubscribers:
    @pytest.mark.asyncio
    async def test_all_subscribers_called(self):
        from schema_manager import _notify_subscribers, _subscribers
        _subscribers.clear()
        _subscribers.extend(["http://a/hook", "http://b/hook"])
        with respx.mock:
            a = respx.post("http://a/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            b = respx.post("http://b/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await _notify_subscribers("testdb", {"t": []})
        assert a.called and b.called

    @pytest.mark.asyncio
    async def test_unreachable_subscriber_does_not_block_others(self):
        from schema_manager import _notify_subscribers, _subscribers
        _subscribers.clear()
        _subscribers.extend(["http://down/hook", "http://up/hook"])
        with respx.mock:
            respx.post("http://down/hook").mock(
                side_effect=httpx.ConnectError("down")
            )
            up = respx.post("http://up/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await _notify_subscribers("db", {})
        assert up.called

    @pytest.mark.asyncio
    async def test_no_subscribers_is_noop(self):
        from schema_manager import _notify_subscribers, _subscribers
        _subscribers.clear()
        await _notify_subscribers("db", {})  # must not raise
