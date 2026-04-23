"""Unit tests for services/query_service.py."""

import httpx
import pytest
import respx
from fastapi.testclient import TestClient


# ─────────────────────── _parse ───────────────────────

class TestParse:
    def _parse(self, text):
        from query_service import _parse
        return _parse(text)

    def test_select(self):
        assert self._parse("select users") == {
            "type": "query", "action": "SELECT", "target": "users", "payload": {},
        }

    def test_select_case_insensitive(self):
        assert self._parse("SELECT users")["action"] == "SELECT"

    def test_insert_multiple_values(self):
        result = self._parse("insert users name=Alice age=30")
        assert result["action"] == "INSERT"
        assert result["target"] == "users"
        assert result["payload"]["values"] == {"name": "Alice", "age": "30"}

    def test_insert_value_containing_equals(self):
        assert self._parse("insert t expr=a=b")["payload"]["values"] == {"expr": "a=b"}

    def test_insert_too_short(self):
        assert self._parse("insert users") is None

    def test_create_table(self):
        result = self._parse("create table products id:INTEGER name:TEXT")
        assert result["action"] == "CREATE_TABLE"
        assert result["target"] == "products"
        assert result["payload"]["columns"] == {"id": "INTEGER", "name": "TEXT"}

    def test_create_missing_table_keyword(self):
        assert self._parse("create products id:INTEGER") is None

    def test_create_too_short(self):
        assert self._parse("create table") is None

    def test_alter_add_multiple(self):
        result = self._parse("alter users add email:TEXT phone:TEXT")
        assert result["action"] == "ALTER"
        assert result["target"] == "users"
        assert result["payload"]["add_columns"] == {"email": "TEXT", "phone": "TEXT"}

    def test_alter_single_column_too_short(self):
        # "alter users add email:TEXT" → 4 tokens but parser needs >=5
        assert self._parse("alter users add email:TEXT") is None

    def test_drop(self):
        assert self._parse("drop users") == {
            "type": "schema_op", "action": "DROP", "target": "users", "payload": {},
        }

    def test_drop_alone(self):
        assert self._parse("drop") is None

    def test_empty(self):
        assert self._parse("") is None

    def test_whitespace(self):
        assert self._parse("   ") is None

    def test_unrecognised(self):
        assert self._parse("show me everything") is None


# ─────────────────────── /schema-update webhook ───────────────────────

class TestSchemaUpdate:
    @pytest.fixture
    def client(self):
        from query_service import app, _schema_cache
        _schema_cache.clear()
        return TestClient(app, raise_server_exceptions=False)

    def test_caches_schema(self, client):
        from query_service import _schema_cache
        schema = {"t": [{"name": "id", "type": "INTEGER"}]}
        resp = client.post("/schema-update", json={"db": "testdb", "schema": schema})
        assert resp.json()["ok"] is True
        assert _schema_cache["testdb"] == schema

    def test_default_db(self, client):
        from query_service import _schema_cache
        import config
        client.post("/schema-update", json={"schema": {"a": []}})
        assert _schema_cache[config.DEFAULT_DB] == {"a": []}


# ─────────────────────── /query handler ───────────────────────

class TestHandleQuery:
    @pytest.fixture
    def client(self):
        from query_service import app, _schema_cache
        _schema_cache.clear()
        return TestClient(app, raise_server_exceptions=False)

    def test_parsed_command_forwarded_to_validator(self, client):
        import config
        with respx.mock:
            route = respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json={"ok": True, "rows": []})
            )
            resp = client.post("/query", json={"input": "select users", "db": "testdb"})
        assert route.called
        assert resp.json() == {"ok": True, "rows": []}

    def test_unparseable_input_falls_back_to_llm(self, client):
        import config
        suggestion = {"type": "query", "action": "SELECT", "target": "users", "payload": {}}
        with respx.mock:
            llm = respx.post(f"{config.URLS['llm']}/suggest").mock(
                return_value=httpx.Response(200, json={"ok": True, "suggestion": suggestion})
            )
            val = respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json={"ok": True, "rows": [{"id": 1}]})
            )
            resp = client.post("/query", json={"input": "show me everything", "db": "testdb"})
        assert llm.called and val.called
        assert resp.json() == {"ok": True, "rows": [{"id": 1}]}

    def test_llm_fails_returns_clean_error(self, client):
        import config
        with respx.mock:
            respx.post(f"{config.URLS['llm']}/suggest").mock(
                return_value=httpx.Response(200, json={"ok": False, "error": "x"})
            )
            resp = client.post("/query", json={"input": "asdf", "db": "testdb"})
        body = resp.json()
        assert body["ok"] is False
        assert "Could not understand" in body["reason"]

    def test_envelope_carries_db_and_schema(self, client):
        """Ensure the validator receives the db name and the cached schema."""
        import config
        from query_service import _schema_cache

        cached = {"users": [{"name": "id", "type": "INTEGER"}]}
        _schema_cache["testdb"] = cached
        captured = {}

        def capture(request):
            import json
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"ok": True, "rows": []})

        with respx.mock:
            respx.post(f"{config.URLS['validator']}/execute").mock(side_effect=capture)
            client.post("/query", json={"input": "select users", "db": "testdb"})

        assert captured["db"] == "testdb"
        assert captured["schema"] == cached
