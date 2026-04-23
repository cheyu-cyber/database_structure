"""Unit tests for services/validator.py."""

import httpx
import pytest
import respx
from fastapi.testclient import TestClient


# ─────────────────────── _validate_create ───────────────────────

class TestValidateCreate:
    def test_valid(self):
        from validator import _validate_create
        assert _validate_create("new_t", {"columns": {"id": "INTEGER"}}, {})["valid"] is True

    def test_table_already_exists(self):
        from validator import _validate_create
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        result = _validate_create("users", {"columns": {"id": "INTEGER"}}, schema)
        assert result["valid"] is False
        assert "already exists" in result["reason"]

    def test_no_columns_key(self):
        from validator import _validate_create
        assert _validate_create("t", {}, {})["valid"] is False

    def test_empty_columns(self):
        from validator import _validate_create
        assert _validate_create("t", {"columns": {}}, {})["valid"] is False


# ─────────────────────── _validate_select ───────────────────────

class TestValidateSelect:
    def test_valid(self):
        from validator import _validate_select
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_select("users", schema)["valid"] is True

    def test_unknown_table(self):
        from validator import _validate_select
        result = _validate_select("nope", {})
        assert result["valid"] is False
        assert "does not exist" in result["reason"]


# ─────────────────────── _validate_insert ───────────────────────

class TestValidateInsert:
    def test_valid(self):
        from validator import _validate_insert
        schema = {"users": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
        ]}
        result = _validate_insert("users", {"values": {"id": "1", "name": "A"}}, schema)
        assert result["valid"] is True

    def test_unknown_table(self):
        from validator import _validate_insert
        assert _validate_insert("nope", {"values": {"a": "1"}}, {})["valid"] is False

    def test_unknown_columns(self):
        from validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        result = _validate_insert("users", {"values": {"id": "1", "bad": "x"}}, schema)
        assert result["valid"] is False
        assert "Unknown columns" in result["reason"]

    def test_empty_values_is_valid(self):
        from validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_insert("users", {"values": {}}, schema)["valid"] is True

    def test_missing_values_key(self):
        from validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_insert("users", {}, schema)["valid"] is True


# ─────────────────────── _validate_alter / _validate_drop ───────────────────────

class TestValidateAlter:
    def test_valid(self):
        from validator import _validate_alter
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_alter("users", schema)["valid"] is True

    def test_unknown_table(self):
        from validator import _validate_alter
        assert _validate_alter("nope", {})["valid"] is False


class TestValidateDrop:
    def test_valid(self):
        from validator import _validate_drop
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_drop("users", schema)["valid"] is True

    def test_unknown_table(self):
        from validator import _validate_drop
        assert _validate_drop("nope", {})["valid"] is False


# ─────────────────────── _validate dispatcher ───────────────────────

class TestValidateDispatcher:
    def _req(self, **overrides):
        from validator import ExecuteRequest
        defaults = dict(
            type="query", action="SELECT", target="users",
            payload={}, schema={}, db="testdb",
        )
        defaults.update(overrides)
        return ExecuteRequest(**defaults)

    def test_dispatches_create(self):
        from validator import _validate
        req = self._req(action="CREATE_TABLE", target="t",
                        payload={"columns": {"id": "INTEGER"}}, schema={})
        assert _validate(req)["valid"] is True

    def test_dispatches_select(self):
        from validator import _validate
        req = self._req(action="SELECT", target="t",
                        schema={"t": [{"name": "id", "type": "INTEGER"}]})
        assert _validate(req)["valid"] is True

    def test_dispatches_insert(self):
        from validator import _validate
        req = self._req(action="INSERT", target="t",
                        payload={"values": {"id": "1"}},
                        schema={"t": [{"name": "id", "type": "INTEGER"}]})
        assert _validate(req)["valid"] is True

    def test_dispatches_alter(self):
        from validator import _validate
        req = self._req(action="ALTER", target="t",
                        schema={"t": [{"name": "id", "type": "INTEGER"}]})
        assert _validate(req)["valid"] is True

    def test_dispatches_drop(self):
        from validator import _validate
        req = self._req(action="DROP", target="t",
                        schema={"t": [{"name": "id", "type": "INTEGER"}]})
        assert _validate(req)["valid"] is True

    def test_unknown_action(self):
        from validator import _validate
        result = _validate(self._req(action="TRUNCATE", schema={}))
        assert result["valid"] is False
        assert "Unknown action" in result["reason"]


# ─────────────────────── _run executor ───────────────────────

class TestRun:
    def _req(self, **overrides):
        from validator import ExecuteRequest
        defaults = dict(
            type="query", action="SELECT", target="t",
            payload={}, schema={}, db="testdb",
        )
        defaults.update(overrides)
        return ExecuteRequest(**defaults)

    def _create(self, name="items"):
        from validator import _run
        _run(self._req(
            type="schema_op", action="CREATE_TABLE", target=name,
            payload={"columns": {"id": "INTEGER", "name": "TEXT"}},
        ))

    def test_create(self):
        from validator import _run
        req = self._req(type="schema_op", action="CREATE_TABLE", target="items",
                        payload={"columns": {"id": "INTEGER", "name": "TEXT"}})
        assert _run(req)["ok"] is True

    def test_insert_then_select(self):
        from validator import _run
        self._create()
        _run(self._req(action="INSERT", target="items",
                       payload={"values": {"id": "1", "name": "Widget"}}))
        result = _run(self._req(action="SELECT", target="items"))
        assert result["rows"] == [{"id": 1, "name": "Widget"}]

    def test_alter_add_column(self):
        from validator import _run
        self._create()
        req = self._req(type="schema_op", action="ALTER", target="items",
                        payload={"add_columns": {"price": "REAL"}})
        assert _run(req)["ok"] is True

    def test_alter_duplicate_column(self):
        from validator import _run
        self._create()
        req = self._req(type="schema_op", action="ALTER", target="items",
                        payload={"add_columns": {"id": "INTEGER"}})
        assert _run(req)["ok"] is False

    def test_drop(self):
        from validator import _run
        self._create()
        assert _run(self._req(type="schema_op", action="DROP", target="items"))["ok"] is True

    def test_unknown_action(self):
        from validator import _run
        result = _run(self._req(action="UNKNOWN"))
        assert result["ok"] is False
        assert "No executor" in result["error"]


# ─────────────────────── _read_schema ───────────────────────

class TestReadSchema:
    def test_empty_db(self):
        from validator import _read_schema
        assert _read_schema("testdb") == {}

    def test_one_table(self):
        import database
        from validator import _read_schema
        database.execute("CREATE TABLE products (id INTEGER, name TEXT)", db="testdb")
        schema = _read_schema("testdb")
        assert "products" in schema
        names = [c["name"] for c in schema["products"]]
        assert names == ["id", "name"]

    def test_multiple_tables(self):
        import database
        from validator import _read_schema
        database.execute("CREATE TABLE a (x INTEGER)", db="testdb")
        database.execute("CREATE TABLE b (y TEXT)", db="testdb")
        schema = _read_schema("testdb")
        assert {"a", "b"}.issubset(schema)

    def test_column_types_preserved(self):
        import database
        from validator import _read_schema
        database.execute(
            "CREATE TABLE typed (age INTEGER, score REAL, name TEXT)", db="testdb"
        )
        types = {c["name"]: c["type"] for c in _read_schema("testdb")["typed"]}
        assert types == {"age": "INTEGER", "score": "REAL", "name": "TEXT"}


# ─────────────────────── _push_schema_update (async) ───────────────────────

class TestPushSchemaUpdate:
    @pytest.mark.asyncio
    async def test_success(self):
        import config
        from validator import _push_schema_update
        with respx.mock:
            route = respx.post(f"{config.URLS['schema']}/schema-update").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await _push_schema_update("testdb", {"t": []})
        assert route.called

    @pytest.mark.asyncio
    async def test_subscriber_unreachable_is_silent(self):
        import config
        from validator import _push_schema_update
        with respx.mock:
            respx.post(f"{config.URLS['schema']}/schema-update").mock(
                side_effect=httpx.ConnectError("down")
            )
            await _push_schema_update("testdb", {})  # must not raise


# ─────────────────────── /execute and /schema endpoints ───────────────────────

class TestExecuteEndpoint:
    @pytest.fixture
    def client(self):
        from validator import app
        return TestClient(app, raise_server_exceptions=False)

    def test_create_table(self, client):
        with respx.mock(assert_all_called=False):
            import config
            respx.post(f"{config.URLS['schema']}/schema-update").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            resp = client.post("/execute", json={
                "type": "schema_op", "action": "CREATE_TABLE",
                "target": "test_table",
                "payload": {"columns": {"id": "INTEGER", "name": "TEXT"}},
                "schema": {}, "db": "testdb",
            })
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_select_after_create(self, client):
        import config
        with respx.mock(assert_all_called=False):
            respx.post(f"{config.URLS['schema']}/schema-update").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            client.post("/execute", json={
                "type": "schema_op", "action": "CREATE_TABLE",
                "target": "t", "payload": {"columns": {"id": "INTEGER"}},
                "schema": {}, "db": "testdb",
            })
        resp = client.post("/execute", json={
            "type": "query", "action": "SELECT", "target": "t",
            "payload": {}, "schema": {"t": [{"name": "id", "type": "INTEGER"}]},
            "db": "testdb",
        })
        assert resp.json() == {"ok": True, "rows": []}

    def test_validation_failure(self, client):
        resp = client.post("/execute", json={
            "type": "query", "action": "SELECT", "target": "nonexistent",
            "payload": {}, "schema": {}, "db": "testdb",
        })
        body = resp.json()
        assert body["ok"] is False
        assert "does not exist" in body["reason"]


class TestGetSchemaEndpoint:
    @pytest.fixture
    def client(self):
        from validator import app
        return TestClient(app, raise_server_exceptions=False)

    def test_empty(self, client):
        resp = client.get("/schema", params={"db": "testdb"})
        assert resp.status_code == 200
        assert resp.json() == {"ok": True, "schema": {}}

    def test_after_create(self, client):
        import config
        with respx.mock(assert_all_called=False):
            respx.post(f"{config.URLS['schema']}/schema-update").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            client.post("/execute", json={
                "type": "schema_op", "action": "CREATE_TABLE",
                "target": "things", "payload": {"columns": {"x": "TEXT"}},
                "schema": {}, "db": "testdb",
            })
        resp = client.get("/schema", params={"db": "testdb"})
        assert "things" in resp.json()["schema"]
