"""
Pytest suite for the SQL System project.

Unit tests for every function across all modules:
  - database.py       : _db_path, get_connection, execute
  - validator.py      : _validate, _validate_create/select/insert/alter/drop,
                        _run, _read_schema, _push_schema_update, endpoints
  - query_service.py  : _parse, schema_update, handle_query
  - schema_manager.py : subscribe, get_schema, schema_update, _notify_subscribers
  - data_loader.py    : infer_type, infer_schema, load
  - cli.py            : print_result, run
  - llm_service.py    : suggest
  - run.py            : wait_for_service
"""

import os
import sys
import csv
import json
import asyncio
import tempfile
from io import StringIO
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
import pytest_asyncio
import httpx
import respx

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────

@pytest.fixture(autouse=True)
def tmp_db_dir(monkeypatch, tmp_path):
    """Redirect all database operations to a temp directory."""
    import config
    monkeypatch.setattr(config, "DB_DIR", str(tmp_path))
    return tmp_path


# ══════════════════════════════════════════════
# database.py
# ══════════════════════════════════════════════

class TestDatabaseDbPath:
    def test_db_path_returns_correct_path(self, tmp_db_dir):
        from services.database import _db_path
        path = _db_path("mydb")
        assert path == os.path.join(str(tmp_db_dir), "mydb.db")

    def test_db_path_creates_directory(self, tmp_path, monkeypatch):
        import config
        new_dir = str(tmp_path / "subdir")
        monkeypatch.setattr(config, "DB_DIR", new_dir)
        from services.database import _db_path
        _db_path("testdb")
        assert os.path.isdir(new_dir)


class TestDatabaseGetConnection:
    def test_get_connection_returns_connection(self):
        import services.database as database
        conn = database.get_connection("testdb")
        assert conn is not None
        conn.close()

    def test_get_connection_row_factory(self):
        import services.database as database
        import sqlite3
        conn = database.get_connection("testdb")
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_get_connection_default_db(self):
        import services.database as database
        import config
        conn = database.get_connection()
        assert conn is not None
        conn.close()


class TestDatabaseExecute:
    def test_execute_create_table(self):
        import services.database as database
        result = database.execute("CREATE TABLE t1 (id INTEGER, name TEXT)", db="testdb")
        assert result["ok"] is True

    def test_execute_insert_and_select(self):
        import services.database as database
        database.execute("CREATE TABLE t2 (id INTEGER, val TEXT)", db="testdb")
        database.execute("INSERT INTO t2 (id, val) VALUES (1, 'hello')", db="testdb")
        result = database.execute("SELECT * FROM t2", db="testdb")
        assert result["ok"] is True
        assert len(result["rows"]) == 1
        assert result["rows"][0]["id"] == 1
        assert result["rows"][0]["val"] == "hello"

    def test_execute_select_empty(self):
        import services.database as database
        database.execute("CREATE TABLE t3 (x INTEGER)", db="testdb")
        result = database.execute("SELECT * FROM t3", db="testdb")
        assert result["ok"] is True
        assert result["rows"] == []

    def test_execute_bad_sql(self):
        import services.database as database
        result = database.execute("NOT VALID SQL", db="testdb")
        assert result["ok"] is False
        assert "error" in result

    def test_execute_with_params(self):
        import services.database as database
        database.execute("CREATE TABLE t4 (a TEXT)", db="testdb")
        result = database.execute("INSERT INTO t4 (a) VALUES (?)", ("foo",), db="testdb")
        assert result["ok"] is True

    def test_execute_multiple_rows(self):
        import services.database as database
        database.execute("CREATE TABLE t5 (id INTEGER)", db="testdb")
        database.execute("INSERT INTO t5 VALUES (1)", db="testdb")
        database.execute("INSERT INTO t5 VALUES (2)", db="testdb")
        database.execute("INSERT INTO t5 VALUES (3)", db="testdb")
        result = database.execute("SELECT * FROM t5", db="testdb")
        assert len(result["rows"]) == 3

    def test_execute_default_db(self):
        import services.database as database
        result = database.execute("CREATE TABLE def_t (x INTEGER)")
        assert result["ok"] is True


# ══════════════════════════════════════════════
# validator.py — individual _validate_* functions
# ══════════════════════════════════════════════

class TestValidateCreate:
    def test_valid(self):
        from services.validator import _validate_create
        result = _validate_create("new_t", {"columns": {"id": "INTEGER"}}, {})
        assert result["valid"] is True

    def test_table_already_exists(self):
        from services.validator import _validate_create
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        result = _validate_create("users", {"columns": {"id": "INTEGER"}}, schema)
        assert result["valid"] is False
        assert "already exists" in result["reason"]

    def test_no_columns(self):
        from services.validator import _validate_create
        result = _validate_create("t", {}, {})
        assert result["valid"] is False

    def test_empty_columns(self):
        from services.validator import _validate_create
        result = _validate_create("t", {"columns": {}}, {})
        assert result["valid"] is False


class TestValidateSelect:
    def test_valid(self):
        from services.validator import _validate_select
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_select("users", schema)["valid"] is True

    def test_table_not_exists(self):
        from services.validator import _validate_select
        result = _validate_select("nope", {})
        assert result["valid"] is False
        assert "does not exist" in result["reason"]


class TestValidateInsert:
    def test_valid(self):
        from services.validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "TEXT"}]}
        result = _validate_insert("users", {"values": {"id": "1", "name": "A"}}, schema)
        assert result["valid"] is True

    def test_table_not_exists(self):
        from services.validator import _validate_insert
        result = _validate_insert("nope", {"values": {"a": "1"}}, {})
        assert result["valid"] is False

    def test_unknown_columns(self):
        from services.validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        result = _validate_insert("users", {"values": {"id": "1", "bad": "x"}}, schema)
        assert result["valid"] is False
        assert "Unknown columns" in result["reason"]

    def test_empty_values(self):
        from services.validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        result = _validate_insert("users", {"values": {}}, schema)
        assert result["valid"] is True

    def test_no_values_key(self):
        from services.validator import _validate_insert
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        result = _validate_insert("users", {}, schema)
        assert result["valid"] is True


class TestValidateAlter:
    def test_valid(self):
        from services.validator import _validate_alter
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_alter("users", schema)["valid"] is True

    def test_table_not_exists(self):
        from services.validator import _validate_alter
        result = _validate_alter("nope", {})
        assert result["valid"] is False


class TestValidateDrop:
    def test_valid(self):
        from services.validator import _validate_drop
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        assert _validate_drop("users", schema)["valid"] is True

    def test_table_not_exists(self):
        from services.validator import _validate_drop
        result = _validate_drop("nope", {})
        assert result["valid"] is False


# ══════════════════════════════════════════════
# validator.py — _validate (dispatcher)
# ══════════════════════════════════════════════

class TestValidateDispatcher:
    def _make_req(self, **kwargs):
        from services.validator import ExecuteRequest
        defaults = {
            "type": "query", "action": "SELECT", "target": "users",
            "payload": {}, "schema": {}, "db": "testdb",
        }
        defaults.update(kwargs)
        return ExecuteRequest(**defaults)

    def test_dispatches_create(self):
        from services.validator import _validate
        req = self._make_req(
            action="CREATE_TABLE", target="t",
            payload={"columns": {"id": "INTEGER"}}, schema={}
        )
        assert _validate(req)["valid"] is True

    def test_dispatches_select(self):
        from services.validator import _validate
        req = self._make_req(
            action="SELECT", target="t",
            schema={"t": [{"name": "id", "type": "INTEGER"}]}
        )
        assert _validate(req)["valid"] is True

    def test_dispatches_insert(self):
        from services.validator import _validate
        req = self._make_req(
            action="INSERT", target="t",
            payload={"values": {"id": "1"}},
            schema={"t": [{"name": "id", "type": "INTEGER"}]}
        )
        assert _validate(req)["valid"] is True

    def test_dispatches_alter(self):
        from services.validator import _validate
        req = self._make_req(
            action="ALTER", target="t",
            schema={"t": [{"name": "id", "type": "INTEGER"}]}
        )
        assert _validate(req)["valid"] is True

    def test_dispatches_drop(self):
        from services.validator import _validate
        req = self._make_req(
            action="DROP", target="t",
            schema={"t": [{"name": "id", "type": "INTEGER"}]}
        )
        assert _validate(req)["valid"] is True

    def test_unknown_action(self):
        from services.validator import _validate
        req = self._make_req(action="TRUNCATE", schema={})
        result = _validate(req)
        assert result["valid"] is False
        assert "Unknown action" in result["reason"]


# ══════════════════════════════════════════════
# validator.py — _run (execution)
# ══════════════════════════════════════════════

class TestValidatorRun:
    def _make_req(self, **kwargs):
        from services.validator import ExecuteRequest
        defaults = {
            "type": "query", "action": "SELECT", "target": "t",
            "payload": {}, "schema": {}, "db": "testdb",
        }
        defaults.update(kwargs)
        return ExecuteRequest(**defaults)

    def _create_table(self, name="items"):
        from services.validator import _run
        _run(self._make_req(
            type="schema_op", action="CREATE_TABLE", target=name,
            payload={"columns": {"id": "INTEGER", "name": "TEXT"}}
        ))

    def test_create_table(self):
        from services.validator import _run
        req = self._make_req(
            type="schema_op", action="CREATE_TABLE", target="items",
            payload={"columns": {"id": "INTEGER", "name": "TEXT"}}
        )
        assert _run(req)["ok"] is True

    def test_select_empty(self):
        from services.validator import _run
        self._create_table()
        result = _run(self._make_req(action="SELECT", target="items"))
        assert result["ok"] is True
        assert result["rows"] == []

    def test_insert(self):
        from services.validator import _run
        self._create_table()
        req = self._make_req(
            action="INSERT", target="items",
            payload={"values": {"id": "1", "name": "Widget"}}
        )
        assert _run(req)["ok"] is True

    def test_insert_and_select(self):
        from services.validator import _run
        self._create_table()
        _run(self._make_req(
            action="INSERT", target="items",
            payload={"values": {"id": "1", "name": "Widget"}}
        ))
        result = _run(self._make_req(action="SELECT", target="items"))
        assert len(result["rows"]) == 1
        assert result["rows"][0]["name"] == "Widget"

    def test_alter_add_column(self):
        from services.validator import _run
        self._create_table()
        req = self._make_req(
            type="schema_op", action="ALTER", target="items",
            payload={"add_columns": {"price": "REAL"}}
        )
        assert _run(req)["ok"] is True

    def test_alter_duplicate_column_error(self):
        from services.validator import _run
        self._create_table()
        req = self._make_req(
            type="schema_op", action="ALTER", target="items",
            payload={"add_columns": {"id": "INTEGER"}}
        )
        result = _run(req)
        assert result["ok"] is False

    def test_drop_table(self):
        from services.validator import _run
        self._create_table()
        req = self._make_req(type="schema_op", action="DROP", target="items")
        assert _run(req)["ok"] is True

    def test_unknown_action(self):
        from services.validator import _run
        req = self._make_req(action="UNKNOWN")
        result = _run(req)
        assert result["ok"] is False
        assert "No executor" in result["error"]


# ══════════════════════════════════════════════
# validator.py — _read_schema
# ══════════════════════════════════════════════

class TestValidatorReadSchema:
    def test_empty_db(self):
        from services.validator import _read_schema
        assert _read_schema("testdb") == {}

    def test_with_table(self):
        import services.database as database
        from services.validator import _read_schema
        database.execute("CREATE TABLE products (id INTEGER, name TEXT)", db="testdb")
        schema = _read_schema("testdb")
        assert "products" in schema
        col_names = [c["name"] for c in schema["products"]]
        assert "id" in col_names
        assert "name" in col_names

    def test_multiple_tables(self):
        import services.database as database
        from services.validator import _read_schema
        database.execute("CREATE TABLE a (x INTEGER)", db="testdb")
        database.execute("CREATE TABLE b (y TEXT)", db="testdb")
        schema = _read_schema("testdb")
        assert "a" in schema
        assert "b" in schema

    def test_column_types(self):
        import services.database as database
        from services.validator import _read_schema
        database.execute("CREATE TABLE typed (age INTEGER, score REAL, name TEXT)", db="testdb")
        schema = _read_schema("testdb")
        types = {c["name"]: c["type"] for c in schema["typed"]}
        assert types["age"] == "INTEGER"
        assert types["score"] == "REAL"
        assert types["name"] == "TEXT"


# ══════════════════════════════════════════════
# validator.py — _push_schema_update (async)
# ══════════════════════════════════════════════

class TestValidatorPushSchemaUpdate:
    @pytest.mark.asyncio
    async def test_push_schema_update_success(self):
        from services.validator import _push_schema_update
        import config
        schema = {"t": [{"name": "id", "type": "INTEGER"}]}
        with respx.mock:
            respx.post(f"{config.URLS['schema']}/schema-update").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            # Should not raise
            await _push_schema_update("testdb", schema)

    @pytest.mark.asyncio
    async def test_push_schema_update_failure_is_silent(self):
        from services.validator import _push_schema_update
        import config
        with respx.mock:
            respx.post(f"{config.URLS['schema']}/schema-update").mock(
                side_effect=httpx.ConnectError("unreachable")
            )
            # Should not raise even on error
            await _push_schema_update("testdb", {})


# ══════════════════════════════════════════════
# validator.py — FastAPI endpoints
# ══════════════════════════════════════════════

class TestValidatorEndpoints:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from services.validator import app
        return TestClient(app, raise_server_exceptions=False)

    def test_execute_create_table(self, client):
        resp = client.post("/execute", json={
            "type": "schema_op", "action": "CREATE_TABLE",
            "target": "test_table",
            "payload": {"columns": {"id": "INTEGER", "name": "TEXT"}},
            "schema": {}, "db": "testdb"
        })
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_execute_select_after_create(self, client):
        client.post("/execute", json={
            "type": "schema_op", "action": "CREATE_TABLE",
            "target": "t", "payload": {"columns": {"id": "INTEGER"}},
            "schema": {}, "db": "testdb"
        })
        schema = {"t": [{"name": "id", "type": "INTEGER"}]}
        resp = client.post("/execute", json={
            "type": "query", "action": "SELECT", "target": "t",
            "payload": {}, "schema": schema, "db": "testdb"
        })
        assert resp.json()["ok"] is True
        assert resp.json()["rows"] == []

    def test_execute_insert(self, client):
        client.post("/execute", json={
            "type": "schema_op", "action": "CREATE_TABLE",
            "target": "t", "payload": {"columns": {"id": "INTEGER"}},
            "schema": {}, "db": "testdb"
        })
        schema = {"t": [{"name": "id", "type": "INTEGER"}]}
        resp = client.post("/execute", json={
            "type": "query", "action": "INSERT", "target": "t",
            "payload": {"values": {"id": "42"}}, "schema": schema, "db": "testdb"
        })
        assert resp.json()["ok"] is True

    def test_execute_validation_failure(self, client):
        resp = client.post("/execute", json={
            "type": "query", "action": "SELECT", "target": "nonexistent",
            "payload": {}, "schema": {}, "db": "testdb"
        })
        assert resp.json()["ok"] is False
        assert "does not exist" in resp.json()["reason"]

    def test_get_schema_endpoint(self, client):
        resp = client.get("/schema", params={"db": "testdb"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_get_schema_after_create(self, client):
        client.post("/execute", json={
            "type": "schema_op", "action": "CREATE_TABLE",
            "target": "things", "payload": {"columns": {"x": "TEXT"}},
            "schema": {}, "db": "testdb"
        })
        resp = client.get("/schema", params={"db": "testdb"})
        assert "things" in resp.json()["schema"]


# ══════════════════════════════════════════════
# query_service.py — _parse()
# ══════════════════════════════════════════════

class TestQueryServiceParse:
    def _parse(self, text):
        from services.query_service import _parse
        return _parse(text)

    def test_select(self):
        result = self._parse("select users")
        assert result == {"type": "query", "action": "SELECT", "target": "users", "payload": {}}

    def test_select_case_insensitive(self):
        result = self._parse("SELECT users")
        assert result["action"] == "SELECT"

    def test_insert(self):
        result = self._parse("insert users name=Alice age=30")
        assert result["action"] == "INSERT"
        assert result["target"] == "users"
        assert result["payload"]["values"] == {"name": "Alice", "age": "30"}

    def test_insert_single_value(self):
        result = self._parse("insert t col=val")
        assert result["payload"]["values"] == {"col": "val"}

    def test_insert_value_with_equals(self):
        result = self._parse("insert t expr=a=b")
        assert result["payload"]["values"] == {"expr": "a=b"}

    def test_insert_too_few_parts(self):
        assert self._parse("insert users") is None

    def test_create_table(self):
        result = self._parse("create table products id:INTEGER name:TEXT")
        assert result["action"] == "CREATE_TABLE"
        assert result["target"] == "products"
        assert result["payload"]["columns"] == {"id": "INTEGER", "name": "TEXT"}

    def test_create_missing_table_keyword(self):
        assert self._parse("create products id:INTEGER") is None

    def test_create_too_few_parts(self):
        assert self._parse("create table") is None

    def test_alter_add_multiple(self):
        result = self._parse("alter users add email:TEXT phone:TEXT")
        assert result["action"] == "ALTER"
        assert result["target"] == "users"
        assert result["payload"]["add_columns"] == {"email": "TEXT", "phone": "TEXT"}

    def test_alter_single_column_too_few_parts(self):
        assert self._parse("alter users add email:TEXT") is None

    def test_drop(self):
        result = self._parse("drop users")
        assert result == {"type": "schema_op", "action": "DROP", "target": "users", "payload": {}}

    def test_drop_single_word(self):
        assert self._parse("drop") is None

    def test_empty_input(self):
        assert self._parse("") is None

    def test_whitespace_only(self):
        assert self._parse("   ") is None

    def test_unrecognized_input(self):
        assert self._parse("show me everything") is None


# ══════════════════════════════════════════════
# query_service.py — schema_update endpoint
# ══════════════════════════════════════════════

class TestQueryServiceSchemaUpdate:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from services.query_service import app, _schema_cache
        _schema_cache.clear()
        return TestClient(app, raise_server_exceptions=False)

    def test_schema_update_stores_in_cache(self, client):
        from services.query_service import _schema_cache
        schema = {"t": [{"name": "id", "type": "INTEGER"}]}
        resp = client.post("/schema-update", json={"db": "testdb", "schema": schema})
        assert resp.json()["ok"] is True
        assert _schema_cache["testdb"] == schema

    def test_schema_update_default_db(self, client):
        from services.query_service import _schema_cache
        import config
        resp = client.post("/schema-update", json={"schema": {"a": []}})
        assert resp.json()["ok"] is True
        assert config.DEFAULT_DB in _schema_cache


# ══════════════════════════════════════════════
# query_service.py — handle_query endpoint
# ══════════════════════════════════════════════

class TestQueryServiceHandleQuery:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from services.query_service import app, _schema_cache
        _schema_cache.clear()
        return TestClient(app, raise_server_exceptions=False)

    def test_parsed_command_sent_to_validator(self, client):
        """A parseable command should be forwarded to the validator."""
        import config
        validator_response = {"ok": True, "rows": []}
        with respx.mock:
            respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json=validator_response)
            )
            resp = client.post("/query", json={"input": "select users", "db": "testdb"})
        assert resp.json() == validator_response

    def test_unparseable_falls_back_to_llm(self, client):
        """Unparseable input should call LLM, then forward to validator."""
        import config
        llm_suggestion = {
            "type": "query", "action": "SELECT", "target": "users", "payload": {}
        }
        validator_response = {"ok": True, "rows": [{"id": 1}]}
        with respx.mock:
            respx.post(f"{config.URLS['llm']}/suggest").mock(
                return_value=httpx.Response(200, json={"ok": True, "suggestion": llm_suggestion})
            )
            respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json=validator_response)
            )
            resp = client.post("/query", json={"input": "show me all users", "db": "testdb"})
        assert resp.json() == validator_response

    def test_llm_failure_returns_error(self, client):
        """If LLM can't understand input, return error."""
        import config
        with respx.mock:
            respx.post(f"{config.URLS['llm']}/suggest").mock(
                return_value=httpx.Response(200, json={"ok": False, "error": "nope"})
            )
            resp = client.post("/query", json={"input": "asdfghjkl", "db": "testdb"})
        assert resp.json()["ok"] is False
        assert "Could not understand" in resp.json()["reason"]


# ══════════════════════════════════════════════
# schema_manager.py — endpoints
# ══════════════════════════════════════════════

class TestSchemaManagerEndpoints:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from services.schema_manager import app, _schema_cache, _subscribers
        _schema_cache.clear()
        _subscribers.clear()
        return TestClient(app, raise_server_exceptions=False)

    def test_get_schema_empty(self, client):
        resp = client.get("/schema", params={"db": "testdb"})
        assert resp.status_code == 200
        assert resp.json() == {"ok": True, "schema": {}}

    def test_get_schema_default_db(self, client):
        resp = client.get("/schema")
        assert resp.json()["ok"] is True

    def test_subscribe(self, client):
        resp = client.post("/subscribe", json={"webhook_url": "http://localhost:9999/hook"})
        assert resp.json()["ok"] is True
        assert resp.json()["subscribers"] == 1

    def test_subscribe_dedup(self, client):
        client.post("/subscribe", json={"webhook_url": "http://localhost:9999/hook"})
        resp = client.post("/subscribe", json={"webhook_url": "http://localhost:9999/hook"})
        assert resp.json()["subscribers"] == 1

    def test_subscribe_multiple(self, client):
        client.post("/subscribe", json={"webhook_url": "http://a"})
        resp = client.post("/subscribe", json={"webhook_url": "http://b"})
        assert resp.json()["subscribers"] == 2

    def test_schema_update_stores(self, client):
        schema = {"users": [{"name": "id", "type": "INTEGER"}]}
        resp = client.post("/schema-update", json={"db": "testdb", "schema": schema})
        assert resp.json()["ok"] is True
        get_resp = client.get("/schema", params={"db": "testdb"})
        assert get_resp.json()["schema"] == schema

    def test_schema_update_default_db(self, client):
        import config
        client.post("/schema-update", json={"schema": {"x": []}})
        resp = client.get("/schema", params={"db": config.DEFAULT_DB})
        assert resp.json()["schema"] == {"x": []}


# ══════════════════════════════════════════════
# schema_manager.py — _notify_subscribers (async)
# ══════════════════════════════════════════════

class TestSchemaManagerNotifySubscribers:
    @pytest.mark.asyncio
    async def test_notifies_all_subscribers(self):
        from services.schema_manager import _notify_subscribers, _subscribers
        _subscribers.clear()
        _subscribers.extend(["http://a/hook", "http://b/hook"])
        schema = {"t": []}
        with respx.mock:
            route_a = respx.post("http://a/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            route_b = respx.post("http://b/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await _notify_subscribers("testdb", schema)
        assert route_a.called
        assert route_b.called

    @pytest.mark.asyncio
    async def test_unreachable_subscriber_is_skipped(self):
        from services.schema_manager import _notify_subscribers, _subscribers
        _subscribers.clear()
        _subscribers.extend(["http://down/hook", "http://up/hook"])
        with respx.mock:
            respx.post("http://down/hook").mock(
                side_effect=httpx.ConnectError("down")
            )
            route_up = respx.post("http://up/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await _notify_subscribers("db", {})
        assert route_up.called


# ══════════════════════════════════════════════
# data_loader.py — infer_type
# ══════════════════════════════════════════════

class TestInferType:
    def test_integer(self):
        from services.data_loader import infer_type
        assert infer_type("42") == "INTEGER"

    def test_negative_integer(self):
        from services.data_loader import infer_type
        assert infer_type("-5") == "INTEGER"

    def test_zero(self):
        from services.data_loader import infer_type
        assert infer_type("0") == "INTEGER"

    def test_real(self):
        from services.data_loader import infer_type
        assert infer_type("3.14") == "REAL"

    def test_negative_real(self):
        from services.data_loader import infer_type
        assert infer_type("-2.5") == "REAL"

    def test_text(self):
        from services.data_loader import infer_type
        assert infer_type("hello") == "TEXT"

    def test_empty_string(self):
        from services.data_loader import infer_type
        assert infer_type("") == "TEXT"

    def test_mixed_alphanumeric(self):
        from services.data_loader import infer_type
        assert infer_type("abc123") == "TEXT"


# ══════════════════════════════════════════════
# data_loader.py — infer_schema
# ══════════════════════════════════════════════

class TestInferSchema:
    def test_empty_rows(self):
        from services.data_loader import infer_schema
        col_types, rows = infer_schema(iter([]), ["id", "name"])
        assert col_types == {"id": "TEXT", "name": "TEXT"}
        assert rows == []

    def test_integer_columns(self):
        from services.data_loader import infer_schema
        rows = [{"id": "1", "count": "10"}, {"id": "2", "count": "20"}]
        col_types, out = infer_schema(iter(rows), ["id", "count"])
        assert col_types["id"] == "INTEGER"
        assert col_types["count"] == "INTEGER"

    def test_type_widening_int_to_real(self):
        from services.data_loader import infer_schema
        rows = [{"val": "1"}, {"val": "2.5"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "REAL"

    def test_type_widening_int_to_text(self):
        from services.data_loader import infer_schema
        rows = [{"val": "1"}, {"val": "hello"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "TEXT"

    def test_type_widening_real_to_text(self):
        from services.data_loader import infer_schema
        rows = [{"val": "1.5"}, {"val": "hello"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "TEXT"

    def test_preserves_all_rows(self):
        from services.data_loader import infer_schema
        rows = [{"a": "1"}, {"a": "2"}, {"a": "3"}]
        _, out = infer_schema(iter(rows), ["a"])
        assert len(out) == 3

    def test_empty_values_skipped(self):
        from services.data_loader import infer_schema
        rows = [{"val": ""}, {"val": "42"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "INTEGER"


# ══════════════════════════════════════════════
# data_loader.py — load()
# ══════════════════════════════════════════════

class TestDataLoaderLoad:
    def _write_csv(self, tmp_path, filename, header, rows):
        path = tmp_path / filename
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return str(path)

    def test_load_creates_table_and_inserts(self, tmp_db_dir):
        import config
        csv_path = self._write_csv(tmp_db_dir, "items.csv", ["id", "name"], [
            {"id": "1", "name": "Apple"},
            {"id": "2", "name": "Banana"},
        ])
        # Mock the httpx calls to Validator
        with respx.mock:
            # CREATE TABLE
            respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            # GET schema after create
            respx.get(f"{config.URLS['validator']}/schema").mock(
                return_value=httpx.Response(200, json={
                    "schema": {"items": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "TEXT"}]}
                })
            )
            from services.data_loader import load
            load("testdb", csv_path)

    def test_load_empty_csv_exits(self, tmp_db_dir):
        # CSV with no header
        csv_path = tmp_db_dir / "empty.csv"
        csv_path.write_text("")
        from services.data_loader import load
        with pytest.raises(SystemExit):
            load("testdb", str(csv_path))

    def test_load_create_failure_exits(self, tmp_db_dir):
        import config
        csv_path = self._write_csv(tmp_db_dir, "fail.csv", ["a"], [{"a": "1"}])
        with respx.mock:
            respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json={"ok": False, "reason": "exists"})
            )
            from services.data_loader import load
            with pytest.raises(SystemExit):
                load("testdb", str(csv_path))


# ══════════════════════════════════════════════
# cli.py — print_result
# ══════════════════════════════════════════════

class TestCliPrintResult:
    def test_ok_with_rows(self, capsys):
        from services.cli import print_result
        print_result({"ok": True, "rows": [{"id": 1, "name": "Alice"}]})
        out = capsys.readouterr().out
        assert "Alice" in out

    def test_ok_with_multiple_rows(self, capsys):
        from services.cli import print_result
        print_result({"ok": True, "rows": [{"x": 1}, {"x": 2}]})
        out = capsys.readouterr().out
        assert "1" in out
        assert "2" in out

    def test_ok_empty_rows(self, capsys):
        from services.cli import print_result
        print_result({"ok": True, "rows": []})
        assert "no rows" in capsys.readouterr().out

    def test_ok_no_rows_key(self, capsys):
        from services.cli import print_result
        print_result({"ok": True})
        assert "OK" in capsys.readouterr().out

    def test_error_with_reason(self, capsys):
        from services.cli import print_result
        print_result({"ok": False, "reason": "Table not found"})
        assert "Table not found" in capsys.readouterr().out

    def test_error_with_error_field(self, capsys):
        from services.cli import print_result
        print_result({"ok": False, "error": "syntax error"})
        assert "syntax error" in capsys.readouterr().out

    def test_error_unknown(self, capsys):
        from services.cli import print_result
        print_result({"ok": False})
        assert "Unknown error" in capsys.readouterr().out


# ══════════════════════════════════════════════
# cli.py — run()
# ══════════════════════════════════════════════

class TestCliRun:
    def test_exit_command(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=["exit"]):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_empty_input_continues(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=["", "exit"]):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_use_command_switches_db(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=["use mydb", "exit"]):
            run()
        out = capsys.readouterr().out
        assert "mydb" in out

    def test_eof_exits(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=EOFError):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_keyboard_interrupt_exits(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_query_sends_to_service(self, capsys):
        from services.cli import run
        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True, "rows": [{"id": 1}]}
        with patch("builtins.input", side_effect=["select users", "exit"]):
            with patch("httpx.post", return_value=mock_response):
                run()
        out = capsys.readouterr().out
        assert "1" in out

    def test_connection_error(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=["select users", "exit"]):
            with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
                run()
        out = capsys.readouterr().out
        assert "not reachable" in out

    def test_generic_exception(self, capsys):
        from services.cli import run
        with patch("builtins.input", side_effect=["select users", "exit"]):
            with patch("httpx.post", side_effect=RuntimeError("boom")):
                run()
        out = capsys.readouterr().out
        assert "boom" in out


# ══════════════════════════════════════════════
# llm_service.py — suggest endpoint
# ══════════════════════════════════════════════

class TestLlmServiceSuggest:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        import services.llm_service as llm_mod
        return TestClient(llm_mod.app, raise_server_exceptions=False), llm_mod

    def _mock_response(self, text):
        """Create a mock Anthropic messages.create response."""
        content_block = MagicMock()
        content_block.text = text
        resp = MagicMock()
        resp.content = [content_block]
        return resp

    def test_suggest_success(self, client):
        test_client, llm_mod = client
        suggestion = {"type": "query", "action": "SELECT", "target": "users", "payload": {}}
        mock_resp = self._mock_response(json.dumps(suggestion))
        with patch.object(llm_mod.anth.messages, "create", return_value=mock_resp):
            resp = test_client.post("/suggest", json={
                "user_input": "show all users",
                "schema": {"users": [{"name": "id", "type": "INTEGER"}]}
            })
        assert resp.json()["ok"] is True
        assert resp.json()["suggestion"] == suggestion

    def test_suggest_api_failure(self, client):
        test_client, llm_mod = client
        with patch.object(llm_mod.anth.messages, "create", side_effect=Exception("API down")):
            resp = test_client.post("/suggest", json={
                "user_input": "something",
                "schema": {}
            })
        assert resp.json()["ok"] is False
        assert "API down" in resp.json()["error"]

    def test_suggest_invalid_json_response(self, client):
        test_client, llm_mod = client
        mock_resp = self._mock_response("not valid json at all")
        with patch.object(llm_mod.anth.messages, "create", return_value=mock_resp):
            resp = test_client.post("/suggest", json={
                "user_input": "something",
                "schema": {}
            })
        assert resp.json()["ok"] is False


# ══════════════════════════════════════════════
# run.py — wait_for_service
# ══════════════════════════════════════════════

class TestRunWaitForService:
    def test_service_ready_immediately(self):
        from run import wait_for_service
        with patch("httpx.get", return_value=MagicMock()):
            result = wait_for_service("http://localhost:9999", "test", retries=1)
        assert result is True

    def test_service_not_ready(self):
        from run import wait_for_service
        with patch("httpx.get", side_effect=httpx.ConnectError("down")):
            result = wait_for_service("http://localhost:9999", "test", retries=2)
        assert result is False

    def test_service_ready_after_retries(self):
        from run import wait_for_service
        call_count = 0
        def fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ConnectError("not yet")
            return MagicMock()
        with patch("httpx.get", side_effect=fake_get):
            with patch("time.sleep"):
                result = wait_for_service("http://localhost:9999", "test", retries=5)
        assert result is True
