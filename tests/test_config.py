"""Unit tests for config.py - JSON / env loading of the Anthropic key."""

import importlib
import json
import sys

import pytest


def _reload_config():
    if "config" in sys.modules:
        del sys.modules["config"]
    return importlib.import_module("config")


class TestEnvKey:
    def test_env_overrides_json(self, tmp_path, monkeypatch):
        key_file = tmp_path / "Anthropic_API_KEY.json"
        key_file.write_text(json.dumps({"key": "from-json"}))
        monkeypatch.setenv("ANTHROPIC_KEY_FILE", str(key_file))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "from-env")
        cfg = _reload_config()
        assert cfg.LLM_KEY == "from-env"

    def test_no_key_anywhere_is_empty_string(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_KEY_FILE", str(tmp_path / "missing.json"))
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        cfg = _reload_config()
        assert cfg.LLM_KEY == ""


class TestJsonKey:
    def test_loads_key_model_max_tokens(self, tmp_path, monkeypatch):
        key_file = tmp_path / "Anthropic_API_KEY.json"
        key_file.write_text(json.dumps({
            "key": "json-key",
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1234,
        }))
        monkeypatch.setenv("ANTHROPIC_KEY_FILE", str(key_file))
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("ANTHROPIC_MODEL", raising=False)
        monkeypatch.delenv("ANTHROPIC_MAX_TOKENS", raising=False)
        cfg = _reload_config()
        assert cfg.LLM_KEY == "json-key"
        assert cfg.LLM_MODEL == "claude-haiku-4-5-20251001"
        assert cfg.LLM_MAX_TOKENS == 1234

    def test_malformed_json_does_not_crash(self, tmp_path, monkeypatch):
        key_file = tmp_path / "Anthropic_API_KEY.json"
        key_file.write_text("{not valid json")
        monkeypatch.setenv("ANTHROPIC_KEY_FILE", str(key_file))
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        cfg = _reload_config()
        assert cfg.LLM_KEY == ""


class TestPortsAndUrls:
    def test_default_ports(self, monkeypatch):
        for name in ("QUERY_PORT", "SCHEMA_PORT", "LLM_PORT", "VALIDATOR_PORT"):
            monkeypatch.delenv(name, raising=False)
        cfg = _reload_config()
        assert cfg.PORTS == {"query": 8000, "schema": 8001, "llm": 8002, "validator": 8003}

    def test_env_overrides_port(self, monkeypatch):
        monkeypatch.setenv("QUERY_PORT", "9999")
        cfg = _reload_config()
        assert cfg.PORTS["query"] == 9999

    def test_env_overrides_host(self, monkeypatch):
        monkeypatch.setenv("VALIDATOR_HOST", "validator.svc")
        cfg = _reload_config()
        assert cfg.URLS["validator"].startswith("http://validator.svc:")
