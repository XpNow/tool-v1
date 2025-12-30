from __future__ import annotations

from phoenix_tool.core.runner import run_command


def _assert_schema(payload: dict):
    assert "ok" in payload
    assert "command" in payload
    assert "params" in payload
    assert "warnings" in payload
    assert "data" in payload
    assert "error" in payload
    assert "meta" in payload
    assert "generated_at" in payload["meta"]


def test_runner_schema_empty_db(temp_db):
    commands = [
        ("ingest", {}),
        ("normalize", {}),
        ("parse", {}),
        ("build", {}),
        ("status", {}),
        ("search", {"ids": ["101"]}),
        ("summary", {"id": "101"}),
        ("storages", {"id": "101"}),
        ("flow", {"id": "101"}),
        ("trace", {"id": "101"}),
        ("between", {"a": "101", "b": "202"}),
        ("report", {"id": "101"}),
        ("audit", {}),
        ("ask", {"question": "summary for 101"}),
    ]
    for cmd, params in commands:
        payload = run_command(cmd, params)
        _assert_schema(payload)


def test_runner_search_limit(loaded_db):
    payload = run_command("search", {"ids": ["101"], "limit": 1})
    _assert_schema(payload)
    assert payload["ok"] is True
    assert payload["data"]["returned_count"] <= 1


def test_runner_build_twice(temp_db):
    first = run_command("build", {})
    second = run_command("build", {})
    _assert_schema(first)
    _assert_schema(second)
    assert first["ok"] is True
    assert second["ok"] is True


def test_runner_search_offset(loaded_db):
    payload = run_command("search", {"ids": ["101"], "limit": 1, "offset": 1})
    _assert_schema(payload)
    assert payload["ok"] is True
    assert payload["data"]["offset"] == 1
