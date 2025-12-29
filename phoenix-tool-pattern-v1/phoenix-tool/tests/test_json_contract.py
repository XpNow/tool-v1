from __future__ import annotations

import json

from app.cli import main


def _run(argv):
    return main(argv)


def _load_json(capsys):
    out = capsys.readouterr().out.strip()
    assert out, "Expected JSON output"
    return json.loads(out)


def _assert_schema(payload, command):
    assert payload["ok"] is True
    assert payload["command"] == command
    assert "params" in payload
    assert "warnings" in payload
    assert "data" in payload
    assert "meta" in payload
    assert "generated_at" in payload["meta"]


def test_json_search(loaded_db, capsys):
    _run(["search", "id=101", "--format", "json"])
    payload = _load_json(capsys)
    _assert_schema(payload, "search")
    assert "events" in payload["data"]


def test_json_summary(loaded_db, capsys):
    _run(["summary", "101", "--format", "json"])
    payload = _load_json(capsys)
    _assert_schema(payload, "summary")
    assert payload["data"]["pid"] == "101"


def test_json_storages(loaded_db, capsys):
    _run(["storages", "101", "--format", "json"])
    payload = _load_json(capsys)
    _assert_schema(payload, "storages")
    assert "containers" in payload["data"]


def test_json_status(loaded_db, capsys):
    _run(["status", "--format", "json"])
    payload = _load_json(capsys)
    _assert_schema(payload, "status")
    assert "events_by_type" in payload["data"]
