from __future__ import annotations

from fastapi.testclient import TestClient

from phoenix_tool.api.server import app


def _assert_schema(payload: dict):
    assert "ok" in payload
    assert "command" in payload
    assert "params" in payload
    assert "warnings" in payload
    assert "data" in payload
    assert "error" in payload
    assert "meta" in payload


def test_health():
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_entities_empty(temp_db):
    client = TestClient(app)
    resp = client.get("/entities?limit=5")
    assert resp.status_code == 200
    payload = resp.json()
    _assert_schema(payload)
    assert payload["ok"] is True


def test_search_empty_db(temp_db):
    client = TestClient(app)
    resp = client.get("/search?entity=101")
    assert resp.status_code == 200
    payload = resp.json()
    _assert_schema(payload)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "EMPTY_DB"


def test_summary_validation_error():
    client = TestClient(app)
    resp = client.get("/summary")
    assert resp.status_code == 422
    payload = resp.json()
    _assert_schema(payload)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "VALIDATION"


def test_summary_loaded_db(loaded_db):
    client = TestClient(app)
    resp = client.get("/summary?entity=101")
    assert resp.status_code == 200
    payload = resp.json()
    _assert_schema(payload)
    assert payload["ok"] is True
    assert payload["data"]["pid"] == "101"


def test_ask_loaded_db(loaded_db):
    client = TestClient(app)
    resp = client.get("/ask?q=summary%20for%20101")
    assert resp.status_code == 200
    payload = resp.json()
    _assert_schema(payload)
    assert payload["ok"] is True


def test_build_twice(temp_db):
    client = TestClient(app)
    first = client.post("/build")
    second = client.post("/build")
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["ok"] is True
    assert second.json()["ok"] is True
