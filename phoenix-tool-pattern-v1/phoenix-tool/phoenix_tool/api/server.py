from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.ask import ask_data
from phoenix_tool.core import commands as core_commands
from phoenix_tool.core.repository import search_entities
from phoenix_tool.core.response import ErrorItem, build_response


app = FastAPI(title="Phoenix Investigation API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _error_response(command: str, params: dict, code: str, message: str, hint: str, details: str | None = None):
    return build_response(
        command=command,
        params=params,
        data=None,
        warnings=[],
        ok=False,
        error=ErrorItem(code=code, message=message, hint=hint, details=details),
    )


def _ensure_events(command: str, params: dict):
    counts = core_commands.status()
    if counts.get("events", 0) == 0:
        return _error_response(
            command=command,
            params=params,
            code="EMPTY_DB",
            message="No parsed events found.",
            hint="Run ingest → normalize → parse (or CLI build).",
        )
    return None


def _ask_payload(question: str):
    payload = ask_data(question)
    if not payload.get("ok", True):
        return payload
    intent = payload.get("intent")
    primary_entity = payload.get("pid")
    answer = f"Intent: {intent}. Review the evidence and suggested actions."
    evidence = []
    suggested = []
    data = payload.get("data", {})
    if isinstance(data, dict):
        if "events" in data:
            evidence = data.get("events", [])[:10]
        if "nodes" in data:
            suggested = list(data.get("nodes", []))[:10]
        if "partners" in data:
            suggested = [p.get("partner_id") for p in data.get("partners", []) if p.get("partner_id")]
    return {
        "answer": answer,
        "evidence": evidence,
        "suggested_entities": suggested,
        "primary_entity": primary_entity,
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/")
async def index():
    index_path = STATIC_DIR / "index.html"
    return HTMLResponse(index_path.read_text(encoding="utf-8"))


@app.get("/entities")
async def entities(q: str = "", limit: int = 20):
    params = {"q": q, "limit": limit}
    try:
        results = search_entities(query=q, limit=limit) if q else core_commands.recent_entities(limit=limit)["entities"]
        return build_response(
            command="entities",
            params=params,
            data={"entities": results},
        )
    except Exception as exc:  # pragma: no cover - safety net for API
        return _error_response("entities", params, "INTERNAL", "Failed to fetch entities.", "Check server logs.", str(exc))


@app.get("/events")
async def events(
    entity: Optional[str] = None,
    limit: int = 200,
    type: Optional[str] = Query(default=None, alias="type"),
    item: Optional[str] = None,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
):
    params = {
        "ids": [entity] if entity else None,
        "between_ids": None,
        "name": None,
        "item": item,
        "event_type": type,
        "min_money": None,
        "max_money": None,
        "ts_from": from_ts,
        "ts_to": to_ts,
        "limit": limit,
        "collapse": "smart",
    }
    if not entity:
        return _error_response("search", params, "VALIDATION", "Missing entity.", "Provide an entity id to search.")
    empty = _ensure_events("search", params)
    if empty:
        return empty
    try:
        data = core_commands.search(params)
        warnings = data.pop("warnings", [])
        if data.get("matched", 0) == 0:
            return _error_response("search", params, "NOT_FOUND", "No events found for entity.", "Try Search first.")
        return build_response(command="search", params=params, data=data, warnings=warnings)
    except Exception as exc:
        return _error_response("search", params, "INTERNAL", "Search failed.", "Check server logs.", str(exc))


@app.get("/summary")
async def summary(entity: str, collapse: str | None = "smart"):
    params = {"id": entity, "collapse": collapse}
    if not entity:
        return _error_response("summary", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
    empty = _ensure_events("summary", params)
    if empty:
        return empty
    try:
        data = core_commands.summary(entity, collapse=collapse)
        warnings = data.pop("warnings", [])
        if not data.get("events"):
            return _error_response("summary", params, "NOT_FOUND", "No summary data found.", "Try Search first.")
        return build_response(command="summary", params=params, data=data, warnings=warnings)
    except Exception as exc:
        return _error_response("summary", params, "INTERNAL", "Summary failed.", "Check server logs.", str(exc))


@app.get("/storages")
async def storages(entity: str, container: Optional[str] = None, from_ts: Optional[str] = Query(default=None, alias="from"), to_ts: Optional[str] = Query(default=None, alias="to")):
    params = {"id": entity, "container": container, "from": from_ts, "to": to_ts}
    if not entity:
        return _error_response("storages", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
    empty = _ensure_events("storages", params)
    if empty:
        return empty
    try:
        data = core_commands.storages(entity, container, from_ts, to_ts)
        warnings = data.pop("warnings", [])
        if not data.get("containers"):
            return _error_response("storages", params, "NOT_FOUND", "No storage data found.", "Try Search first.")
        return build_response(
            command="storages",
            params=params,
            data=data,
            warnings=warnings,
        )
    except Exception as exc:
        return _error_response("storages", params, "INTERNAL", "Storages failed.", "Check server logs.", str(exc))


@app.get("/flow")
async def flow(entity: str, direction: str = "both", depth: int = 4, window: int = 120, item: Optional[str] = None):
    params = {"id": entity, "direction": direction, "depth": depth, "window": window, "item": item}
    if not entity:
        return _error_response("flow", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
    empty = _ensure_events("flow", params)
    if empty:
        return empty
    try:
        data = core_commands.flow(entity, direction=direction, depth=depth, window=window, item=item)
        if not data.get("chains"):
            return _error_response("flow", params, "NOT_FOUND", "No flow data found.", "Try Search first.")
        return build_response(command="flow", params=params, data=data)
    except Exception as exc:
        return _error_response("flow", params, "INTERNAL", "Flow failed.", "Check server logs.", str(exc))


@app.get("/trace")
async def trace(entity: str, depth: int = 2, item: Optional[str] = None):
    params = {"id": entity, "depth": depth, "item": item}
    if not entity:
        return _error_response("trace", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
    empty = _ensure_events("trace", params)
    if empty:
        return empty
    try:
        data = core_commands.trace_path(entity, depth=depth, item=item)
        if not data.get("events"):
            return _error_response("trace", params, "NOT_FOUND", "No trace data found.", "Try Search first.")
        return build_response(command="trace", params=params, data=data)
    except Exception as exc:
        return _error_response("trace", params, "INTERNAL", "Trace failed.", "Check server logs.", str(exc))


@app.get("/between")
async def between(a: str, b: str, limit: int = 200, from_ts: Optional[str] = Query(default=None, alias="from"), to_ts: Optional[str] = Query(default=None, alias="to")):
    base_params = {"a": a, "b": b, "limit": limit, "from": from_ts, "to": to_ts}
    if not a or not b:
        return _error_response("between", base_params, "VALIDATION", "Missing entities.", "Provide both entity ids.")
    empty = _ensure_events("between", base_params)
    if empty:
        return empty
    params = {
        "ids": None,
        "between_ids": [a, b],
        "name": None,
        "item": None,
        "event_type": None,
        "min_money": None,
        "max_money": None,
        "ts_from": from_ts,
        "ts_to": to_ts,
        "limit": limit,
        "collapse": "smart",
    }
    try:
        data = core_commands.search(params)
        warnings = data.pop("warnings", [])
        if data.get("matched", 0) == 0:
            return _error_response("between", base_params, "NOT_FOUND", "No connecting events found.", "Try Search first.")
        return build_response(command="between", params=base_params, data=data, warnings=warnings)
    except Exception as exc:
        return _error_response("between", base_params, "INTERNAL", "Between failed.", "Check server logs.", str(exc))


@app.get("/ask")
async def ask(q: str = ""):
    params = {"q": q}
    if not q:
        return _error_response("ask", params, "VALIDATION", "Missing question.", "Provide a question to ask.")
    empty = _ensure_events("ask", params)
    if empty:
        return empty
    try:
        payload = _ask_payload(q)
        if payload.get("ok") is False:
            return _error_response("ask", params, "VALIDATION", payload.get("message", "Ask failed."), "Include a valid entity id.")
        return build_response(command="ask", params=params, data=payload)
    except Exception as exc:
        return _error_response("ask", params, "INTERNAL", "Ask failed.", "Check server logs.", str(exc))


@app.post("/build")
async def build_db():
    params = {}
    try:
        data = {
            "normalized": core_commands.normalize()["normalized"],
            "parsed": core_commands.parse()["parsed"],
        }
        return build_response(command="build", params=params, data=data)
    except Exception as exc:
        return _error_response("build", params, "INTERNAL", "Build failed.", "Check server logs.", str(exc))
