from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from phoenix_tool.core import commands as core_commands
from phoenix_tool.core.repository import search_entities
from phoenix_tool.core.response import build_response


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


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/")
async def index():
    index_path = STATIC_DIR / "index.html"
    return HTMLResponse(index_path.read_text(encoding="utf-8"))


@app.get("/entities")
async def entities(q: str = "", limit: int = 20):
    results = search_entities(query=q, limit=limit) if q else core_commands.recent_entities(limit=limit)["entities"]
    return build_response(
        command="entities",
        params={"q": q, "limit": limit},
        data={"entities": results},
    )


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
    data = core_commands.search(params)
    warnings = data.pop("warnings", [])
    return build_response(command="search", params=params, data=data, warnings=warnings)


@app.get("/summary")
async def summary(entity: str, collapse: str | None = "smart"):
    data = core_commands.summary(entity, collapse=collapse)
    warnings = data.pop("warnings", [])
    return build_response(command="summary", params={"id": entity, "collapse": collapse}, data=data, warnings=warnings)


@app.get("/storages")
async def storages(entity: str, container: Optional[str] = None, from_ts: Optional[str] = Query(default=None, alias="from"), to_ts: Optional[str] = Query(default=None, alias="to")):
    data = core_commands.storages(entity, container, from_ts, to_ts)
    warnings = data.pop("warnings", [])
    return build_response(
        command="storages",
        params={"id": entity, "container": container, "from": from_ts, "to": to_ts},
        data=data,
        warnings=warnings,
    )


@app.get("/flow")
async def flow(entity: str, direction: str = "both", depth: int = 4, window: int = 120, item: Optional[str] = None):
    data = core_commands.flow(entity, direction=direction, depth=depth, window=window, item=item)
    return build_response(
        command="flow",
        params={"id": entity, "direction": direction, "depth": depth, "window": window, "item": item},
        data=data,
    )


@app.get("/trace")
async def trace(entity: str, depth: int = 2, item: Optional[str] = None):
    data = core_commands.trace_path(entity, depth=depth, item=item)
    return build_response(command="trace", params={"id": entity, "depth": depth, "item": item}, data=data)


@app.get("/between")
async def between(a: str, b: str, limit: int = 200, from_ts: Optional[str] = Query(default=None, alias="from"), to_ts: Optional[str] = Query(default=None, alias="to")):
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
    data = core_commands.search(params)
    warnings = data.pop("warnings", [])
    return build_response(command="between", params={"a": a, "b": b, "limit": limit}, data=data, warnings=warnings)
