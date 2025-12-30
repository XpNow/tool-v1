from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from phoenix_tool.core.runner import run_command
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

def _format_validation_details(exc: RequestValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", []))
        msg = err.get("msg", "Invalid input")
        parts.append(f"{loc}: {msg}")
    return "; ".join(parts) or "Validation error"


def _api_error(request: Request, code: str, message: str, hint: str, details: str | None = None, status: int = 400):
    params = {
        "path": request.url.path,
        "method": request.method,
        "query": dict(request.query_params),
    }
    payload = build_response(
        command="api",
        params=params,
        data=None,
        warnings=[],
        ok=False,
        error=ErrorItem(code=code, message=message, hint=hint, details=details),
    )
    return JSONResponse(payload, status_code=status)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    details = _format_validation_details(exc)
    return _api_error(request, "VALIDATION", "Validation error.", "Check required fields and retry.", details, status=422)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return _api_error(request, "INTERNAL", "Request failed.", "Check inputs and retry.", str(exc.detail), status=exc.status_code)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return _api_error(request, "INTERNAL", "Unexpected server error.", "Check server logs and retry.", str(exc), status=500)


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/")
async def index():
    index_path = STATIC_DIR / "index.html"
    return HTMLResponse(index_path.read_text(encoding="utf-8"))


@app.get("/entities")
async def entities(q: str = "", limit: int = 20):
    return run_command("entities", {"q": q, "limit": limit})


@app.get("/search")
async def search(
    entity: Optional[str] = None,
    name: Optional[str] = None,
    item: Optional[str] = None,
    event_type: Optional[str] = Query(default=None, alias="type"),
    limit: int = 200,
    offset: int = 0,
    min_money: Optional[int] = Query(default=None, alias="min$"),
    max_money: Optional[int] = Query(default=None, alias="max$"),
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
):
    params = {
        "entity": entity,
        "name": name,
        "item": item,
        "event_type": event_type,
        "limit": limit,
        "offset": offset,
        "min_money": min_money,
        "max_money": max_money,
        "from": from_ts,
        "to": to_ts,
        "collapse": "smart",
    }
    return run_command("search", params)


@app.get("/summary")
async def summary(entity: str, collapse: str | None = "smart"):
    return run_command("summary", {"entity": entity, "collapse": collapse})


@app.get("/storages")
async def storages(
    entity: str,
    container: Optional[str] = None,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
):
    return run_command(
        "storages",
        {"entity": entity, "container": container, "from": from_ts, "to": to_ts},
    )


@app.get("/flow")
async def flow(entity: str, direction: str = "both", depth: int = 4, window: int = 120, item: Optional[str] = None):
    return run_command(
        "flow",
        {"entity": entity, "direction": direction, "depth": depth, "window": window, "item": item},
    )


@app.get("/trace")
async def trace(entity: str, depth: int = 2, item: Optional[str] = None):
    return run_command("trace", {"id": entity, "depth": depth, "item": item})


@app.get("/between")
async def between(
    a: str,
    b: str,
    limit: int = 200,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
):
    return run_command("between", {"a": a, "b": b, "limit": limit, "from": from_ts, "to": to_ts})


@app.post("/build")
async def build_db():
    return run_command("build", {})


@app.get("/ask")
async def ask_get(q: str = ""):
    return run_command("ask", {"question": q})


@app.post("/ask")
async def ask_post(payload: dict = Body(default_factory=dict)):
    return run_command("ask", {"question": payload.get("question")})
