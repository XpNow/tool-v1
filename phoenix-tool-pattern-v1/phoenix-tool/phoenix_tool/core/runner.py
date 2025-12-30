from __future__ import annotations

import sqlite3
import time
from typing import Any, Callable

from app import save as save_store
from app import export as export_tools
from app import hub as hub_tools
from app import audit as audit_tools
from app import debug as debug_tools
from phoenix_tool.core import commands as core_commands
from phoenix_tool.core.repository import search_entities
from phoenix_tool.core.response import ErrorItem, WarningItem, build_response


def _error(
    command: str,
    params: dict[str, Any],
    code: str,
    message: str,
    hint: str,
    details: str | None = None,
):
    return build_response(
        command=command,
        params=params,
        data=None,
        warnings=[WarningItem(code=code, message=message, count=1)],
        ok=False,
        error=ErrorItem(code=code, message=message, hint=hint, details=details),
    )


def _normalize_ids(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        ids = [str(v).strip() for v in value if str(v).strip()]
        return ids or None
    if isinstance(value, str):
        parts = [p.strip() for p in value.replace(";", ",").split(",") if p.strip()]
        return parts or None
    return [str(value).strip()]


def _normalize_limit(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_search_params(params: dict[str, Any]) -> dict[str, Any]:
    ids = _normalize_ids(params.get("ids") or params.get("id") or params.get("entity"))
    between_ids = params.get("between_ids") or params.get("between")
    if between_ids and isinstance(between_ids, str):
        between_ids = [p.strip() for p in between_ids.split(",") if p.strip()]
    if not between_ids and params.get("a") and params.get("b"):
        between_ids = [str(params.get("a")), str(params.get("b"))]
    return {
        "ids": ids,
        "between_ids": between_ids if between_ids else None,
        "name": params.get("name"),
        "item": params.get("item"),
        "event_type": params.get("event_type") or params.get("type"),
        "min_money": _normalize_optional_int(params.get("min_money") or params.get("min$")),
        "max_money": _normalize_optional_int(params.get("max_money") or params.get("max$")),
        "ts_from": params.get("ts_from") or params.get("from") or params.get("start") or params.get("since"),
        "ts_to": params.get("ts_to") or params.get("to") or params.get("end") or params.get("until"),
        "limit": _normalize_limit(params.get("limit"), 500),
        "offset": _normalize_limit(params.get("offset"), 0),
        "collapse": params.get("collapse"),
    }


def _ensure_events(command: str, params: dict[str, Any]):
    counts = core_commands.status()
    if counts.get("events", 0) == 0:
        return _error(
            command,
            params,
            "EMPTY_DB",
            "No parsed events found.",
            "Run ingest → normalize → parse (or CLI build).",
        )
    return None


def _as_warnings(items: list[Any] | None) -> list[WarningItem]:
    warnings: list[WarningItem] = []
    for item in items or []:
        if isinstance(item, WarningItem):
            warnings.append(item)
        elif isinstance(item, dict):
            warnings.append(WarningItem(**item))
    return warnings


def _run_with_retry(command: str, params: dict[str, Any], func: Callable[[], dict[str, Any]]):
    for attempt in range(3):
        try:
            return func()
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" in msg or "busy" in msg:
                if attempt < 2:
                    time.sleep(0.2 * (2**attempt))
                    continue
                return _error(
                    command,
                    params,
                    "SQLITE_BUSY",
                    "SQLite is busy. Please retry.",
                    "Wait a moment and retry the command.",
                    details=str(exc),
                )
            return _error(command, params, "INTERNAL", "Database error.", "Check logs.", details=str(exc))
        except Exception as exc:  # pragma: no cover - safety net
            return _error(command, params, "INTERNAL", "Command failed.", "Check logs.", details=str(exc))


def run_command(command: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    params = params or {}
    cmd = (command or "").strip().lower()
    if cmd == "load":
        cmd = "ingest"

    def _execute() -> dict[str, Any]:
        if cmd == "ingest":
            path = params.get("path")
            if not path:
                return _error("ingest", params, "VALIDATION", "Missing path.", "Provide a file or folder path.")
            data = core_commands.ingest(str(path))
            return build_response("ingest", {"path": str(path)}, data)

        if cmd == "normalize":
            data = core_commands.normalize()
            return build_response("normalize", {}, data)

        if cmd == "parse":
            data = core_commands.parse()
            return build_response("parse", {}, data)

        if cmd == "build":
            data = {
                "normalized": core_commands.normalize()["normalized"],
                "parsed": core_commands.parse()["parsed"],
            }
            return build_response("build", {}, data)

        if cmd == "status":
            data = core_commands.status()
            return build_response("status", {}, data)

        if cmd == "identities":
            data = core_commands.identities()
            return build_response("identities", {}, data)

        if cmd == "identity":
            query = params.get("query") or params.get("id") or params.get("name")
            if not query:
                return _error("identity", params, "VALIDATION", "Missing identity query.", "Provide an ID or name.")
            data = core_commands.identity_lookup(str(query))
            return build_response("identity", {"query": str(query)}, data)

        if cmd == "entities":
            q = (params.get("q") or "").strip()
            limit = _normalize_limit(params.get("limit"), 20)
            if q:
                entities = search_entities(query=q, limit=limit)
            else:
                entities = core_commands.recent_entities(limit=limit)["entities"]
            return build_response("entities", {"q": q, "limit": limit}, {"entities": entities})

        if cmd in {"search", "between"}:
            search_params = _normalize_search_params(params)
            if cmd == "between":
                if not search_params.get("between_ids") or len(search_params["between_ids"]) != 2:
                    return _error(
                        "between",
                        params,
                        "VALIDATION",
                        "Missing entity pair.",
                        "Provide both entity ids.",
                    )
            empty = _ensure_events(cmd, search_params)
            if empty:
                return empty
            data = core_commands.search(search_params)
            warnings = _as_warnings(data.pop("warnings", []))
            if data.get("matched_total", 0) == 0:
                return _error(cmd, search_params, "NOT_FOUND", "No events found.", "Adjust filters or ingest more data.")
            return build_response(cmd, search_params, data, warnings=warnings)

        if cmd == "summary":
            entity = params.get("entity")
            if not entity or not str(entity).strip():
                return _error("summary", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
            entity = str(entity).strip()
            empty = _ensure_events("summary", {"entity": entity})
            if empty:
                return empty
            data = core_commands.summary(entity, collapse=params.get("collapse"))
            warnings = _as_warnings(data.pop("warnings", []))
            if not data.get("events"):
                return _error("summary", {"entity": entity}, "NOT_FOUND", "No summary data found.", "Try Search first.")
            return build_response(
                "summary",
                {"entity": entity, "collapse": params.get("collapse")},
                data,
                warnings=warnings,
            )

        if cmd == "storages":
            entity = params.get("entity")
            if not entity or not str(entity).strip():
                return _error("storages", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
            entity = str(entity).strip()
            empty = _ensure_events("storages", {"entity": entity})
            if empty:
                return empty
            data = core_commands.storages(
                entity,
                params.get("container"),
                params.get("from") or params.get("ts_from"),
                params.get("to") or params.get("ts_to"),
            )
            warnings = _as_warnings(data.pop("warnings", []))
            if not data.get("containers"):
                return _error("storages", {"entity": entity}, "NOT_FOUND", "No storage data found.", "Try Search first.")
            return build_response(
                "storages",
                {"entity": entity, "container": params.get("container"), "from": params.get("from"), "to": params.get("to")},
                data,
                warnings=warnings,
            )

        if cmd == "flow":
            entity = params.get("entity")
            if not entity or not str(entity).strip():
                return _error("flow", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
            entity = str(entity).strip()
            empty = _ensure_events("flow", {"entity": entity})
            if empty:
                return empty
            direction = params.get("direction") or params.get("dir") or "both"
            depth = _normalize_limit(params.get("depth"), 4)
            window = _normalize_limit(params.get("window"), 120)
            item = params.get("item")
            data = core_commands.flow(entity, direction=direction, depth=depth, window=window, item=item)
            if not data.get("chains"):
                return _error("flow", {"entity": entity}, "NOT_FOUND", "No flow data found.", "Try Search first.")
            return build_response(
                "flow",
                {"entity": entity, "direction": direction, "depth": depth, "window": window, "item": item},
                data,
            )

        if cmd == "trace":
            pid = params.get("id") or params.get("entity") or params.get("pid")
            if not pid:
                return _error("trace", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
            empty = _ensure_events("trace", {"id": pid})
            if empty:
                return empty
            depth = _normalize_limit(params.get("depth"), 2)
            item = params.get("item")
            data = core_commands.trace_path(str(pid), depth=depth, item=item)
            if not data.get("events"):
                return _error("trace", {"id": pid}, "NOT_FOUND", "No trace data found.", "Try Search first.")
            return build_response("trace", {"id": str(pid), "depth": depth, "item": item}, data)

        if cmd == "report":
            pid = params.get("id") or params.get("entity") or params.get("pid")
            if not pid:
                return _error("report", params, "VALIDATION", "Missing entity.", "Provide an entity id.")
            empty = _ensure_events("report", {"id": pid})
            if empty:
                return empty
            data = core_commands.report(str(pid))
            if not data.get("events"):
                return _error("report", {"id": pid}, "NOT_FOUND", "No report data found.", "Try Search first.")
            return build_response("report", {"id": str(pid)}, data)

        if cmd == "audit":
            data = audit_tools.audit_unparsed()
            return build_response("audit", {}, {"path": str(data[0]), "total_groups": data[1], "limit_groups": data[2]})

        if cmd == "ask":
            question = params.get("question") or params.get("q")
            if not question:
                return _error("ask", params, "VALIDATION", "Missing question.", "Provide a question to ask.")
            empty = _ensure_events("ask", {"question": question})
            if empty:
                return empty
            data = core_commands.ask(str(question))
            if data.get("ok") is False:
                return _error("ask", {"question": question}, "VALIDATION", data.get("message", "Ask failed."), "Include a valid entity id.")
            return build_response("ask", {"question": str(question)}, data)

        if cmd == "save":
            tag = params.get("tag")
            kind = params.get("kind")
            if not tag or not kind:
                return _error("save", params, "VALIDATION", "Missing tag/kind.", "Provide tag and kind.")
            save_store.save_payload(tag, kind, params, silent=True)
            return build_response("save", {"tag": tag, "kind": kind}, {"saved": tag, "kind": kind})

        if cmd == "export":
            tag = params.get("tag")
            fmt = params.get("fmt", "txt")
            if not tag:
                return _error("export", params, "VALIDATION", "Missing tag.", "Provide a tag to export.")
            export_tools.export_tag(tag, fmt=fmt, silent=True)
            return build_response("export", {"tag": tag, "fmt": fmt}, {"exported": tag})

        if cmd == "hub":
            out = hub_tools.build_hub(silent=True)
            return build_response("hub", {}, {"path": str(out)})

        if cmd == "debug":
            out = debug_tools.make_debug_bundle(silent=True)
            return build_response("debug", {}, {"path": str(out)})

        return _error("unknown", params, "VALIDATION", f"Unknown command: {command}", "Check --help for commands.")

    return _run_with_retry(cmd, params, _execute)
