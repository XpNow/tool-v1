from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.ask import ask_data
from app.flow import build_flow
from app.normalize import normalize_all
from app.parse import parse_events
from app.report import build_case_file
from app.search import search_events, count_search_events
from app.storages import compute_storage_summary
from app.summary import summary_for_id
from app.trace import trace
from app.ingest import load_logs
from app.identity import rebuild_identities, show_identity
from app.hub import build_hub
from app.audit import audit_unparsed
from app.repository import fetch_event_counts, fetch_recent_entities
from app.util import format_money_ro
from app.render.common import collapse_events, count_warnings
from .serialize import to_dict
from .warnings import warnings_from_lines


def ingest(path: str) -> dict[str, Any]:
    loaded = load_logs(path, silent=True)
    return {"loaded": loaded, "path": path}


def normalize() -> dict[str, Any]:
    normalized = normalize_all(silent=True)
    return {"normalized": normalized}


def parse() -> dict[str, Any]:
    parsed = parse_events(silent=True)
    return {"parsed": parsed}


def identities() -> dict[str, Any]:
    sightings = rebuild_identities(silent=True)
    return {"sightings": sightings}


def identity_lookup(value: str) -> dict[str, Any]:
    result = show_identity(value, as_data=True)
    return {"query": value, "identities": to_dict(result)}


def search(params: dict[str, Any]) -> dict[str, Any]:
    rows = search_events(
        ids=params.get("ids"),
        between_ids=params.get("between_ids"),
        name=params.get("name"),
        item=params.get("item"),
        event_type=params.get("event_type"),
        min_money=params.get("min_money"),
        max_money=params.get("max_money"),
        ts_from=params.get("ts_from"),
        ts_to=params.get("ts_to"),
        limit=params.get("limit", 500),
    )
    matched = count_search_events(
        ids=params.get("ids"),
        between_ids=params.get("between_ids"),
        name=params.get("name"),
        item=params.get("item"),
        event_type=params.get("event_type"),
        min_money=params.get("min_money"),
        max_money=params.get("max_money"),
        ts_from=params.get("ts_from"),
        ts_to=params.get("ts_to"),
    )
    warnings = warnings_from_lines(count_warnings(rows))
    collapse = params.get("collapse")
    evidence = collapse_events(rows, collapse)
    return {
        "events": to_dict(evidence),
        "matched": matched,
        "limit": params.get("limit", 500),
        "warnings": warnings,
    }


def between(params: dict[str, Any]) -> dict[str, Any]:
    data = search(params)
    return data


def summary(pid: str, collapse: str | None = None) -> dict[str, Any]:
    summary_data = summary_for_id(pid, collapse=collapse)
    warnings = warnings_from_lines(count_warnings(summary_data["events"]))
    evidence = collapse_events(summary_data["events"], collapse)
    return {
        "pid": pid,
        "events": to_dict(evidence),
        "event_counts": summary_data["event_counts"],
        "money_in": summary_data["money_in"],
        "money_out": summary_data["money_out"],
        "money_in_formatted": format_money_ro(summary_data["money_in"]) if summary_data["money_in"] is not None else None,
        "money_out_formatted": format_money_ro(summary_data["money_out"]) if summary_data["money_out"] is not None else None,
        "top_partners": to_dict(summary_data["top_partners"]),
        "warnings": warnings,
    }


def storages(pid: str, container: str | None, ts_from: str | None, ts_to: str | None) -> dict[str, Any]:
    containers, warnings_lines, negative_count = compute_storage_summary(
        pid,
        container_filter=container,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    warnings = warnings_from_lines(warnings_lines)
    return {
        "pid": pid,
        "container": container,
        "containers": to_dict(containers),
        "negative_storage_count": negative_count,
        "warnings": warnings,
    }


def flow(pid: str, direction: str, depth: int, window: int, item: str | None) -> dict[str, Any]:
    if direction == "both":
        out_chains = build_flow(pid, direction="out", depth=depth, window_minutes=window, item_filter=item)
        in_chains = build_flow(pid, direction="in", depth=depth, window_minutes=window, item_filter=item)
        chains = [{"direction": "out", "chain": to_dict(c)} for c in out_chains] + [
            {"direction": "in", "chain": to_dict(c)} for c in in_chains
        ]
    else:
        chains = [{"direction": direction, "chain": to_dict(c)} for c in build_flow(pid, direction=direction, depth=depth, window_minutes=window, item_filter=item)]
    return {
        "pid": pid,
        "direction": direction,
        "depth": depth,
        "window": window,
        "item": item,
        "chains": chains,
    }


def trace_path(pid: str, depth: int, item: str | None) -> dict[str, Any]:
    events, nodes = trace(pid, depth=depth, item_filter=item)
    return {
        "pid": pid,
        "depth": depth,
        "item": item,
        "nodes": sorted(nodes),
        "events": to_dict(events),
    }


def report(pid: str) -> dict[str, Any]:
    case_dir, events, identities = build_case_file(pid)
    return {
        "pid": pid,
        "case_dir": str(case_dir),
        "events": to_dict(events),
        "identities": to_dict(identities),
    }


def audit() -> dict[str, Any]:
    out_path, total_groups, limit_groups = audit_unparsed()
    return {
        "path": str(out_path),
        "total_groups": total_groups,
        "limit_groups": limit_groups,
    }


def ask(question: str) -> dict[str, Any]:
    return to_dict(ask_data(question))


def status() -> dict[str, Any]:
    return fetch_event_counts()


def hub() -> dict[str, Any]:
    out = build_hub(silent=True)
    return {"path": str(out)}


def recent_entities(limit: int = 10) -> dict[str, Any]:
    return {"entities": fetch_recent_entities(limit=limit)}
