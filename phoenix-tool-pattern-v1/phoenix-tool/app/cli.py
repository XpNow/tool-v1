import json
import sys
from rich.console import Console
from rich.panel import Panel

from .db import init_db
from .ingest import load_logs
from .normalize import normalize_all
from .parse import parse_events
from .identity import rebuild_identities, show_identity
from .repository import search_events, count_search_events
from .trace import trace
from .flow import build_flow
from .summary import summary_for_id
from .report import build_case_file
from .save import save_payload
from .export import export_tag
from .hub import build_hub
from .audit import audit_unparsed
from .debug import make_debug_bundle
from .status import show_status
from .ask import ask_dispatch
from .storages import compute_storage_summary
from .render import (
    render_search,
    render_trace,
    render_flow,
    render_summary,
    render_storages,
    render_report,
    render_audit,
)
from phoenix_tool.core.response import build_error
from phoenix_tool.core.runner import run_command

console = Console()

HELP_TEXT = """Phoenix Investigation Tool

Commands:

help
  Show this help

load <path>
  Load .txt logs into database (raw evidence)

ingest <path>
  Alias for load

normalize
  Normalize raw logs into clean lines with timestamps

parse
  Parse normalized lines into structured events

build
  Shortcut: normalize + parse

identities
  Rebuild identity observations (ID <-> name <-> IP)

identity <id|name>
  Show identity info for one ID or one name

search [filters]
  Examples:
    search id=633
    search name=VataRman
    search type=bank_transfer min$=1000000
    search id=633 item=\"Navy Revolver\"

trace <id> [depth=2] [item=\"...\"]

flow <id> [dir=in|out] [depth=4] [window=120] [item=\"...\"]
  Strict chain tracing (time coherent when timestamps exist)

summary <id>
  Quick overview (counts, totals, top partners)

report <id>
  Generate per-ID case file folder (output/reports/ID_<id>/)

storages <id> [container=NAME] [from=ISO] [to=ISO]
  Best-effort container state: sums container_put minus container_remove.
  If container= is provided, prints current contents for matching containers.

ask <question>
  Natural-language helper (RO/EN). Maps your question to existing commands safely.
  Examples:
    ask Arata-mi daca id 161 a dat iteme si a primit la schimb ceva
    ask Quick timeline for ID 161 from 19.12.2025 00:00 to 19.12.2025 03:00

save tag=NAME kind=trace|flow|search|summary report=<id> [extra args...]
  Save a snapshot for exporting

export <tag> fmt=txt|html|json
  Export saved snapshot to output/exports/

hub
  Build local HTML manual (output/hub/index.html)

audit
  Build unparsed-line audit clusters (output/audit/audit_unparsed.txt)

status
  Show parser/db coverage counts (raw logs, normalized lines, events by type)

web
  Start local web UI/API server (http://127.0.0.1:8000)

Options:
  --format pretty|json (default: pretty)

Examples:
  search id=633 --format json
  flow 633 out 3 120 "gold bar"

debug
  Create a debug bundle zip (output/debug/)
"""


def _parse_kv_args(args: list[str]) -> dict:
    out = {}
    for a in args:
        if "=" in a:
            k, v = a.split("=", 1)
            out[k.strip()] = v.strip().strip('"')
    return out


def _has_kv(args: list[str]) -> bool:
    return any("=" in a for a in args)


def _parse_flow_shortcut_args(args: list[str]):
    """Parse flow shortcut args after <id>. Returns (direction, depth, window, item_filter)."""
    direction = None
    depth = None
    window = None
    item_parts: list[str] = []

    for a in args:
        al = a.lower().strip()
        if al in ("in", "out", "both"):
            direction = al
            continue
        if al.isdigit():
            if depth is None:
                depth = int(al)
            elif window is None:
                window = int(al)
            else:
                # ignore extra numbers
                pass
            continue
        item_parts.append(a)

    item = " ".join(item_parts).strip() or None
    return direction, depth, window, item


def _parse_trace_shortcut_args(args: list[str]):
    """Parse trace shortcut args after <id>. Returns (depth, item_filter)."""
    depth = None
    item_parts: list[str] = []
    for a in args:
        if a.isdigit() and depth is None:
            depth = int(a)
        else:
            item_parts.append(a)
    item = " ".join(item_parts).strip() or None
    return depth, item


def _parse_search_shortcut_args(args: list[str]):
    """Parse search shortcut args. Returns (ids, name, item, event_type, limit)."""
    ids = None
    event_type = None
    limit = None
    item_parts: list[str] = []

    # between ids: <id1> - <id2>
    if len(args) >= 3 and args[0].isdigit() and args[1] == "-" and args[2].isdigit():
        ids = [args[0], args[2]]
        rest = args[3:]
    elif args and args[0].isdigit():
        ids = [args[0]]
        rest = args[1:]
    else:
        rest = args

    # last numeric token => limit
    if rest and rest[-1].isdigit():
        limit = int(rest[-1])
        rest = rest[:-1]

    # allow a simple type keyword (transfer/ofera/etc.) by treating a single token that matches an event_type
    # if it doesn't match, it becomes part of item search.
    if rest:
        # If user wrote something like: search 123 transfer
        if len(rest) == 1 and rest[0].isidentifier():
            event_type = rest[0]
        else:
            item_parts = rest

    item = " ".join(item_parts).strip() or None
    return ids, None, item, event_type, limit


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    output_format = "pretty"
    if "--format" in argv:
        idx = argv.index("--format")
        if idx + 1 < len(argv):
            output_format = argv[idx + 1]
            argv = argv[:idx] + argv[idx + 2 :]
    else:
        for i, arg in enumerate(list(argv)):
            if arg.startswith("--format="):
                output_format = arg.split("=", 1)[1]
                argv.pop(i)
                break

    if not argv or argv[0] in ("help", "-h", "--help"):
        console.print(Panel(HELP_TEXT.strip(), title="HELP"))
        return 0

    cmd, *args = argv

    # ensure DB schema
    init_db()

    def emit_response(payload: dict) -> int:
        print(json.dumps(payload, ensure_ascii=False))
        return 0 if payload.get("ok") else 1

    def emit_error(command: str, params: dict, message: str, code: str = "VALIDATION", hint: str = "Check usage."):
        response = build_error(command=command, params=params, message=message, code=code, hint=hint)
        print(json.dumps(response, ensure_ascii=False))
        return 1

    if cmd == "load":
        if not args:
            if output_format == "json":
                return emit_error("ingest", {}, "Usage: load <path>")
            console.print("[red]Usage:[/red] load <path>")
            return 1
        if output_format == "json":
            return emit_response(run_command("ingest", {"path": args[0]}))
        n = load_logs(args[0])
        console.print(Panel(f"Raw logs loaded: {n}", title="LOAD"))
        return 0

    if cmd == "ingest":
        if not args:
            if output_format == "json":
                return emit_error("ingest", {}, "Usage: ingest <path>")
            console.print("[red]Usage:[/red] ingest <path>")
            return 1
        if output_format == "json":
            return emit_response(run_command("ingest", {"path": args[0]}))
        n = load_logs(args[0])
        console.print(Panel(f"Raw logs loaded: {n}", title="LOAD"))
        return 0

    if cmd == "normalize":
        if output_format == "json":
            return emit_response(run_command("normalize", {}))
        n = normalize_all()
        console.print(Panel(f"Normalized lines inserted: {n}", title="NORMALIZE"))
        return 0

    if cmd == "parse":
        if output_format == "json":
            return emit_response(run_command("parse", {}))
        n = parse_events()
        console.print(Panel(f"Events parsed and inserted: {n}", title="PARSE"))
        return 0

    if cmd == "build":
        if output_format == "json":
            return emit_response(run_command("build", {}))
        n_norm = normalize_all()
        n_parse = parse_events()
        console.print(Panel(f"Normalized lines inserted: {n_norm}", title="NORMALIZE"))
        console.print(Panel(f"Events parsed and inserted: {n_parse}", title="PARSE"))
        return 0

    if cmd == "identities":
        if output_format == "json":
            return emit_response(run_command("identities", {}))
        n = rebuild_identities(silent=output_format == "json")
        console.print(Panel(f"Identity sightings rebuilt: {n}", title="IDENTITIES"))
        return 0

    if cmd == "identity":
        if not args:
            if output_format == "json":
                return emit_error("identity", {}, "Usage: identity <id|name>")
            console.print("[red]Usage:[/red] identity <id|name>")
            return 1
        query = " ".join(args)
        if output_format == "json":
            return emit_response(run_command("identity", {"query": query}))
        show_identity(query)
        return 0

    if cmd == "search":
        # Supports both legacy key=value syntax and shortcut syntax.
        kv = _parse_kv_args(args)
        if not kv and args:
            ids, name, item, et, lim = _parse_search_shortcut_args(args)
            kv = {}
            if ids:
                kv["id"] = ",".join(ids)
            if name:
                kv["name"] = name
            if item:
                kv["item"] = item
            if et:
                kv["type"] = et
            if lim is not None:
                kv["limit"] = str(lim)

        ids = None
        if "id" in kv:
            ids = [x.strip() for x in kv["id"].split(",") if x.strip()]

        between_ids = None
        if "between" in kv:
            parts = [p.strip() for p in kv["between"].split(",") if p.strip()]
            if len(parts)==2:
                between_ids = parts

        if output_format == "json":
            params = {
                "ids": ids,
                "between_ids": between_ids,
                "name": kv.get("name"),
                "item": kv.get("item"),
                "event_type": kv.get("type"),
                "min_money": int(kv.get("min$", "0")) if "min$" in kv else None,
                "max_money": int(kv.get("max$", "0")) if "max$" in kv else None,
                "ts_from": kv.get("from") or kv.get("start") or kv.get("since"),
                "ts_to": kv.get("to") or kv.get("end") or kv.get("until"),
                "limit": int(kv.get("limit", "500")),
                "collapse": kv.get("collapse", "smart"),
            }
            return emit_response(run_command("search", params))

        rows = search_events(
            ids=ids,
            between_ids=between_ids,
            name=kv.get("name"),
            item=kv.get("item"),
            event_type=kv.get("type"),
            min_money=int(kv.get("min$", "0")) if "min$" in kv else None,
            max_money=int(kv.get("max$", "0")) if "max$" in kv else None,
            ts_from=kv.get("from") or kv.get("start") or kv.get("since"),
            ts_to=kv.get("to") or kv.get("end") or kv.get("until"),
            limit=int(kv.get("limit", "500")),
        )
        matched = count_search_events(
            ids=ids,
            between_ids=between_ids,
            name=kv.get("name"),
            item=kv.get("item"),
            event_type=kv.get("type"),
            min_money=int(kv.get("min$", "0")) if "min$" in kv else None,
            max_money=int(kv.get("max$", "0")) if "max$" in kv else None,
            ts_from=kv.get("from") or kv.get("start") or kv.get("since"),
            ts_to=kv.get("to") or kv.get("end") or kv.get("until"),
        )
        meta = {
            'title': 'SEARCH — pattern view',
            'query': ' '.join([f"{k}={v}" for k,v in kv.items()]),
            'window': f"{kv.get('from') or kv.get('start') or kv.get('since') or 'ALL'} → {kv.get('to') or kv.get('end') or kv.get('until') or 'ALL'}",
            'limit': int(kv.get('limit', '500')),
            'collapse': kv.get('collapse', 'smart'),
            'focus_id': (ids[0] if ids and len(ids)==1 else None),
            'between_ids': between_ids,
            'matched': matched,
        }
        render_search(rows, meta)
        return 0

    if cmd == "trace":
        if not args:
            if output_format == "json":
                return emit_error("trace", {}, "Usage: trace <id> [depth=2] [item=...]")
            console.print("[red]Usage:[/red] trace <id> [depth=2] [item=...]")
            return 1
        pid = args[0]
        kv = _parse_kv_args(args[1:])
        if not kv and args[1:]:
            d2, item2 = _parse_trace_shortcut_args(args[1:])
            kv = {}
            if d2 is not None:
                kv["depth"] = str(d2)
            if item2:
                kv["item"] = item2

        depth = int(kv.get("depth", "2"))
        item = kv.get("item")
        if output_format == "json":
            return emit_response(run_command("trace", {"id": pid, "depth": depth, "item": item}))
        events, nodes = trace(pid, depth=depth, item_filter=item)
        render_trace(pid, events, nodes, depth, item)
        return 0

    if cmd == "flow":
        if not args:
            if output_format == "json":
                return emit_error("flow", {}, "Usage: flow <id> [dir=in|out] [depth=4] [window=120] [item=...]")
            console.print("[red]Usage:[/red] flow <id> [dir=in|out] [depth=4] [window=120] [item=...]")
            return 1
        pid = args[0]
        kv = _parse_kv_args(args[1:])
        if not kv and args[1:]:
            ddir, ddepth, dwindow, ditem = _parse_flow_shortcut_args(args[1:])
            kv = {}
            if ddir:
                kv["dir"] = ddir
            if ddepth is not None:
                kv["depth"] = str(ddepth)
            if dwindow is not None:
                kv["window"] = str(dwindow)
            if ditem:
                kv["item"] = ditem

        direction = kv.get("dir", "out")
        depth = int(kv.get("depth", "4"))
        window = int(kv.get("window", "120"))
        item = kv.get("item")
        if output_format == "json":
            return emit_response(
                run_command("flow", {"id": pid, "direction": direction, "depth": depth, "window": window, "item": item})
            )
        if direction.lower() == "both":
            chains_out = build_flow(pid, direction="out", depth=depth, window_minutes=window, item_filter=item)
            chains_in = build_flow(pid, direction="in", depth=depth, window_minutes=window, item_filter=item)
            combined = [("out", c) for c in chains_out] + [("in", c) for c in chains_in]
            render_flow(pid, combined, direction="both")
        else:
            chains = build_flow(pid, direction=direction, depth=depth, window_minutes=window, item_filter=item)
            render_flow(pid, chains, direction=direction)
        return 0

    if cmd == "summary":
        if not args:
            if output_format == "json":
                return emit_error("summary", {}, "Usage: summary <id>")
            console.print("[red]Usage:[/red] summary <id>")
            return 1
        kv = _parse_kv_args(args[1:])
        if output_format == "json":
            return emit_response(run_command("summary", {"id": args[0], "collapse": kv.get("collapse")}))
        summary = summary_for_id(args[0], collapse=kv.get("collapse"))
        render_summary(
            summary["pid"],
            summary["events"],
            summary["event_counts"],
            summary["money_in"],
            summary["money_out"],
            summary["top_partners"],
            summary["collapse"],
        )
        return 0

    if cmd == "report":
        if not args:
            if output_format == "json":
                return emit_error("report", {}, "Usage: report <id>")
            console.print("[red]Usage:[/red] report <id>")
            return 1
        if output_format == "json":
            return emit_response(run_command("report", {"id": args[0]}))
        case_dir, events, identities = build_case_file(args[0])
        render_report(args[0], case_dir, events, identities)
        return 0

    if cmd == "storages":
        if not args:
            if output_format == "json":
                return emit_error("storages", {}, "Usage: storages <id> [container=NAME] [from=ISO] [to=ISO]")
            console.print("[red]Usage:[/red] storages <id> [container=NAME] [from=ISO] [to=ISO]")
            return 1
        pid = args[0]
        kv = _parse_kv_args(args[1:])
        if output_format == "json":
            return emit_response(
                run_command(
                    "storages",
                    {"id": pid, "container": kv.get("container"), "from": kv.get("from"), "to": kv.get("to")},
                )
            )
        containers, warnings, negative_count = compute_storage_summary(
            pid,
            container_filter=kv.get("container"),
            ts_from=kv.get("from"),
            ts_to=kv.get("to"),
        )
        render_storages(pid, kv.get("container"), containers, warnings, negative_count)
        return 0

    if cmd in ("ask", "chat"):
        if not args:
            if output_format == "json":
                return emit_error("ask", {}, "Usage: ask <question>")
            console.print("[red]Usage:[/red] ask <question>")
            return 1
        q = " ".join(args).strip()
        if output_format == "json":
            return emit_response(run_command("ask", {"question": q}))
        ask_dispatch(q)
        return 0

    if cmd == "save":
        kv = _parse_kv_args(args)
        if "tag" not in kv or "kind" not in kv:
            if output_format == "json":
                return emit_error("save", kv, "Usage: save tag=NAME kind=trace|flow|search|summary [args...]")
            console.print("[red]Usage:[/red] save tag=NAME kind=trace|flow|search|summary [args...] ")
            return 1
        if output_format == "json":
            return emit_response(run_command("save", kv))
        save_payload(kv['tag'], kv['kind'], kv)
        console.print(Panel(f"Saved: {kv['tag']}", title="SAVE"))
        return 0

    if cmd == "export":
        if not args:
            if output_format == "json":
                return emit_error("export", {}, "Usage: export <tag> fmt=txt|html|json")
            console.print("[red]Usage:[/red] export <tag> fmt=txt|html|json")
            return 1
        tag = args[0]
        kv = _parse_kv_args(args[1:])
        if output_format == "json":
            return emit_response(run_command("export", {"tag": tag, "fmt": kv.get("fmt", "txt")}))
        export_tag(tag, fmt=kv.get("fmt", "txt"))
        return 0

    if cmd == "hub":
        if output_format == "json":
            return emit_response(run_command("hub", {}))
        out = build_hub()
        console.print(Panel(str(out), title="HUB"))
        return 0

    if cmd == "audit":
        if output_format == "json":
            return emit_response(run_command("audit", {}))
        out_path, total_groups, limit_groups = audit_unparsed()
        render_audit(out_path, total_groups, limit_groups)
        return 0

    if cmd == "status":
        if output_format == "json":
            return emit_response(run_command("status", {}))
        show_status()
        return 0

    if cmd == "debug":
        if output_format == "json":
            return emit_response(run_command("debug", {}))
        out = make_debug_bundle()
        console.print(Panel(str(out), title="DEBUG"))
        return 0

    if cmd == "web":
        if output_format == "json":
            return emit_error("web", {}, "Use `python -m phoenix_tool.api.server` to start the web server.", hint="Run the web server command directly.")
        import uvicorn

        console.print("[green]Starting web server...[/green]")
        uvicorn.run("phoenix_tool.api.server:app", host="127.0.0.1", port=8000, reload=False)
        return 0

    if output_format == "json":
        return emit_error("unknown", {"command": cmd}, f"Unknown command: {cmd}", hint="Run `help` to see commands.")
    console.print(Panel(f"Unknown command: {cmd}\n\n" + HELP_TEXT.strip(), title="ERROR"))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
