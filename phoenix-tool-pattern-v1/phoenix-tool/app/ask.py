import re
from dataclasses import dataclass
from datetime import datetime, timezone

from rich.console import Console
from rich.panel import Panel

from .repository import search_events
from .report import build_case_file
from .flow import build_flow
from .trace import trace
from .summary import summary_for_id
from .repository import fetch_directional_events
from .render import render_search, render_flow, render_trace, render_summary, render_report
from .util import format_ts_display, render_event_line, format_money_ro


console = Console()


@dataclass
class AskIntent:
    kind: str
    pid: str | None = None
    pid2: str | None = None
    item: str | None = None
    ts_from: str | None = None
    ts_to: str | None = None


_RE_ID = re.compile(r"\b(\d{1,7})\b")
_RE_ITEM_Q = re.compile(r"\"([^\"]+)\"")


def _parse_user_dt(s: str) -> str | None:
    """Parse user datetime into ISO UTC Z (matches normalize.ts storage).

    Supported inputs:
      - DD.MM.YYYY HH:MM
      - YYYY-MM-DD HH:MM
      - DD.MM.YYYY (assumes 00:00)
      - YYYY-MM-DD (assumes 00:00)

    Note: interpreted as UTC. (For your current datasets, winter dates match UK time.)
    """
    s = (s or "").strip()
    if not s:
        return None

    fmts = [
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M",
        "%d.%m.%Y",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            continue
    return None


def _extract_time_range(q: str) -> tuple[str | None, str | None]:
    """Extract a simple time range from a question.

    Accepts separators:
      - "from <A> to <B>"
      - "între <A> și <B>" / "intre <A> si <B>"
      - "<A> - <B>"
    where A/B are one of the supported datetime formats.
    """
    qn = " ".join(q.split())

    # from A to B
    m = re.search(r"\bfrom\s+(.+?)\s+to\s+(.+?)\b", qn, re.I)
    if m:
        return _parse_user_dt(m.group(1)), _parse_user_dt(m.group(2))

    # intre A si B
    m = re.search(r"\bintre\s+(.+?)\s+(?:si|și)\s+(.+?)\b", qn, re.I)
    if m:
        return _parse_user_dt(m.group(1)), _parse_user_dt(m.group(2))

    # A - B (with dates)
    m = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4}(?:\s+\d{1,2}:\d{2})?)\s*-\s*(\d{1,2}\.\d{1,2}\.\d{4}(?:\s+\d{1,2}:\d{2})?)", qn)
    if m:
        return _parse_user_dt(m.group(1)), _parse_user_dt(m.group(2))
    m = re.search(r"(\d{4}-\d{1,2}-\d{1,2}(?:\s+\d{1,2}:\d{2})?)\s*-\s*(\d{4}-\d{1,2}-\d{1,2}(?:\s+\d{1,2}:\d{2})?)", qn)
    if m:
        return _parse_user_dt(m.group(1)), _parse_user_dt(m.group(2))

    return None, None


def _classify_intent(q: str) -> AskIntent:
    qn = " ".join(q.strip().split())
    ql = qn.lower()

    ids = _RE_ID.findall(qn)
    pid = ids[0] if ids else None
    pid2 = ids[1] if len(ids) > 1 else None

    item = None
    m = _RE_ITEM_Q.search(qn)
    if m:
        item = m.group(1).strip()

    ts_from, ts_to = _extract_time_range(qn)

    # report pack
    if any(k in ql for k in ["report pack", "pachet de raport", "pachet raport", "build report", "construieste pachet", "construiește pachet", "report "]):
        return AskIntent(kind="report", pid=pid)

    # timeline
    if any(k in ql for k in ["timeline", "cronologie", "cronologia", "quick timeline", "cronologie rapida", "cronologie rapidă"]):
        return AskIntent(kind="timeline", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # exchange check
    if any(k in ql for k in ["exchange", "la schimb", "schimb", "trade", "a primit la schimb", "a primit la schimb ceva", "a dat iteme"]):
        return AskIntent(kind="exchange", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # item story (flow)
    if any(k in ql for k in ["item story", "poveste", "povestea item", "urma", "urmărește", "urmareste", "follow"]):
        return AskIntent(kind="item_story", pid=pid, item=item)

    # telefon pairing view
    if "telefon" in ql and any(k in ql for k in ["pair", "perechi", "vizualizare", "transferuri"]):
        return AskIntent(kind="telefon", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # banking review
    if any(k in ql for k in ["banking", "banca", "banc", "revizuire bancara", "revizuire bancară", "bancar"]):
        return AskIntent(kind="banking", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # vehicle activity
    if any(k in ql for k in ["vehicul", "vehicle", "showroom", "remat"]):
        return AskIntent(kind="vehicles", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # dropped items
    if any(k in ql for k in ["aruncat", "aruncate", "dropped", "pe jos", "obiect aruncat"]):
        return AskIntent(kind="dropped", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # partners overview (do NOT dump full trace by default)
    if any(k in ql for k in ["partners", "partener", "parteneri", "cu cine", "with whom", "top partners"]):
        return AskIntent(kind="partners", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # explicit graph trace
    if any(k in ql for k in ["trace", "legaturi", "legături", "connections", "graph"]):
        return AskIntent(kind="trace", pid=pid, ts_from=ts_from, ts_to=ts_to)

    # summary
    if any(k in ql for k in ["summary", "rezumat", "overview"]):
        return AskIntent(kind="summary", pid=pid)

    # Safe default: timeline search for the ID
    return AskIntent(kind="timeline", pid=pid, ts_from=ts_from, ts_to=ts_to)


def _query_directional(pid: str, direction: str, types: list[str], ts_from: str | None, ts_to: str | None, limit: int = 300):
    return fetch_directional_events(
        pid=pid,
        direction=direction,
        types=types,
        ts_from=ts_from,
        ts_to=ts_to,
        limit=limit,
    )


def _render_exchange_candidates(pid: str, ts_from: str | None, ts_to: str | None):
    """Conservative exchange helper.

    What it does:
      - groups by counterparty
      - only highlights counterparties where BOTH directions exist (something left AND something returned)

    What it avoids:
      - forcing a 1-to-1 pairing between specific events
      - claiming a definitive "trade" when time is missing
    """

    out_types = ["ofera_item", "ofera_bani", "phone_remove", "bank_transfer"]
    in_types = ["ofera_item", "ofera_bani", "phone_add", "bank_transfer"]

    out_rows = _query_directional(pid, "out", out_types, ts_from, ts_to, limit=800)
    in_rows = _query_directional(pid, "in", in_types, ts_from, ts_to, limit=800)

    # Build per-partner buckets
    buckets: dict[str, dict] = {}

    def _bp(partner_id: str, partner_name: str | None):
        b = buckets.setdefault(partner_id, {
            "partner_name": partner_name or "",
            "out": [],
            "in": [],
            "out_money": 0,
            "in_money": 0,
        })
        if partner_name and not b["partner_name"]:
            b["partner_name"] = partner_name
        return b

    for r in out_rows:
        partner_id = str(r.dst_id or "")
        if not partner_id:
            continue
        b = _bp(partner_id, r.dst_name)
        b["out"].append(r)
        if r.money:
            b["out_money"] += int(r.money)

    for r in in_rows:
        partner_id = str(r.src_id or "")
        if not partner_id:
            continue
        b = _bp(partner_id, r.src_name)
        b["in"].append(r)
        if r.money:
            b["in_money"] += int(r.money)

    # keep only candidates where both directions exist
    candidates = []
    for pid2, b in buckets.items():
        if b["out"] and b["in"]:
            candidates.append((pid2, b))

    # sort by total interactions
    candidates.sort(key=lambda x: (len(x[1]["out"]) + len(x[1]["in"])), reverse=True)

    header = f"Exchange candidates for ID {pid}"
    if ts_from or ts_to:
        header += f"\nRange: {format_ts_display(ts_from, None)} → {format_ts_display(ts_to, None)}"
    console.print(Panel(header, title="ASK/EXCHANGE"))

    if not candidates:
        console.print("No counterparties found with evidence in BOTH directions (gave + received).")
        console.print("[dim]Tip: widen the time range, or check item-only movement via flow.[/dim]")
        return

    # Show top N candidates with compact evidence
    show_n = 8
    for i, (pid2, b) in enumerate(candidates[:show_n], start=1):
        pname = b.get("partner_name") or ""
        title = f"Option {i}: {pname}[{pid2}] — OUT {len(b['out'])} / IN {len(b['in'])}"
        if b["out_money"] or b["in_money"]:
            title += f" — money OUT {format_money_ro(b['out_money'])} / IN {format_money_ro(b['in_money'])}"
        console.print(Panel(title, title_align="left"))

        # Show a few lines each direction (narrative)
        out_lines = ["  - " + render_event_line(ev) for ev in b["out"][:5]]
        in_lines = ["  - " + render_event_line(ev) for ev in b["in"][:5]]
        if out_lines:
            console.print("[bold]GAVE (OUT):[/bold]\n" + "\n".join(out_lines))
        if in_lines:
            console.print("[bold]RECEIVED (IN):[/bold]\n" + "\n".join(in_lines))
        if len(b["out"]) > 5 or len(b["in"]) > 5:
            console.print("[dim]…more events exist for this counterparty (use search for full list).[/dim]")

    console.print("\n[dim]Note: options are grouped evidence only. No 1-to-1 trade pairing is forced.[/dim]")


def _render_timeline(pid: str, ts_from: str | None, ts_to: str | None):
    rows = search_events(ids=[pid], ts_from=ts_from, ts_to=ts_to, limit=500)
    title = f"Timeline for ID {pid}"
    if ts_from or ts_to:
        title += f"\nRange: {format_ts_display(ts_from, None)} → {format_ts_display(ts_to, None)}"
    console.print(Panel(title, title="ASK/TIMELINE"))

    if not rows:
        console.print("No events found for the requested filters.")
        return

    # Narrative list (readable in Discord/terminal)
    lines = ["- " + render_event_line(r) for r in rows[:200]]
    if len(rows) > 200:
        lines.append("- … (trimmed) …")
        lines.append("- Tip: use search with a narrower time range for full output")
    console.print("\n".join(lines))


def _render_partners(pid: str, ts_from: str | None, ts_to: str | None):
    """Top partners without printing a huge trace graph."""
    rows = search_events(ids=[pid], ts_from=ts_from, ts_to=ts_to, limit=2000)
    counts = {}
    for ev in rows:
        if ev.src_id == pid and ev.dst_id:
            key = (ev.dst_id, ev.dst_name)
        elif ev.dst_id == pid and ev.src_id:
            key = (ev.src_id, ev.src_name)
        else:
            continue
        counts[key] = counts.get(key, 0) + 1

    top_partners = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:25]

    title = f"Top partners for ID {pid}"
    if ts_from or ts_to:
        title += f"\nRange: {format_ts_display(ts_from, None)} → {format_ts_display(ts_to, None)}"
    console.print(Panel(title, title="ASK/PARTNERS"))

    if not top_partners:
        console.print("No partner interactions found for the requested filters.")
        return

    for i, (partner, count) in enumerate(top_partners, start=1):
        pid2, pname = partner
        pid2 = pid2 or ""
        pname = pname or ""
        console.print(f"{i:>2}. {pname}[{pid2}] — {count}")


def ask_dispatch(question: str):
    intent = _classify_intent(question)

    if not intent.pid:
        console.print(Panel("I couldn't find an ID in your question. Include an ID number (e.g. 161).", title="ASK"))
        return

    if intent.kind == "report":
        case_dir, events, identities = build_case_file(intent.pid)
        render_report(intent.pid, case_dir, events, identities)
        return

    if intent.kind == "summary":
        summary = summary_for_id(intent.pid)
        render_summary(
            summary["pid"],
            summary["events"],
            summary["event_counts"],
            summary["money_in"],
            summary["money_out"],
            summary["top_partners"],
            summary["collapse"],
        )
        return

    if intent.kind == "partners":
        _render_partners(intent.pid, intent.ts_from, intent.ts_to)
        return

    if intent.kind == "timeline":
        _render_timeline(intent.pid, intent.ts_from, intent.ts_to)
        return

    if intent.kind == "trace":
        events, nodes = trace(intent.pid, depth=2, item_filter=None)
        render_trace(intent.pid, events, nodes, depth=2, item_filter=None)
        return

    if intent.kind == "item_story":
        # Safe default: both directions, shallow depth unless user asked more.
        chains = build_flow(intent.pid, direction="both", depth=2, window_minutes=180, item_filter=intent.item)
        render_flow(intent.pid, chains, direction="both")
        return

    if intent.kind == "banking":
        rows = search_events(ids=[intent.pid], event_type=None, ts_from=intent.ts_from, ts_to=intent.ts_to, limit=500)
        # Filter banking-ish types in-memory to keep search() simple.
        rows2 = [r for r in rows if (r.event_type or "") in {"bank_transfer", "bank_deposit", "bank_withdraw", "phone_add", "phone_remove", "ofera_bani"}]
        console.print(Panel(f"Banking review for ID {intent.pid}", title="ASK"))
        render_search(rows2)
        return

    if intent.kind == "vehicles":
        rows = search_events(ids=[intent.pid], ts_from=intent.ts_from, ts_to=intent.ts_to, limit=500)
        rows2 = [r for r in rows if (r.event_type or "").startswith("vehicle_")]
        console.print(Panel(f"Vehicle activity for ID {intent.pid}", title="ASK"))
        render_search(rows2)
        return

    if intent.kind == "dropped":
        rows = search_events(ids=[intent.pid], event_type="drop_item", ts_from=intent.ts_from, ts_to=intent.ts_to, limit=500)
        console.print(Panel(f"Dropped items for ID {intent.pid}", title="ASK"))
        render_search(rows)
        return

    if intent.kind == "telefon":
        rows = search_events(ids=[intent.pid], ts_from=intent.ts_from, ts_to=intent.ts_to, limit=500)
        rows2 = [r for r in rows if (r.event_type or "") in {"phone_add", "phone_remove"}]
        console.print(Panel(f"Telefon events for ID {intent.pid}", title="ASK"))
        render_search(rows2)
        console.print("\n[dim]Pairing is not forced in ASK mode yet. Use normal views to investigate matches.[/dim]")
        return

    if intent.kind == "exchange":
        _render_exchange_candidates(intent.pid, intent.ts_from, intent.ts_to)
        return

    # Default timeline
    _render_timeline(intent.pid, intent.ts_from, intent.ts_to)
