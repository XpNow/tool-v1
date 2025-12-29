from collections import Counter, OrderedDict
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from .db import get_db
from .util import format_money_ro, format_ts_display, actor_label, build_warning_lines, parse_iso_maybe

console = Console()

def summary_for_id(pid: str, collapse: str | None = None):
    pid = str(pid)
    conn = get_db()
    cur = conn.cursor()

    events = cur.execute("""
        SELECT ts, ts_raw, timestamp_quality, event_type, src_id, src_name, dst_id, dst_name, item, qty, money, container
        FROM events
        WHERE src_id=? OR dst_id=?
        ORDER BY (ts IS NULL) ASC, ts ASC, id ASC
    """, (pid, pid)).fetchall()

    rows = cur.execute("""
        SELECT event_type, COUNT(*) c
        FROM events
        WHERE src_id=? OR dst_id=?
        GROUP BY event_type
        ORDER BY c DESC
    """, (pid, pid)).fetchall()

    money_out = cur.execute("""
        SELECT COALESCE(SUM(money),0) s
        FROM events
        WHERE src_id=? AND money IS NOT NULL
    """, (pid,)).fetchone()["s"]

    money_in = cur.execute("""
        SELECT COALESCE(SUM(money),0) s
        FROM events
        WHERE dst_id=? AND money IS NOT NULL
    """, (pid,)).fetchone()["s"]

    top_partners = cur.execute("""
        SELECT
          CASE WHEN src_id=? THEN dst_id ELSE src_id END partner_id,
          CASE WHEN src_id=? THEN dst_name ELSE src_name END partner_name,
          COUNT(*) c
        FROM events
        WHERE (src_id=? OR dst_id=?)
        GROUP BY partner_id, partner_name
        HAVING partner_id IS NOT NULL AND partner_id != ?
        ORDER BY c DESC
        LIMIT 15
    """, (pid, pid, pid, pid, pid)).fetchall()

    conn.close()

    rel = 0
    unk_qty = 0
    unk_container = 0
    for ev in events:
        if (ev["timestamp_quality"] or "").upper() == "RELATIVE":
            rel += 1
        if ev["item"] and ev["qty"] is None:
            unk_qty += 1
        if ev["event_type"] in ("container_put", "container_remove") and not (ev["container"] or "").strip():
            unk_container += 1

    warnings = build_warning_lines(
        relative_count=rel,
        unknown_qty_count=unk_qty,
        unknown_container_count=unk_container,
        negative_storage_count=0,
    )

    header = [
        "[bold]SUMMARY — pattern view[/bold]",
        f"ID: {pid}",
        f"Matched: {len(events)} events | Showing: {min(len(events), 50)}",
        f"Collapse: {collapse or 'smart'}",
        "Warnings: " + " | ".join(warnings),
    ]
    console.print(Panel("\n".join(header), expand=False))

    pattern = [
        f"• Money IN: {format_money_ro(money_in)}",
        f"• Money OUT: {format_money_ro(money_out)}",
        f"• Money NET: {format_money_ro(money_in - money_out)}",
    ]

    if rows:
        pattern.append("• Top types: " + ", ".join([f"{r['event_type']} ({r['c']})" for r in rows[:5]]))

    if top_partners:
        partners_fmt = []
        for r in top_partners[:5]:
            label = actor_label(r["partner_name"], r["partner_id"]) or "UNKNOWN"
            partners_fmt.append(f"{label} ({r['c']})")
        pattern.append("• Top partners: " + ", ".join(partners_fmt))

    console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    grouped = []
    if rows:
        grouped.append("• Types: " + ", ".join([f"{r['event_type']} ({r['c']})" for r in rows[:10]]))

    if top_partners:
        partners_fmt = []
        for r in top_partners[:10]:
            label = actor_label(r["partner_name"], r["partner_id"]) or "UNKNOWN"
            partners_fmt.append(f"{label} ({r['c']})")
        grouped.append("• Partners: " + ", ".join(partners_fmt))

    items = Counter()
    for ev in events:
        item = (ev["item"] or "").strip()
        if item:
            items[item] += 1
    if items:
        grouped.append("• Items: " + ", ".join([f"{k} ({v})" for k, v in items.most_common(10)]))

    if grouped:
        console.print(Panel("\n".join(grouped), title="GROUPED SUMMARY", expand=False))

    def _minute_key(ts, ts_raw):
        dt = parse_iso_maybe(ts or "")
        if dt is not None:
            return dt.replace(second=0, microsecond=0).isoformat()
        return None if not ts_raw else ts_raw.strip()

    evidence_rows = []
    if collapse is None or str(collapse).lower() not in ("0", "false", "no"):
        grouped_events = OrderedDict()
        for ev in events:
            key = (
                _minute_key(ev["ts"], ev["ts_raw"]),
                ev["event_type"],
                ev["src_id"],
                ev["dst_id"],
                ev["item"],
                ev["qty"],
                ev["money"],
                ev["container"],
            )
            if key not in grouped_events:
                grouped_events[key] = {"row": ev, "count": 0}
            grouped_events[key]["count"] += 1

        for item in grouped_events.values():
            row = dict(item["row"])
            row["_count"] = item["count"]
            evidence_rows.append(row)
    else:
        for ev in events:
            row = dict(ev)
            row["_count"] = 1
            evidence_rows.append(row)

    t = Table(title="EVIDENCE", show_lines=True)
    t.add_column("Time")
    t.add_column("Type")
    t.add_column("Count", justify="right")
    t.add_column("From")
    t.add_column("To")
    t.add_column("Container")
    t.add_column("Item")
    t.add_column("Qty", justify="right")
    t.add_column("Money", justify="right")

    for ev in evidence_rows[:50]:
        src = actor_label(ev.get("src_name"), ev.get("src_id"))
        dst = actor_label(ev.get("dst_name"), ev.get("dst_id"))
        qty = ev.get("qty")
        qty_cell = "?" if ev.get("item") and qty is None else (str(qty) if qty is not None else "")
        count_cell = f"x{ev['_count']}" if ev.get("_count", 1) > 1 else ""
        t.add_row(
            format_ts_display(ev.get("ts"), ev.get("ts_raw")),
            str(ev.get("event_type") or ""),
            count_cell,
            src,
            dst,
            str(ev.get("container") or ""),
            str(ev.get("item") or ""),
            qty_cell,
            format_money_ro(ev.get("money")) if ev.get("money") is not None else "",
        )

    console.print(t)

    footer = ["Next: refine with search id=<id> type=..., item=..., from=..., to=..., collapse=0"]
    console.print(Panel("\n".join(footer), expand=False))
