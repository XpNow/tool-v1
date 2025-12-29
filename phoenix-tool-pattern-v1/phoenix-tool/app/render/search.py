from __future__ import annotations

from collections import Counter

from rich.panel import Panel
from rich.table import Table

from ..models import Event
from ..util import actor_label, format_money_ro, format_ts_display
from .common import console, count_warnings, collapse_events, top_counts, top_items


def _money_totals_for_focus(events: list[Event], focus_id: str) -> tuple[int, int]:
    out_total = 0
    in_total = 0
    fid = str(focus_id)

    for ev in events:
        m = ev.money
        if m is None:
            continue
        if str(ev.src_id or "") == fid:
            out_total += int(m)
        if str(ev.dst_id or "") == fid:
            in_total += int(m)

    return out_total, in_total


def _partner_counts_for_focus(events: list[Event], focus_id: str, limit: int = 8) -> list[tuple[str, int]]:
    c: Counter[str] = Counter()
    fid = str(focus_id)

    for ev in events:
        s = str(ev.src_id or "")
        d = str(ev.dst_id or "")
        if s == fid and d:
            c[d] += 1
        elif d == fid and s:
            c[s] += 1

    return c.most_common(limit)


def _between_summaries(events: list[Event], a: str, b: str, topn: int = 5):
    a = str(a)
    b = str(b)

    out_items: Counter[str] = Counter()
    in_items: Counter[str] = Counter()
    out_money = 0
    in_money = 0

    for ev in events:
        s = str(ev.src_id or "")
        d = str(ev.dst_id or "")
        it = (ev.item or "").strip()
        qty = ev.qty
        m = ev.money

        if s == a and d == b:
            if it and qty is not None:
                out_items[it] += int(qty)
            if m is not None:
                out_money += int(m)
        elif s == b and d == a:
            if it and qty is not None:
                in_items[it] += int(qty)
            if m is not None:
                in_money += int(m)

    return out_items.most_common(topn), in_items.most_common(topn), out_money, in_money


def render_search(events: list[Event], meta: dict | None = None):
    meta = meta or {}
    query = meta.get("query") or ""
    window = meta.get("window") or "ALL"
    limit = meta.get("limit")
    title = meta.get("title") or "SEARCH — pattern view"
    shown = len(events)
    matched = meta.get("matched") or shown
    collapse = meta.get("collapse") or "smart"

    warnings = count_warnings(events)

    hdr_lines = [
        f"[bold]{title}[/bold]",
        f"Query: {query}" if query else "Query: (none)",
        f"Window: {window}",
        f"Matched: {matched} events | Showing: {shown}" + (f" (limit={limit})" if limit is not None else ""),
        f"Collapse: {collapse}",
    ]
    if warnings:
        hdr_lines.append("Warnings: " + " | ".join(warnings))

    console.print(Panel("\n".join(hdr_lines), expand=False))

    focus_id = meta.get("focus_id")
    between_ids = meta.get("between_ids")

    pat: list[str] = []
    top_types = top_counts(events, "event_type", limit=5)
    if top_types:
        pat.append("• Top types: " + ", ".join([f"{k} ({v})" for k, v in top_types]))

    if between_ids and len(between_ids) == 2:
        a, b = between_ids
        out_items, in_items, out_money, in_money = _between_summaries(events, a, b, topn=5)
        if out_items:
            pat.append(f"• OUT {a} → {b}: " + ", ".join([f"{it} {qty:,}".replace(",", " ") for it, qty in out_items]))
        if in_items:
            pat.append(f"• IN  {a} ← {b}: " + ", ".join([f"{it} {qty:,}".replace(",", " ") for it, qty in in_items]))
        if out_money or in_money:
            pat.append(f"• Money: {a}→{b} {format_money_ro(out_money)} | {b}→{a} {format_money_ro(in_money)}")

    elif focus_id:
        partners = _partner_counts_for_focus(events, focus_id, limit=6)
        if partners:
            pat.append("• Top partners: " + ", ".join([f"{pid} ({cnt})" for pid, cnt in partners]))

        out_money, in_money = _money_totals_for_focus(events, focus_id)
        if out_money or in_money:
            pat.append(
                f"• Money: OUT {format_money_ro(out_money)} | IN {format_money_ro(in_money)} | NET {format_money_ro(in_money - out_money)}"
            )

        items = top_items(events, limit=5)
        if items:
            pat.append("• Top items: " + ", ".join([f"{it} ({cnt})" for it, cnt in items]))

    else:
        items = top_items(events, limit=5)
        if items:
            pat.append("• Top items: " + ", ".join([f"{it} ({cnt})" for it, cnt in items]))

    if pat:
        console.print(Panel("\n".join(pat), title="PATTERN", expand=False))

    grouped: list[str] = []
    top_types_full = top_counts(events, "event_type", limit=10)
    if top_types_full:
        grouped.append("• Types: " + ", ".join([f"{k} ({v})" for k, v in top_types_full]))

    top_items_full = top_items(events, limit=10)
    if top_items_full:
        grouped.append("• Items: " + ", ".join([f"{it} ({cnt})" for it, cnt in top_items_full]))

    if focus_id:
        partners_full = _partner_counts_for_focus(events, focus_id, limit=10)
        if partners_full:
            grouped.append("• Partners: " + ", ".join([f"{pid} ({cnt})" for pid, cnt in partners_full]))

    if grouped:
        console.print(Panel("\n".join(grouped), title="GROUPED SUMMARY", expand=False))

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

    collapsed = collapse_events(events, collapse)

    for r in collapsed:
        src_id = r.get("src_id")
        src_name = r.get("src_name")
        dst_id = r.get("dst_id")
        dst_name = r.get("dst_name")

        src = actor_label(src_name, src_id) if src_id or src_name else ""
        dst = actor_label(dst_name, dst_id) if dst_id or dst_name else ""

        container = str(r.get("container") or "")

        it = (r.get("item") or "")
        qty = r.get("qty")
        money = r.get("money")

        count = r.get("_count", 1)

        t.add_row(
            format_ts_display(r.get("ts"), r.get("ts_raw")),
            str(r.get("event_type") or ""),
            f"x{count}" if count > 1 else "",
            src,
            dst,
            container,
            it,
            f"{int(qty):,}".replace(",", " ") if qty is not None else "",
            format_money_ro(int(money)) if money is not None else "",
        )

    console.print(t)

    footer = "Try: limit=50, from=..., to=..., item=..., type=..., collapse=0, export=1"
    console.print(Panel(footer, title="FOOTER", expand=False))
