from __future__ import annotations

from collections import Counter

from rich.panel import Panel
from rich.table import Table

from ..models import Event, PartnerStat
from ..util import actor_label, format_money_ro, format_ts_display, render_event_line
from .common import console, collapse_events, count_warnings


def render_summary(
    pid: str,
    events: list[Event],
    event_counts: list[tuple[str, int]],
    money_in: int,
    money_out: int,
    top_partners: list[PartnerStat],
    collapse: str | None = None,
):
    warnings = count_warnings(events)

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

    if event_counts:
        pattern.append("• Top types: " + ", ".join([f"{name} ({count})" for name, count in event_counts[:5]]))

    if top_partners:
        partners_fmt = []
        for stat in top_partners[:5]:
            label = actor_label(stat.partner_name, stat.partner_id) or "UNKNOWN"
            partners_fmt.append(f"{label} ({stat.count})")
        pattern.append("• Top partners: " + ", ".join(partners_fmt))

    console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    grouped: list[str] = []
    if event_counts:
        grouped.append("• Types: " + ", ".join([f"{name} ({count})" for name, count in event_counts[:10]]))

    if top_partners:
        partners_fmt = []
        for stat in top_partners[:10]:
            label = actor_label(stat.partner_name, stat.partner_id) or "UNKNOWN"
            partners_fmt.append(f"{label} ({stat.count})")
        grouped.append("• Partners: " + ", ".join(partners_fmt))

    items = Counter()
    for ev in events:
        item = (ev.item or "").strip()
        if item:
            items[item] += 1
    if items:
        grouped.append("• Items: " + ", ".join([f"{k} ({v})" for k, v in items.most_common(10)]))

    if grouped:
        console.print(Panel("\n".join(grouped), title="GROUPED SUMMARY", expand=False))

    t = Table(title="EVIDENCE", show_lines=True)
    t.add_column("Time")
    t.add_column("Type")
    t.add_column("Count", justify="right")
    t.add_column("From")
    t.add_column("To")
    t.add_column("Item")
    t.add_column("Qty", justify="right")
    t.add_column("Money", justify="right")

    collapsed = collapse_events(events, collapse)

    for r in collapsed[:50]:
        src = actor_label(r.get("src_name"), r.get("src_id")) if r.get("src_id") or r.get("src_name") else ""
        dst = actor_label(r.get("dst_name"), r.get("dst_id")) if r.get("dst_id") or r.get("dst_name") else ""
        qty = r.get("qty")
        money = r.get("money")
        count = r.get("_count", 1)

        t.add_row(
            format_ts_display(r.get("ts"), r.get("ts_raw")),
            str(r.get("event_type") or ""),
            f"x{count}" if count > 1 else "",
            src,
            dst,
            str(r.get("item") or ""),
            f"{int(qty):,}".replace(",", " ") if qty is not None else "",
            format_money_ro(int(money)) if money is not None else "",
        )

    console.print(t)

    footer = "Try: collapse=0, limit=100, from=..., to=..., type=..., item=..."
    console.print(Panel(footer, title="FOOTER", expand=False))
