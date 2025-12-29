from __future__ import annotations

from rich.panel import Panel

from ..models import Event, IdentityRecord
from ..util import format_money_ro
from .common import console, count_warnings, top_counts, top_items


def render_report(pid: str, case_dir: str, events: list[Event], identities: list[IdentityRecord]):
    warnings = count_warnings(events)

    header = [
        "[bold]REPORT — pattern view[/bold]",
        f"ID: {pid}",
        f"Events: {len(events)}",
        f"Identities: {len(identities)}",
        "Warnings: " + " | ".join(warnings),
    ]
    console.print(Panel("\n".join(header), expand=False))

    money_in = sum(int(e.money or 0) for e in events if e.dst_id == pid)
    money_out = sum(int(e.money or 0) for e in events if e.src_id == pid)

    pattern = [
        f"• Money IN: {format_money_ro(money_in)}",
        f"• Money OUT: {format_money_ro(money_out)}",
        f"• Money NET: {format_money_ro(money_in - money_out)}",
    ]

    top_types = top_counts(events, "event_type", limit=5)
    if top_types:
        pattern.append("• Top types: " + ", ".join([f"{k} ({v})" for k, v in top_types]))

    top_items_list = top_items(events, limit=5)
    if top_items_list:
        pattern.append("• Top items: " + ", ".join([f"{k} ({v})" for k, v in top_items_list]))

    console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    console.print(Panel(case_dir, title="EVIDENCE (FILES)", expand=False))

    footer = "Report files generated. Try: summary id, search id=..."
    console.print(Panel(footer, title="FOOTER", expand=False))
