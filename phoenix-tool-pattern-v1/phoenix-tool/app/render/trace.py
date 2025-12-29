from __future__ import annotations

from datetime import datetime
import os

from rich.panel import Panel

from ..models import Event
from ..util import render_event_line
from .common import console, count_warnings, top_counts, top_items


def render_trace(start_id: str, events: list[Event], nodes: set[str], depth: int, item_filter: str | None):
    title = f"TRACE — ID {start_id} — depth={depth}"
    if item_filter:
        title += f" — item~{item_filter}"

    warnings = count_warnings(events)

    header_lines = [
        f"[bold]{title}[/bold]",
        f"Nodes in trace: {len(nodes)}",
        f"Events: {len(events)}",
        "Warnings: " + " | ".join(warnings),
    ]
    console.print(Panel("\n".join(header_lines), expand=False))

    pattern = []
    top_types = top_counts(events, "event_type", limit=5)
    if top_types:
        pattern.append("• Top types: " + ", ".join([f"{k} ({v})" for k, v in top_types]))

    top_items_list = top_items(events, limit=5)
    if top_items_list:
        pattern.append("• Top items: " + ", ".join([f"{k} ({v})" for k, v in top_items_list]))

    if pattern:
        console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    lines = [f"Nodes: {', '.join(sorted(nodes))}"]
    for ev in events:
        lines.append(render_event_line(ev))

    os.makedirs(os.path.join("output", "trace"), exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_item = (item_filter or "all").replace(" ", "_")[:40]
    out_path = os.path.join("output", "trace", f"trace_{start_id}_d{depth}_{safe_item}_{stamp}.txt")
    full_text = "\n".join(lines).strip() + "\n"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_text)

    preview_lines = lines[:200]
    if len(lines) > 200:
        preview_lines.append("… (trimmed) …")
    preview_lines.append(f"Full trace saved to: {out_path}")

    console.print(Panel("\n".join(preview_lines).strip(), title="EVIDENCE", expand=False))

    footer = "Try: depth=3, item=..., collapse=0, export=1"
    console.print(Panel(footer, title="FOOTER", expand=False))
