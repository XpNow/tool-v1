from __future__ import annotations

from datetime import datetime
import os

from rich.panel import Panel

from ..models import Event
from ..util import last_known_location_from_chain, render_event_line
from .common import console, count_warnings, top_counts, top_items


def render_flow(start_id: str, chains, direction: str):
    title = f"FLOW — {direction.upper()} — start ID {start_id}"

    if not chains:
        console.print(Panel("No flow chains found.", title=title))
        return

    events_flat: list[Event] = []
    for chain_entry in chains:
        chain = chain_entry
        if direction.lower() == "both" and isinstance(chain_entry, tuple) and len(chain_entry) == 2:
            chain = chain_entry[1]
        events_flat.extend(chain)

    warnings = count_warnings(events_flat)

    header = [
        f"[bold]{title}[/bold]",
        f"Chains: {len(chains)}",
        f"Events: {len(events_flat)}",
        "Warnings: " + " | ".join(warnings),
    ]
    console.print(Panel("\n".join(header), expand=False))

    pattern = []
    top_types = top_counts(events_flat, "event_type", limit=5)
    if top_types:
        pattern.append("• Top types: " + ", ".join([f"{k} ({v})" for k, v in top_types]))

    top_items_list = top_items(events_flat, limit=5)
    if top_items_list:
        pattern.append("• Top items: " + ", ".join([f"{k} ({v})" for k, v in top_items_list]))

    if pattern:
        console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    out: list[str] = []
    for i, chain_entry in enumerate(chains, 1):
        chain_dir = direction
        chain = chain_entry
        if direction.lower() == "both" and isinstance(chain_entry, tuple) and len(chain_entry) == 2:
            chain_dir, chain = chain_entry

        out.append(f"--- CHAIN #{i} ---")

        for ev in chain:
            out.append(render_event_line(ev))

        loc = last_known_location_from_chain(chain, chain_dir)
        out.append("LAST KNOWN LOCATION:")
        out.append(f"{loc}")
        out.append("")

    os.makedirs(os.path.join("output", "flow"), exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("output", "flow", f"flow_{start_id}_{direction}_{stamp}.txt")
    full_text = "\n".join(out).rstrip() + "\n"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_text)

    preview_lines = out[:200]
    if len(out) > 200:
        preview_lines.append("… (trimmed) …")
    preview_lines.append(f"Full flow saved to: {out_path}")

    console.print(Panel("\n".join(preview_lines).rstrip(), title="EVIDENCE", expand=False))

    footer = "Try: depth=5, window=120, direction=both, item=..., export=1"
    console.print(Panel(footer, title="FOOTER", expand=False))
