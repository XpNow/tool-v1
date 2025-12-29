from __future__ import annotations

from rich.panel import Panel
from rich.table import Table

from ..models import StorageContainerSummary
from ..util import build_warning_lines
from .common import console


def render_storages(
    pid: str,
    container_filter: str | None,
    containers: list[StorageContainerSummary],
    warnings: list[str],
    negative_count: int,
):
    if not containers:
        console.print(Panel(f"No container_put/container_remove events found for ID {pid}", title="STORAGES"))
        return

    hdr = [
        "[bold]STORAGES — pattern view[/bold]",
        f"ID: {pid}",
        f"Filter: {container_filter or 'ALL'}",
        f"Containers: {len(containers)}",
        "Warnings: " + " | ".join(warnings),
    ]
    console.print(Panel("\n".join(hdr), expand=False))

    # Pattern summary
    total_items = sum(len(c.items) for c in containers)
    pattern = [
        f"• Containers: {len(containers)}",
        f"• Items tracked: {total_items}",
    ]
    console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    grouped: list[str] = []
    if negative_count:
        grouped.append(f"• Negative balances: {negative_count} (missing history likely)")
    if grouped:
        console.print(Panel("\n".join(grouped), title="GROUPED SUMMARY", expand=False))

    for container in containers:
        t = Table(title=f"CONTAINER: {container.container}", show_lines=True)
        t.add_column("Item")
        t.add_column("Current", justify="right")
        t.add_column("Total In", justify="right")
        t.add_column("Total Out", justify="right")

        for item in container.items:
            t.add_row(
                item.item,
                f"{item.current:,}".replace(",", " "),
                f"{item.total_in:,}".replace(",", " "),
                f"{item.total_out:,}".replace(",", " "),
            )
        console.print(t)

    footer = "Try: container=..., from=..., to=..."
    console.print(Panel(footer, title="FOOTER", expand=False))
