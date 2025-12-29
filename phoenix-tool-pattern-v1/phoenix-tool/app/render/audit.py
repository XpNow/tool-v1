from __future__ import annotations

from rich.panel import Panel

from .common import console


def render_audit(output_path: str, total_groups: int, limit_groups: int):
    header = [
        "[bold]AUDIT — unparsed lines[/bold]",
        f"Groups (top {limit_groups}): {total_groups}",
    ]
    console.print(Panel("\n".join(header), expand=False))
    console.print(Panel(output_path, title="EVIDENCE (FILE)", expand=False))
    footer = "Review audit file for new parser rules."
    console.print(Panel(footer, title="FOOTER", expand=False))
