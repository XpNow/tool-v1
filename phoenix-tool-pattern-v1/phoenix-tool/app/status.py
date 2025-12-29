from collections import defaultdict
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .db import get_db


console = Console()


def show_status():
    """Read-only coverage view: raw logs, normalized lines, events by type."""
    conn = get_db()
    cur = conn.cursor()

    raw_n = cur.execute("SELECT COUNT(*) c FROM raw_logs").fetchone()["c"]
    norm_n = cur.execute("SELECT COUNT(*) c FROM normalized_lines").fetchone()["c"]
    ev_n = cur.execute("SELECT COUNT(*) c FROM events").fetchone()["c"]

    by_type = cur.execute(
        """
        SELECT event_type, COUNT(*) c
        FROM events
        GROUP BY event_type
        ORDER BY c DESC
        """
    ).fetchall()

    conn.close()

    console.print(
        Panel(
            f"Raw logs: {raw_n}\nNormalized lines: {norm_n}\nParsed events: {ev_n}",
            title="STATUS",
        )
    )

    t = Table(title="Events by type", show_lines=True)
    t.add_column("Event type")
    t.add_column("Count", justify="right")
    for r in by_type:
        t.add_row(str(r["event_type"]), str(r["c"]))
    console.print(t)
