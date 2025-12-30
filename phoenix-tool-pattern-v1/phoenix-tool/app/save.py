import json

from rich.console import Console
from rich.panel import Panel

from .db import get_conn
from .util import utc_now_iso

console = Console()


def save_payload(tag: str, kind: str, payload, silent: bool = False):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO saved_findings(tag, kind, created_at, payload)
            VALUES (?, ?, ?, ?)
            """,
            (tag, kind, utc_now_iso(), json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
    if not silent:
        console.print(Panel(f"Saved as tag: {tag}\nKind: {kind}", title="SAVED"))


def load_payload(tag: str):
    with get_conn() as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT tag, kind, created_at, payload FROM saved_findings WHERE tag=?",
            (tag,),
        ).fetchone()
    if not row:
        return None
    return dict(row)
