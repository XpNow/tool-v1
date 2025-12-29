import json
from rich.console import Console
from rich.panel import Panel
from .db import get_db
from .util import utc_now_iso

console = Console()

def save_payload(tag: str, kind: str, payload):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO saved_findings(tag, kind, created_at, payload)
        VALUES (?, ?, ?, ?)
    """, (tag, kind, utc_now_iso(), json.dumps(payload, ensure_ascii=False)))
    conn.commit()
    conn.close()
    console.print(Panel(f"Saved as tag: {tag}\nKind: {kind}", title="SAVED"))

def load_payload(tag: str):
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT tag, kind, created_at, payload FROM saved_findings WHERE tag=?", (tag,)).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)
