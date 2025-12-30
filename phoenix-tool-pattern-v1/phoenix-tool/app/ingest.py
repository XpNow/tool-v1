from pathlib import Path
from datetime import datetime, timezone
from rich.console import Console
from rich.panel import Panel

from .db import get_conn
from .util import sha1_text, utc_now_iso

console = Console()


def load_logs(path: str, silent: bool = False):
    """
    Stage 1: RAW INGESTION
    - Accepts a file OR a directory
    - Recursively loads *.txt files
    - Stores raw evidence unchanged
    - Deduplicates by content hash
    """

    p = Path(path)

    if not p.exists():
        if not silent:
            console.print(f"[red]Path not found:[/red] {path}")
        return 0

    # Resolve files deterministically
    if p.is_file():
        files = [p]
    else:
        files = sorted(p.rglob("*.txt"))

    if not files:
        if not silent:
            console.print("[yellow]No .txt log files found.[/yellow]")
        return 0

    inserted = 0
    skipped = 0

    with get_conn() as conn:
        cur = conn.cursor()

        for f in files:
            try:
                text = f.read_text(encoding="utf-8", errors="ignore")
            except Exception as e:
                if not silent:
                    console.print(f"[red]Failed to read[/red] {f}: {e}")
                continue

            h = sha1_text(text)

            # Deduplication by content hash (evidence safety)
            exists = cur.execute(
                "SELECT 1 FROM raw_logs WHERE content_hash=?",
                (h,),
            ).fetchone()

            if exists:
                skipped += 1
                continue

            cur.execute(
                """
                INSERT INTO raw_logs
                (source_file, content, content_hash, loaded_at)
                VALUES (?,?,?,?)
                """,
                (
                    str(f),
                    text,
                    h,
                    utc_now_iso(),
                ),
            )
            inserted += 1

        conn.commit()

    if not silent:
        console.print(
            Panel(
                f"Files loaded: {inserted}\nDuplicates skipped: {skipped}",
                title="RAW INGEST",
            )
        )

    return inserted
