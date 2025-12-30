from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .db import get_conn

console = Console(force_terminal=True)


def rebuild_identities(silent: bool = False):
    """
    Stage 4: Identity resolution is OBSERVED, not assumed.
    Rebuild deterministically from parsed events (not raw logs).
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM identities")

        rows = cur.execute(
            """
            SELECT
                ts,
                raw_log_id,
                id as event_id,
                event_type,
                container,
                src_id, src_name,
                dst_id, dst_name
            FROM events
            ORDER BY
                CASE WHEN ts IS NULL THEN 1 ELSE 0 END,
                ts ASC,
                raw_log_id ASC,
                event_id ASC
            """
        ).fetchall()

        inserted = 0

        def observe(pid, name, ip=None):
            nonlocal inserted

            pid = (str(pid).strip() if pid is not None else "") or None
            name = (str(name).strip() if name is not None else "") or None
            ip = (str(ip).strip() if ip is not None else "") or None

            # Require at least ID or name (avoid junk)
            if pid is None and name is None:
                return

            row = cur.execute(
                """
                SELECT id FROM identities
                WHERE COALESCE(player_id,'') = COALESCE(?, '')
                  AND COALESCE(name,'')      = COALESCE(?, '')
                  AND COALESCE(ip,'')        = COALESCE(?, '')
                """,
                (pid, name, ip),
            ).fetchone()

            if row:
                cur.execute("UPDATE identities SET sightings = sightings + 1 WHERE id = ?", (row["id"],))
            else:
                cur.execute(
                    "INSERT INTO identities(player_id, name, ip, sightings) VALUES (?,?,?,1)",
                    (pid, name, ip),
                )
                inserted += 1

        for r in rows:
            # observe source
            observe(r["src_id"], r["src_name"], None)

            # observe destination, with IP if connect/disconnect
            ip = None
            if r["event_type"] in ("connect", "disconnect"):
                cand = (r["container"] or "").strip()
                # only accept likely IPs; ignore nil/empty
                if cand and cand.lower() != "nil" and "." in cand:
                    ip = cand.replace("**", "")

            observe(r["dst_id"], r["dst_name"], ip)

        conn.commit()

    if not silent:
        console.print(Panel(f"Identity rows inserted: {inserted}", title="IDENTITY REBUILD"))
    return inserted


def show_identity(query: str, as_data: bool = False):
    with get_conn() as conn:
        cur = conn.cursor()

        if query.isdigit():
            rows = cur.execute(
                """
                SELECT player_id, name, ip, sightings
                FROM identities
                WHERE player_id=?
                ORDER BY sightings DESC
                LIMIT 50
                """,
                (query,),
            ).fetchall()
            title = f"ID {query}"
        else:
            rows = cur.execute(
                """
                SELECT player_id, name, ip, sightings
                FROM identities
                WHERE name LIKE ?
                ORDER BY sightings DESC
                LIMIT 50
                """,
                (f"%{query}%",),
            ).fetchall()
            title = f"Query '{query}'"

    if as_data:
        return [
            {
                "player_id": r["player_id"],
                "name": r["name"],
                "ip": r["ip"],
                "sightings": int(r["sightings"] or 0),
            }
            for r in rows
        ]

    t = Table(title=f"IDENTITY — {title}", show_lines=True)
    t.add_column("Player ID")
    t.add_column("Name")
    t.add_column("IP")
    t.add_column("Sightings", justify="right")

    for r in rows:
        t.add_row(str(r["player_id"] or ""), str(r["name"] or ""), str(r["ip"] or ""), str(r["sightings"] or 0))

    console.print(t)
