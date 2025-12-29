from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from .db import get_db
from .util import format_money_ro

console = Console()

def summary_for_id(pid: str):
    pid = str(pid)
    conn = get_db()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT event_type, COUNT(*) c
        FROM events
        WHERE src_id=? OR dst_id=?
        GROUP BY event_type
        ORDER BY c DESC
    """, (pid, pid)).fetchall()

    money_out = cur.execute("""
        SELECT COALESCE(SUM(money),0) s
        FROM events
        WHERE src_id=? AND money IS NOT NULL
    """, (pid,)).fetchone()["s"]

    money_in = cur.execute("""
        SELECT COALESCE(SUM(money),0) s
        FROM events
        WHERE dst_id=? AND money IS NOT NULL
    """, (pid,)).fetchone()["s"]

    top_partners = cur.execute("""
        SELECT
          CASE WHEN src_id=? THEN dst_id ELSE src_id END partner_id,
          CASE WHEN src_id=? THEN dst_name ELSE src_name END partner_name,
          COUNT(*) c
        FROM events
        WHERE (src_id=? OR dst_id=?)
        GROUP BY partner_id, partner_name
        HAVING partner_id IS NOT NULL
        ORDER BY c DESC
        LIMIT 15
    """, (pid, pid, pid, pid)).fetchall()

    conn.close()

    console.print(Panel(f"Money IN: {format_money_ro(money_in)}\nMoney OUT: {format_money_ro(money_out)}", title="Money Overview"))

    table = Table(title=f"SUMMARY — ID {pid}", show_lines=True)
    table.add_column("Event Type")
    table.add_column("Count", justify="right")
    for r in rows:
        table.add_row(str(r["event_type"]), str(r["c"]))
    console.print(table)

    partners = Table(title="Top partners (by count)", show_lines=True)
    partners.add_column("Partner ID")
    partners.add_column("Partner Name")
    partners.add_column("Count", justify="right")
    for r in top_partners:
        partners.add_row(str(r["partner_id"] or ""), str(r["partner_name"] or ""), str(r["c"]))
    console.print(partners)
