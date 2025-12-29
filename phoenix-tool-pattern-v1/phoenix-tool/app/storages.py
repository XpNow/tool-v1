from __future__ import annotations

from collections import defaultdict

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .db import get_db
from .util import format_money_ro

console = Console(force_terminal=True)


def compute_storage_balances(
    pid: str,
    container_filter: str | None = None,
    ts_from: str | None = None,
    ts_to: str | None = None,
):
    """Compute container balances for one player.

    Balance model (best-effort):
      - container_put  => +qty
      - container_remove => -qty

    Returns:
      containers_meta: {container: {"puts": int, "removes": int}}
      balances: {container: {item: net_qty}}
    """

    conn = get_db()
    cur = conn.cursor()

    where = ["src_id = ?", "event_type IN ('container_put','container_remove')"]
    params: list[object] = [str(pid)]

    if container_filter:
        where.append("container LIKE ?")
        params.append(f"%{container_filter}%")

    if ts_from:
        where.append("ts >= ?")
        params.append(ts_from)
    if ts_to:
        where.append("ts <= ?")
        params.append(ts_to)

    sql = (
        "SELECT event_type, container, item, qty "
        "FROM events WHERE " + " AND ".join(where)
    )

    rows = cur.execute(sql, params).fetchall()
    conn.close()

    containers_meta: dict[str, dict[str, int]] = defaultdict(lambda: {"puts": 0, "removes": 0})
    balances: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    totals_in: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    totals_out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))


    for r in rows:
        et = (r["event_type"] or "").strip()
        container = (r["container"] or "UNKNOWN").strip()
        item = (r["item"] or "").strip()
        qty = r["qty"]
        if not item:
            continue
        if qty is None:
            # Can't compute balance without a qty; skip but still count the event.
            if et == "container_put":
                containers_meta[container]["puts"] += 1
            elif et == "container_remove":
                containers_meta[container]["removes"] += 1
            continue

        q = int(qty)
        if et == "container_put":
            containers_meta[container]["puts"] += 1
            balances[container][item] += q
            totals_in[container][item] += q
        elif et == "container_remove":
            containers_meta[container]["removes"] += 1
            balances[container][item] -= q
            totals_out[container][item] += q

    return containers_meta, balances, totals_in, totals_out


def render_storages(pid: str, container_filter: str | None = None, ts_from: str | None = None, ts_to: str | None = None):
    containers_meta, balances, totals_in, totals_out = compute_storage_balances(
        pid=pid,
        container_filter=container_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )

    # Containers list
    cont_table = Table(title=f"STORAGES — containers touched by ID {pid}", show_lines=True)
    cont_table.add_column("Container")
    cont_table.add_column("Puts", justify="right")
    cont_table.add_column("Removes", justify="right")

    # Sort by activity
    for c, meta in sorted(containers_meta.items(), key=lambda kv: (kv[1]["puts"] + kv[1]["removes"]), reverse=True):
        cont_table.add_row(c, str(meta["puts"]), str(meta["removes"]))

    if not containers_meta:
        console.print(Panel(f"No container_put/container_remove events found for ID {pid}", title="STORAGES"))
        return

    console.print(cont_table)

    # Pattern summary
    warn = []
    if ts_from or ts_to:
        pass
    console.print(Panel(f"ID: {pid}\nWindow: {ts_from or 'ALL'} → {ts_to or 'ALL'}\nContainers: {len(containers_meta)}", title="STORAGES — pattern view", expand=False))


    # If user requested a specific container filter, show its content state view
    if container_filter:
        # Choose exact matches first, else any matched.
        matched = [c for c in balances.keys() if container_filter.lower() in c.lower()]
        if not matched:
            console.print(Panel(f"No containers matched filter: {container_filter}", title="STORAGES"))
            return

        for c in matched:
            _render_one_container(pid, c, balances[c])


def _render_one_container(pid: str, container: str, item_balances: dict[str, int]):
    pos = [(item, qty) for item, qty in item_balances.items() if qty > 0]
    neg = [(item, qty) for item, qty in item_balances.items() if qty < 0]

    pos.sort(key=lambda x: abs(x[1]), reverse=True)
    neg.sort(key=lambda x: abs(x[1]), reverse=True)

    t = Table(title=f"CURRENT CONTENTS — {container} (ID {pid})", show_lines=True)
    t.add_column("Item")
    t.add_column("Balance", justify="right")

    for item, qty in pos:
        # if item is money-like, show money formatting too
        if item.lower().strip() == "bani murdari":
            t.add_row(item, f"{qty} ({format_money_ro(qty)})")
        else:
            t.add_row(item, str(qty))

    if not pos:
        t.add_row("(none)", "0")

    console.print(t)

    if neg:
        an = Table(title=f"ANOMALIES — negative balances in {container}", show_lines=True)
        an.add_column("Item")
        an.add_column("Balance", justify="right")
        for item, qty in neg:
            an.add_row(item, str(qty))
        console.print(an)
