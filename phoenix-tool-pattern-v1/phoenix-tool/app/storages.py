from __future__ import annotations

from collections import defaultdict

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .db import get_db
from .util import format_money_ro, build_warning_lines

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
        "SELECT event_type, container, item, qty, timestamp_quality "
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

    return containers_meta, balances, totals_in, totals_out, rows


def render_storages(pid: str, container_filter: str | None = None, ts_from: str | None = None, ts_to: str | None = None):
    containers_meta, balances, totals_in, totals_out, rows = compute_storage_balances(
        pid=pid,
        container_filter=container_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )

    if not containers_meta:
        console.print(Panel(f"No container_put/container_remove events found for ID {pid}", title="STORAGES"))
        return

    rel = 0
    unk_qty = 0
    unk_container = 0
    for r in rows:
        if (r["timestamp_quality"] or "").upper() == "RELATIVE":
            rel += 1
        if r["item"] and r["qty"] is None:
            unk_qty += 1
        if r["event_type"] in ("container_put", "container_remove") and not (r["container"] or "").strip():
            unk_container += 1

    negative_count = sum(
        1
        for container_items in balances.values()
        for qty in container_items.values()
        if qty < 0
    )

    warnings = build_warning_lines(
        relative_count=rel,
        unknown_qty_count=unk_qty,
        unknown_container_count=unk_container,
        negative_storage_count=negative_count,
    )

    header = [
        "[bold]STORAGES — pattern view[/bold]",
        f"ID: {pid}",
        f"Window: {ts_from or 'ALL'} → {ts_to or 'ALL'}",
        f"Containers: {len(containers_meta)}",
        "Warnings: " + " | ".join(warnings),
    ]
    console.print(Panel("\n".join(header), expand=False))

    total_items = sum(len(items) for items in balances.values())
    total_puts = sum(meta["puts"] for meta in containers_meta.values())
    total_removes = sum(meta["removes"] for meta in containers_meta.values())

    pattern = [
        f"• Containers touched: {len(containers_meta)}",
        f"• Items tracked: {total_items}",
        f"• Total events: puts={total_puts}, removes={total_removes}",
    ]
    console.print(Panel("\n".join(pattern), title="PATTERN", expand=False))

    grouped = []
    top_containers = sorted(
        containers_meta.items(),
        key=lambda kv: (kv[1]["puts"] + kv[1]["removes"]),
        reverse=True,
    )[:10]
    if top_containers:
        grouped.append(
            "• Containers: "
            + ", ".join([f"{c} ({m['puts'] + m['removes']})" for c, m in top_containers])
        )
    if grouped:
        console.print(Panel("\n".join(grouped), title="GROUPED SUMMARY", expand=False))

    cont_table = Table(title=f"CONTAINERS — ID {pid}", show_lines=True)
    cont_table.add_column("Container")
    cont_table.add_column("Puts", justify="right")
    cont_table.add_column("Removes", justify="right")
    for c, meta in sorted(containers_meta.items(), key=lambda kv: (kv[1]["puts"] + kv[1]["removes"]), reverse=True):
        cont_table.add_row(c, str(meta["puts"]), str(meta["removes"]))
    console.print(cont_table)

    evidence = Table(title="EVIDENCE — current balances", show_lines=True)
    evidence.add_column("Container")
    evidence.add_column("Item")
    evidence.add_column("Balance", justify="right")
    evidence.add_column("Total In", justify="right")
    evidence.add_column("Total Out", justify="right")

    for container, items in sorted(balances.items(), key=lambda kv: kv[0].lower()):
        for item, balance in sorted(items.items(), key=lambda kv: abs(kv[1]), reverse=True):
            total_in = totals_in[container].get(item, 0)
            total_out = totals_out[container].get(item, 0)
            evidence.add_row(container, item, str(balance), str(total_in), str(total_out))

    console.print(evidence)

    if negative_count:
        anomalies = Table(title="ANOMALIES — negative balances (missing history likely)", show_lines=True)
        anomalies.add_column("Container")
        anomalies.add_column("Item")
        anomalies.add_column("Balance", justify="right")
        for container, items in sorted(balances.items(), key=lambda kv: kv[0].lower()):
            for item, balance in sorted(items.items(), key=lambda kv: kv[1]):
                if balance < 0:
                    anomalies.add_row(container, item, str(balance))
        console.print(anomalies)


    if container_filter:
        matched = [c for c in balances.keys() if container_filter.lower() in c.lower()]
        if not matched:
            console.print(Panel(f"No containers matched filter: {container_filter}", title="STORAGES"))
            return

        for c in matched:
            _render_one_container(pid, c, balances[c], totals_in[c], totals_out[c])

    footer = ["Next: refine with container=..., from=..., to=..., collapse=0"]
    console.print(Panel("\n".join(footer), expand=False))


def _render_one_container(
    pid: str,
    container: str,
    item_balances: dict[str, int],
    total_in: dict[str, int],
    total_out: dict[str, int],
):
    pos = [(item, qty) for item, qty in item_balances.items() if qty > 0]
    neg = [(item, qty) for item, qty in item_balances.items() if qty < 0]

    pos.sort(key=lambda x: abs(x[1]), reverse=True)
    neg.sort(key=lambda x: abs(x[1]), reverse=True)

    t = Table(title=f"CURRENT CONTENTS — {container} (ID {pid})", show_lines=True)
    t.add_column("Item")
    t.add_column("Balance", justify="right")
    t.add_column("Total In", justify="right")
    t.add_column("Total Out", justify="right")

    for item, qty in pos:
        # if item is money-like, show money formatting too
        if item.lower().strip() == "bani murdari":
            t.add_row(item, f"{qty} ({format_money_ro(qty)})", str(total_in.get(item, 0)), str(total_out.get(item, 0)))
        else:
            t.add_row(item, str(qty), str(total_in.get(item, 0)), str(total_out.get(item, 0)))

    if not pos:
        t.add_row("(none)", "0", "0", "0")

    console.print(t)

    if neg:
        an = Table(title=f"ANOMALIES — negative balances in {container}", show_lines=True)
        an.add_column("Item")
        an.add_column("Balance", justify="right")
        for item, qty in neg:
            an.add_row(item, str(qty))
        console.print(an)
