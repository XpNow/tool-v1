from __future__ import annotations

from pathlib import Path
from collections import defaultdict

from .models import Event
from .repository import fetch_events_for_id, fetch_identities
from .util import render_event_line, format_money_ro

BASE_DIR = Path(__file__).resolve().parents[1]
REPORTS_DIR = BASE_DIR / "output" / "reports"


def build_case_file(pid: str):
    """
    Generate an admin-friendly case folder for a single ID.
    Output is intentionally split into small files (their style),
    while keeping narrative quality for screenshots and reviews.
    """
    pid = str(pid)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    case_dir = REPORTS_DIR / f"ID_{pid}"
    case_dir.mkdir(parents=True, exist_ok=True)

    identities = fetch_identities(pid)
    events = fetch_events_for_id(pid)

    names = [r.name for r in identities if r.name]
    ips = [r.ip for r in identities if r.ip]

    money_in = sum(int(e.money or 0) for e in events if e.dst_id == pid)
    money_out = sum(int(e.money or 0) for e in events if e.src_id == pid)

    items_in = defaultdict(int)
    items_out = defaultdict(int)

    containers_used = set()
    partners = defaultdict(int)

    received_lines = []
    given_lines = []
    storage_lines = []
    banking_lines = []
    dropped_lines = []
    other_lines = []

    for e in events:
        et = e.event_type
        if e.src_id == pid and e.dst_id:
            partners[f"{e.dst_name or ''}[{e.dst_id}]".strip()] += 1
        elif e.dst_id == pid and e.src_id:
            partners[f"{e.src_name or ''}[{e.src_id}]".strip()] += 1

        if e.container:
            containers_used.add(e.container)

        if et in ("ofera_item", "container_put", "container_remove", "drop_item"):
            item = e.item or ""
            qty = int(e.qty or 0)
            if e.dst_id == pid:
                items_in[item] += qty
            if e.src_id == pid:
                items_out[item] += qty

        line = render_event_line(e)
        if et in ("bank_transfer", "ofera_bani", "phone_add", "phone_remove", "deposit", "withdraw", "bank_deposit", "bank_withdraw"):
            banking_lines.append(line)
        elif et in ("container_put", "container_remove"):
            storage_lines.append(line)
        elif et == "drop_item" and e.src_id == pid:
            dropped_lines.append(line)
        elif e.dst_id == pid:
            received_lines.append(line)
        elif e.src_id == pid:
            given_lines.append(line)
        else:
            other_lines.append(line)

    header = []
    header.append(f"ID: {pid}")
    if names:
        header.append("Names seen: " + ", ".join(dict.fromkeys(names)))
    if ips:
        header.append("IPs seen: " + ", ".join(dict.fromkeys(ips)))
    header.append("")
    header.append(f"Money IN:  {format_money_ro(money_in)}")
    header.append(f"Money OUT: {format_money_ro(money_out)}")
    header.append("")

    if items_in:
        header.append("Top items received:")
        for k, v in sorted(items_in.items(), key=lambda kv: kv[1], reverse=True)[:25]:
            header.append(f"  - {k}: {v}")
        header.append("")
    if items_out:
        header.append("Top items given:")
        for k, v in sorted(items_out.items(), key=lambda kv: kv[1], reverse=True)[:25]:
            header.append(f"  - {k}: {v}")
        header.append("")

    if containers_used:
        header.append("Containers used:")
        for c in sorted(containers_used)[:50]:
            header.append(f"  - {c}")
        header.append("")

    if partners:
        header.append("Top partners (by count):")
        for k, v in sorted(partners.items(), key=lambda kv: kv[1], reverse=True)[:25]:
            header.append(f"  - {k}: {v}")
        header.append("")

    (case_dir / "identity_and_totals.txt").write_text("\n".join(header).strip() + "\n", encoding="utf-8")
    (case_dir / "received.txt").write_text("\n".join(received_lines).strip() + "\n", encoding="utf-8")
    (case_dir / "given.txt").write_text("\n".join(given_lines).strip() + "\n", encoding="utf-8")
    (case_dir / "storage.txt").write_text("\n".join(storage_lines).strip() + "\n", encoding="utf-8")
    (case_dir / "banking.txt").write_text("\n".join(banking_lines).strip() + "\n", encoding="utf-8")
    (case_dir / "dropped.txt").write_text("\n".join(dropped_lines).strip() + "\n", encoding="utf-8")
    (case_dir / "other.txt").write_text("\n".join(other_lines).strip() + "\n", encoding="utf-8")

    return str(case_dir), events, identities
