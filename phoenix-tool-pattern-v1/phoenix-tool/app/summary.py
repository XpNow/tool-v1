from __future__ import annotations

from .repository import (
    fetch_events_for_id,
    fetch_event_type_counts_for_id,
    fetch_money_totals_for_id,
    fetch_top_partners,
)


def summary_for_id(pid: str, collapse: str | None = None):
    pid = str(pid)
    events = fetch_events_for_id(pid)
    event_counts = fetch_event_type_counts_for_id(pid)
    money_in, money_out = fetch_money_totals_for_id(pid)
    top_partners = fetch_top_partners(pid)
    return {
        "pid": pid,
        "events": events,
        "event_counts": event_counts,
        "money_in": money_in,
        "money_out": money_out,
        "top_partners": top_partners,
        "collapse": collapse,
    }
