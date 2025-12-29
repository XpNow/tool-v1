from __future__ import annotations

import re
from collections import Counter

from rich.console import Console

from ..models import Event
from ..util import build_warning_lines, parse_iso_maybe

console = Console(force_terminal=True)


def count_warnings(events: list[Event], negative_storage_count: int = 0) -> list[str]:
    rel = 0
    unk_qty = 0
    unk_container = 0

    rel_re = re.compile(r"\b(today|yesterday)\b", re.I)

    for ev in events:
        ts_quality = (ev.timestamp_quality or "").upper()
        ts_raw = ev.ts_raw or ""
        if ts_quality == "RELATIVE":
            rel += 1
        elif isinstance(ts_raw, str) and rel_re.search(ts_raw):
            rel += 1

        if ev.item and ev.qty is None:
            unk_qty += 1

        if ev.event_type in ("container_put", "container_remove") and not (ev.container or "").strip():
            unk_container += 1

    return build_warning_lines(
        relative_count=rel,
        unknown_qty_count=unk_qty,
        unknown_container_count=unk_container,
        negative_storage_count=negative_storage_count,
    )


def minute_key(ts: str | None, ts_raw: str | None) -> str | None:
    dt = parse_iso_maybe(ts or "")
    if dt is not None:
        return dt.replace(second=0, microsecond=0).isoformat()
    return None if not ts_raw else ts_raw.strip()


def collapse_events(events: list[Event], collapse: str | None) -> list[Event | dict]:
    if collapse is None or str(collapse).lower() in ("smart", "1", "true", "yes"):
        collapse = "smart"
    if str(collapse) in ("0", "false", "no"):
        return [
            {
                **ev.__dict__,
                "_count": 1,
            }
            for ev in events
        ]

    if collapse != "smart":
        return [
            {
                **ev.__dict__,
                "_count": 1,
            }
            for ev in events
        ]

    groups: dict[tuple, dict] = {}
    for ev in events:
        k = (
            minute_key(ev.ts, ev.ts_raw),
            ev.event_type,
            ev.src_id,
            ev.dst_id,
            ev.item,
            ev.qty,
            ev.money,
            ev.container,
        )
        if k not in groups:
            groups[k] = {
                **ev.__dict__,
                "_count": 0,
            }
        groups[k]["_count"] += 1

    return list(groups.values())


def top_counts(events: list[Event], attr: str, limit: int = 5) -> list[tuple[str, int]]:
    c: Counter[str] = Counter()
    for ev in events:
        val = getattr(ev, attr)
        if val:
            c[str(val)] += 1
    return c.most_common(limit)


def top_items(events: list[Event], limit: int = 5) -> list[tuple[str, int]]:
    c: Counter[str] = Counter()
    for ev in events:
        it = (ev.item or "").strip()
        if it:
            c[it] += 1
    return c.most_common(limit)
