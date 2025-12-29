from __future__ import annotations

from collections import deque

from .models import Event
from .repository import fetch_trace_events

EDGE_TYPES = {"bank_transfer", "ofera_bani", "ofera_item"}


def trace(start_id: str, depth: int = 2, item_filter: str | None = None):
    sid = str(start_id)
    rows = fetch_trace_events(EDGE_TYPES, item_filter)

    # Build adjacency (undirected for trace)
    adj: dict[str, set[str]] = {}
    for ev in rows:
        a = str(ev.src_id) if ev.src_id else None
        b = str(ev.dst_id) if ev.dst_id else None
        if not a or not b:
            continue
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    # BFS nodes within depth
    q = deque([(sid, 0)])
    seen = {sid}
    while q:
        node, d = q.popleft()
        if d >= depth:
            continue
        for nb in adj.get(node, []):
            if nb not in seen:
                seen.add(nb)
                q.append((nb, d + 1))

    # Events among nodes
    node_set = set(seen)
    events: list[Event] = []
    for ev in rows:
        a = str(ev.src_id) if ev.src_id else None
        b = str(ev.dst_id) if ev.dst_id else None
        if a in node_set and b in node_set:
            events.append(ev)

    return events, node_set
