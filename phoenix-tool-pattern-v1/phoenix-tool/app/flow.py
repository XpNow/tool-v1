from __future__ import annotations

from datetime import timedelta

from .models import Event
from .repository import fetch_flow_events
from .util import parse_iso_maybe


FLOW_EVENT_TYPES = (
    "bank_transfer",
    "bank_deposit",
    "bank_withdraw",
    "ofera_bani",
    "ofera_item",
    "container_put",
    "container_remove",
    "drop_item",
    "vehicle_buy_showroom",
    "vehicle_sell_remat",
    "vehicle_sell_to_player",
    "remat",
    "showroom_buy",
    "showroom_sell",
)

MONEY_TYPES = {"bank_transfer", "ofera_bani", "bank_deposit", "bank_withdraw"}
ITEM_TYPES = {"ofera_item"}
CONTAINER_TYPES = {"container_put", "container_remove"}
HARD_STOP_TYPES = {
    "remat",
    "showroom_buy",
    "showroom_sell",
    "vehicle_buy_showroom",
    "vehicle_sell_remat",
    "vehicle_sell_to_player",
    "drop_item",
}


def _event_dt(ev: Event):
    return parse_iso_maybe(ev.ts) if ev.ts else None


def build_flow(
    start_id: str,
    depth: int = 4,
    direction: str = "out",
    window_minutes: int = 120,
    item_filter: str | None = None,
):
    """
    Spec-aligned flow:
    - time-based (uses ts; NULL ts cannot extend a chain)
    - window-limited
    - continuity: money stays money, items stay same item
    - stop on containers + vehicle sale/remat/showroom
    - no guessing: if trail stops, endpoint UNKNOWN
    """
    sid = str(start_id)
    direction = direction.lower().strip()
    if direction not in ("out", "in", "both"):
        direction = "out"

    if direction == "both":
        out_chains = build_flow(start_id, depth=depth, direction="out", window_minutes=window_minutes, item_filter=item_filter)
        in_chains = build_flow(start_id, depth=depth, direction="in", window_minutes=window_minutes, item_filter=item_filter)
        merged = []
        for c in out_chains:
            merged.append(("out", c))
        for c in in_chains:
            merged.append(("in", c))
        return merged

    rows = fetch_flow_events(FLOW_EVENT_TYPES, item_filter)

    # Build adjacency indexes
    by_src: dict[str, list[Event]] = {}
    by_dst: dict[str, list[Event]] = {}
    for ev in rows:
        if ev.src_id:
            by_src.setdefault(str(ev.src_id), []).append(ev)
        if ev.dst_id:
            by_dst.setdefault(str(ev.dst_id), []).append(ev)

    def sort_key(ev: Event):
        dt = _event_dt(ev)
        return (1 if dt is None else 0, dt, ev.raw_log_id or 0, ev.id or 0)

    for k in list(by_src.keys()):
        by_src[k].sort(key=sort_key)
    for k in list(by_dst.keys()):
        by_dst[k].sort(key=sort_key)

    window = timedelta(minutes=int(window_minutes))
    chains: list[list[Event]] = []

    def ok_time(prev_dt, cur_dt):
        if cur_dt is None:
            return False
        if prev_dt is None:
            return True
        if direction == "out":
            return (cur_dt >= prev_dt) and (cur_dt - prev_dt <= window)
        return (cur_dt <= prev_dt) and (prev_dt - cur_dt <= window)

    def is_stop_event(ev: Event):
        if ev.event_type in CONTAINER_TYPES:
            return True
        if ev.event_type in HARD_STOP_TYPES:
            return True
        return False

    def next_node(ev: Event):
        return str(ev.dst_id) if direction == "out" else str(ev.src_id)

    def relevant(ev: Event, mode, item_name):
        et = ev.event_type
        if mode == "money":
            return et in MONEY_TYPES
        if mode == "item":
            return et in ITEM_TYPES and (ev.item or "").strip() == (item_name or "").strip()
        return True

    def dfs(node, d, path, last_dt, mode=None, item_name=None):
        if d >= depth:
            if path:
                chains.append(path.copy())
            return

        nxt_events = by_src.get(node, []) if direction == "out" else by_dst.get(node, [])
        extended = False

        for ev in nxt_events:
            ev_dt = _event_dt(ev)
            if not ok_time(last_dt, ev_dt):
                continue

            new_mode = mode
            new_item = item_name
            if new_mode is None:
                if ev.event_type in MONEY_TYPES:
                    new_mode = "money"
                elif ev.event_type in ITEM_TYPES:
                    new_mode = "item"
                    new_item = (ev.item or "").strip()
                else:
                    new_mode = None

            if not relevant(ev, new_mode, new_item):
                continue

            path.append(ev)

            if is_stop_event(ev):
                chains.append(path.copy())
                path.pop()
                extended = True
                continue

            nxt = next_node(ev)
            if not nxt or nxt == "None":
                chains.append(path.copy())
                path.pop()
                extended = True
                continue

            dfs(nxt, d + 1, path, ev_dt, new_mode, new_item)
            path.pop()
            extended = True

        if not extended and path:
            chains.append(path.copy())

    dfs(sid, 0, [], None)

    if not chains:
        direct = by_src.get(sid, []) if direction == "out" else by_dst.get(sid, [])
        for ev in direct:
            ev_dt = _event_dt(ev)
            if ev_dt is None:
                continue
            chains.append([ev])

    seen = set()
    uniq = []
    for c in chains:
        sig = tuple(e.id for e in c)
        if sig not in seen:
            seen.add(sig)
            uniq.append(c)
    return uniq
