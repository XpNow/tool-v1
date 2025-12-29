from datetime import timedelta
from rich.console import Console
from rich.panel import Panel

from .db import get_db
from .util import parse_iso_maybe, render_event_line, last_known_location_from_chain

console = Console(force_terminal=True)


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
HARD_STOP_TYPES = {"remat", "showroom_buy", "showroom_sell", "vehicle_buy_showroom", "vehicle_sell_remat", "vehicle_sell_to_player", "drop_item"}  # stop chain here


def _event_dt(ev):
    return parse_iso_maybe(ev["ts"]) if ev["ts"] else None


def build_flow(start_id: str, depth: int = 4, direction: str = "out", window_minutes: int = 120, item_filter: str | None = None):
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

    # Bidirectional helper: run two passes and merge chains (no guessing; only labeled).
    if direction == "both":
        out_chains = build_flow(start_id, depth=depth, direction="out", window_minutes=window_minutes, item_filter=item_filter)
        in_chains = build_flow(start_id, depth=depth, direction="in", window_minutes=window_minutes, item_filter=item_filter)
        # tag chains with direction for rendering (tuple: (dir, chain))
        merged = []
        for c in out_chains:
            merged.append(("out", c))
        for c in in_chains:
            merged.append(("in", c))
        return merged

    conn = get_db()
    cur = conn.cursor()

        # Pull all relevant event types with canonical fields
    sql = f"""
        SELECT
            id,
            ts, ts_raw,
            event_type,
            src_id, src_name,
            dst_id, dst_name,
            item, qty,
            money,
            container,
            raw_log_id
        FROM events
        WHERE event_type IN ({",".join(["?"] * len(FLOW_EVENT_TYPES))})
    """

    params = list(FLOW_EVENT_TYPES)
    if item_filter:
        sql += " AND item LIKE ?"
        params.append(f"%{item_filter}%")

    sql += """
        ORDER BY
            CASE WHEN ts IS NULL THEN 1 ELSE 0 END,
            ts ASC,
            raw_log_id ASC,
            id ASC
    """

    rows = cur.execute(sql, params).fetchall()
    conn.close()

    # Build adjacency indexes
    by_src = {}
    by_dst = {}
    for ev in rows:
        if ev["src_id"]:
            by_src.setdefault(str(ev["src_id"]), []).append(ev)
        if ev["dst_id"]:
            by_dst.setdefault(str(ev["dst_id"]), []).append(ev)

    # Ensure adjacency lists are time-sorted deterministically
    def sort_key(ev):
        dt = _event_dt(ev)
        # NULL ts events last; then stable tiebreakers
        return (1 if dt is None else 0, dt, ev["raw_log_id"] or 0, ev["id"])

    for k in list(by_src.keys()):
        by_src[k].sort(key=sort_key)
    for k in list(by_dst.keys()):
        by_dst[k].sort(key=sort_key)

    window = timedelta(minutes=int(window_minutes))
    chains = []

    def ok_time(prev_dt, cur_dt):
        # Cannot extend chain through unknown time events
        if cur_dt is None:
            return False
        if prev_dt is None:
            return True
        if direction == "out":
            return (cur_dt >= prev_dt) and (cur_dt - prev_dt <= window)
        else:
            return (cur_dt <= prev_dt) and (prev_dt - cur_dt <= window)

    def is_stop_event(ev):
        if ev["event_type"] in CONTAINER_TYPES:
            return True
        if ev["event_type"] in HARD_STOP_TYPES:
            return True
        return False

    def next_node(ev):
        return str(ev["dst_id"]) if direction == "out" else str(ev["src_id"])

    def relevant(ev, mode, item_name):
        et = ev["event_type"]
        if mode == "money":
            return et in MONEY_TYPES
        if mode == "item":
            return et in ITEM_TYPES and (ev["item"] or "").strip() == (item_name or "").strip()
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

            # Decide continuity mode based on first event
            new_mode = mode
            new_item = item_name
            if new_mode is None:
                if ev["event_type"] in MONEY_TYPES:
                    new_mode = "money"
                elif ev["event_type"] in ITEM_TYPES:
                    new_mode = "item"
                    new_item = (ev["item"] or "").strip()
                else:
                    new_mode = None

            if not relevant(ev, new_mode, new_item):
                continue

            path.append(ev)

            # Stop conditions (container/remat/showroom): endpoint is explicit, do not extend
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
    

        # If we found nothing, but there ARE direct events for the start ID,
    # return one-hop chains (evidence exists, trail ends here).
    if not chains:
        direct = by_src.get(sid, []) if direction == "out" else by_dst.get(sid, [])
        for ev in direct:
            ev_dt = _event_dt(ev)
            if ev_dt is None:
                continue  # cannot use unknown timestamps in flow chains
            chains.append([ev])

    # de-dup by event id sequence
    seen = set()
    uniq = []
    for c in chains:
        sig = tuple(e["id"] for e in c)
        if sig not in seen:
            seen.add(sig)
            uniq.append(c)
    return uniq

def _line(ev):
    return render_event_line(dict(ev))


from rich.console import Console
from rich.panel import Panel

console = Console()

def render_flow(start_id: str, chains, direction: str):
    title = f"FLOW — {direction.upper()} — start ID {start_id}"

    if not chains:
        console.print(Panel("No flow chains found.", title=title))
        return

    out: list[str] = []
    for i, chain_entry in enumerate(chains, 1):
        chain_dir = direction
        chain = chain_entry
        if direction.lower() == "both" and isinstance(chain_entry, tuple) and len(chain_entry) == 2:
            chain_dir, chain = chain_entry

        out.append(f"--- CHAIN #{i} ---")

        for ev in chain:
            try:
                out.append(_line(ev))
            except Exception:
                out.append(f"{ev.get('ts') or ev.get('ts_raw') or ''} - {ev.get('event_type')}")

        loc = last_known_location_from_chain([dict(e) for e in chain], chain_dir)
        out.append("LAST KNOWN LOCATION:")
        out.append(f"{loc}")
        out.append("")

    console.print(Panel("\n".join(out).rstrip(), title=title))
