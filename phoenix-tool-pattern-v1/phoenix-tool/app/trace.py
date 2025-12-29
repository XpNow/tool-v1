from collections import deque
import os
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from .db import get_db
from .util import render_event_line

console = Console(force_terminal=True)

EDGE_TYPES = {"bank_transfer", "ofera_bani", "ofera_item"}

def trace(start_id: str, depth: int = 2, item_filter: str | None = None):
    sid = str(start_id)

    conn = get_db()
    cur = conn.cursor()

    params = []
    where = ["event_type IN ('bank_transfer','ofera_bani','ofera_item')"]
    if item_filter:
        where.append("item LIKE ?")
        params.append(f"%{item_filter}%")

    rows = cur.execute(f"""
        SELECT id, ts, ts_raw, event_type, src_id, src_name, dst_id, dst_name, item, qty, money, container
        FROM events
        WHERE {" AND ".join(where)}
        ORDER BY (ts IS NULL) ASC, ts ASC, id ASC
    """, params).fetchall()

    conn.close()

    # Build adjacency (undirected for trace)
    adj = {}
    for ev in rows:
        a = str(ev["src_id"]) if ev["src_id"] else None
        b = str(ev["dst_id"]) if ev["dst_id"] else None
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
    events = []
    for ev in rows:
        a = str(ev["src_id"]) if ev["src_id"] else None
        b = str(ev["dst_id"]) if ev["dst_id"] else None
        if a in node_set and b in node_set:
            events.append(ev)

    return events, node_set

def render_trace_story(start_id: str, events, nodes, depth: int, item_filter: str | None):
    title = f"TRACE — ID {start_id} — depth={depth}"
    if item_filter:
        title += f" — item~{item_filter}"

    lines = []
    lines.append(f"Nodes in trace: {len(nodes)} -> " + ", ".join(sorted(nodes))[:2000])

    for ev in events:
        t = ev["ts"] or ev["ts_raw"] or ""
        et = ev["event_type"]
        src = f"{ev['src_name']}[{ev['src_id']}]" if ev["src_id"] else ""
        dst = f"{ev['dst_name']}[{ev['dst_id']}]" if ev["dst_id"] else ""
        if et in ("bank_transfer", "ofera_bani"):
            m = ev["money"] or 0
            lines.append(f"{t} — {src} transferred ${m:,} to {dst}  ({et.upper()})")
        elif et == "ofera_item":
            qty = ev["qty"] or 0
            item = ev["item"] or ""
            lines.append(f"{t} — {src} gave {qty}× {item} to {dst}  (OFERA_ITEM)")
        else:
            lines.append(f"{t} — {et}")


    # Always write full trace to a file (terminal scrollback is limited)
    os.makedirs(os.path.join("output", "trace"), exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_item = (item_filter or "all").replace(" ", "_")[:40]
    out_path = os.path.join("output", "trace", f"trace_{start_id}_d{depth}_{safe_item}_{stamp}.txt")
    full_text = "\n".join(lines).strip() + "\n"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_text)

    # Print a trimmed preview in terminal
    preview_lines = lines[:200]
    if len(lines) > 200:
        preview_lines.append("… (trimmed) …")
        preview_lines.append(f"Full trace saved to: {out_path}")
    else:
        preview_lines.append(f"Saved to: {out_path}")

    console.print(Panel("\n".join(preview_lines).strip(), title=title))
