import re
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from .db import get_db
from .util import format_ts_display, format_money_ro, actor_label

console = Console(force_terminal=True)

def rv(r, k, default=None):
    """Read value from sqlite3.Row or dict safely."""
    try:
        return r[k]
    except Exception:
        try:
            return r.get(k, default)  # type: ignore[attr-defined]
        except Exception:
            return default

def has_key(r, k):
    try:
        _ = r[k]
        return True
    except Exception:
        try:
            return k in r.keys()  # sqlite3.Row has keys()
        except Exception:
            return False

def search_events(
    ids=None,
    between_ids=None,
    name=None,
    item=None,
    event_type=None,
    min_money=None,
    max_money=None,
    ts_from: str | None = None,
    ts_to: str | None = None,
    limit=500,
):
    conn = get_db()
    cur = conn.cursor()

    where = []
    params = []

    if ids:
        qs = ",".join(["?"] * len(ids))
        where.append(f"(src_id IN ({qs}) OR dst_id IN ({qs}))")
        params.extend(ids)
        params.extend(ids)

    if between_ids and len(between_ids) == 2:
        a, b = between_ids
        where.append("((src_id=? AND dst_id=?) OR (src_id=? AND dst_id=?))")
        params.extend([a, b, b, a])

    if name:
        where.append("(src_name LIKE ? OR dst_name LIKE ?)")
        params.extend([f"%{name}%", f"%{name}%"])

    if item:
        where.append("item LIKE ?")
        params.append(f"%{item}%")

    if event_type:
        where.append("event_type = ?")
        params.append(event_type)

    if min_money is not None:
        where.append("money >= ?")
        params.append(min_money)

    if max_money is not None:
        where.append("money <= ?")
        params.append(max_money)

    if ts_from:
        where.append("ts >= ?")
        params.append(ts_from)
    if ts_to:
        where.append("ts <= ?")
        params.append(ts_to)

    sql = """
        SELECT ts, ts_raw, event_type, src_id, src_name, dst_id, dst_name, item, qty, money, container
        FROM events
    """
    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY (ts IS NULL) ASC, ts ASC, id ASC LIMIT ?"
    params.append(int(limit))

    rows = cur.execute(sql, params).fetchall()
    conn.close()
    return rows


def _count_warnings(rows):
    rel = 0
    unk_qty = 0
    unk_container = 0

    rel_re = re.compile(r"\b(today|yesterday)\b", re.I)

    for r in rows:
        ts_raw = (rv(r, "ts_raw") or "")
        if isinstance(ts_raw, str) and rel_re.search(ts_raw):
            rel += 1

        it = (rv(r, "item") or "")
        qty = rv(r, "qty")
        if it and qty is None:
            unk_qty += 1

        et = rv(r, "event_type")
        container = (rv(r, "container") or "")
        if et in ("container_put", "container_remove") and not container:
            unk_container += 1

    out = []
    if rel:
        out.append(f"{rel} RELATIVE timestamps")
    if unk_qty:
        out.append(f"{unk_qty} UNKNOWN qty")
    if unk_container:
        out.append(f"{unk_container} UNKNOWN container")
    return out


def _top_counts(rows, key, limit=5):
    from collections import Counter
    c = Counter()
    for r in rows:
        v = rv(r, key)
        if v:
            c[str(v)] += 1
    return c.most_common(limit)


def _top_items(rows, limit=5):
    from collections import Counter
    c = Counter()
    for r in rows:
        it = (rv(r, "item") or "").strip()
        if it:
            c[it] += 1
    return c.most_common(limit)


def _money_totals_for_focus(rows, focus_id: str):
    out_total = 0
    in_total = 0
    fid = str(focus_id)

    for r in rows:
        m = rv(r, "money")
        if m is None:
            continue
        if str(rv(r, "src_id") or "") == fid:
            out_total += int(m)
        if str(rv(r, "dst_id") or "") == fid:
            in_total += int(m)

    return out_total, in_total


def _partner_counts_for_focus(rows, focus_id: str, limit=8):
    from collections import Counter
    c = Counter()
    fid = str(focus_id)

    for r in rows:
        s = str(rv(r, "src_id") or "")
        d = str(rv(r, "dst_id") or "")
        if s == fid and d:
            c[d] += 1
        elif d == fid and s:
            c[s] += 1

    return c.most_common(limit)


def _between_summaries(rows, a: str, b: str, topn=5):
    from collections import Counter
    a = str(a)
    b = str(b)

    out_items = Counter()
    in_items = Counter()
    out_money = 0
    in_money = 0

    for r in rows:
        s = str(rv(r, "src_id") or "")
        d = str(rv(r, "dst_id") or "")
        it = (rv(r, "item") or "").strip()
        qty = rv(r, "qty")
        m = rv(r, "money")

        if s == a and d == b:
            if it and qty is not None:
                out_items[it] += int(qty)
            if m is not None:
                out_money += int(m)
        elif s == b and d == a:
            if it and qty is not None:
                in_items[it] += int(qty)
            if m is not None:
                in_money += int(m)

    return out_items.most_common(topn), in_items.most_common(topn), out_money, in_money


def render_search(rows, meta: dict | None = None):
    meta = meta or {}
    query = meta.get("query") or ""
    window = meta.get("window") or "ALL"
    limit = meta.get("limit")
    title = meta.get("title") or "SEARCH — pattern view"
    shown = len(rows)
    matched = meta.get("matched") or shown

    warnings = _count_warnings(rows)

    hdr_lines = [
        f"[bold]{title}[/bold]",
        f"Query: {query}" if query else "Query: (none)",
        f"Window: {window}",
        f"Matched: {matched} events | Showing: {shown}" + (f" (limit={limit})" if limit is not None else ""),
    ]
    if warnings:
        hdr_lines.append("Warnings: " + " | ".join(warnings))

    console.print(Panel("\n".join(hdr_lines), expand=False))

    focus_id = meta.get("focus_id")
    between_ids = meta.get("between_ids")

    pat = []
    top_types = _top_counts(rows, "event_type", limit=5)
    if top_types:
        pat.append("• Top types: " + ", ".join([f"{k} ({v})" for k, v in top_types]))

    if between_ids and len(between_ids) == 2:
        a, b = between_ids
        out_items, in_items, out_money, in_money = _between_summaries(rows, a, b, topn=5)
        if out_items:
            pat.append(f"• OUT {a} → {b}: " + ", ".join([f"{it} {qty:,}".replace(",", " ") for it, qty in out_items]))
        if in_items:
            pat.append(f"• IN  {a} ← {b}: " + ", ".join([f"{it} {qty:,}".replace(",", " ") for it, qty in in_items]))
        if out_money or in_money:
            pat.append(f"• Money: {a}→{b} {format_money_ro(out_money)} | {b}→{a} {format_money_ro(in_money)}")

    elif focus_id:
        partners = _partner_counts_for_focus(rows, focus_id, limit=6)
        if partners:
            pat.append("• Top partners: " + ", ".join([f"{pid} ({cnt})" for pid, cnt in partners]))

        out_money, in_money = _money_totals_for_focus(rows, focus_id)
        if out_money or in_money:
            pat.append(
                f"• Money: OUT {format_money_ro(out_money)} | IN {format_money_ro(in_money)} | NET {format_money_ro(in_money - out_money)}"
            )

        items = _top_items(rows, limit=5)
        if items:
            pat.append("• Top items: " + ", ".join([f"{it} ({cnt})" for it, cnt in items]))

    else:
        items = _top_items(rows, limit=5)
        if items:
            pat.append("• Top items: " + ", ".join([f"{it} ({cnt})" for it, cnt in items]))

    if pat:
        console.print(Panel("\n".join(pat), title="PATTERN", expand=False))

    t = Table(title="EVIDENCE", show_lines=True)
    t.add_column("Time")
    t.add_column("Type")
    t.add_column("From")
    t.add_column("To")
    t.add_column("Container")
    t.add_column("Item")
    t.add_column("Qty", justify="right")
    t.add_column("Money", justify="right")

    for r in rows:
        src_id = rv(r, "src_id")
        src_name = rv(r, "src_name")
        dst_id = rv(r, "dst_id")
        dst_name = rv(r, "dst_name")

        src = actor_label(src_name, src_id) if src_id or src_name else ""
        dst = actor_label(dst_name, dst_id) if dst_id or dst_name else ""

        container = str(rv(r, "container") or "")

        it = (rv(r, "item") or "")
        qty = rv(r, "qty")
        qty_cell = "?" if it and qty is None else (str(qty) if qty is not None else "")

        ts = rv(r, "ts")
        ts_raw = rv(r, "ts_raw")

        money = rv(r, "money")

        t.add_row(
            format_ts_display(ts, ts_raw),
            str(rv(r, "event_type") or ""),
            src,
            dst,
            container,
            str(it),
            qty_cell,
            format_money_ro(money) if money is not None else "",
        )

    console.print(t)

    footer = [
        "Next: refine with item=..., type=..., from=..., to=..., limit=..., view=full (planned), export=1 (planned)"
    ]
    console.print(Panel("\n".join(footer), expand=False))
