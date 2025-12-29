from __future__ import annotations

from collections.abc import Iterable

from .db import get_db
from .models import Event, IdentityRecord, PartnerStat


EVENT_COLUMNS = (
    "id",
    "ts",
    "ts_raw",
    "timestamp_quality",
    "event_type",
    "src_id",
    "src_name",
    "dst_id",
    "dst_name",
    "item",
    "qty",
    "money",
    "container",
    "raw_log_id",
    "line_no",
    "source_file",
)


def _row_to_event(row) -> Event:
    return Event(
        id=row["id"] if "id" in row.keys() else None,
        ts=row["ts"],
        ts_raw=row["ts_raw"],
        timestamp_quality=row["timestamp_quality"] if "timestamp_quality" in row.keys() else None,
        event_type=row["event_type"],
        src_id=row["src_id"],
        src_name=row["src_name"],
        dst_id=row["dst_id"],
        dst_name=row["dst_name"],
        item=row["item"],
        qty=row["qty"],
        money=row["money"],
        container=row["container"],
        raw_log_id=row["raw_log_id"] if "raw_log_id" in row.keys() else None,
        line_no=row["line_no"] if "line_no" in row.keys() else None,
        source_file=row["source_file"] if "source_file" in row.keys() else None,
    )


def _fetch_events(sql: str, params: Iterable[object]) -> list[Event]:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(sql, list(params)).fetchall()
    conn.close()
    return [_row_to_event(row) for row in rows]


def build_search_query(
    ids=None,
    between_ids=None,
    name=None,
    item=None,
    event_type=None,
    min_money=None,
    max_money=None,
    ts_from: str | None = None,
    ts_to: str | None = None,
):
    where = []
    params: list[object] = []

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

    sql = f"SELECT {', '.join(EVENT_COLUMNS)} FROM events"
    if where:
        sql += " WHERE " + " AND ".join(where)

    return sql, params


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
    limit: int = 500,
) -> list[Event]:
    sql, params = build_search_query(
        ids=ids,
        between_ids=between_ids,
        name=name,
        item=item,
        event_type=event_type,
        min_money=min_money,
        max_money=max_money,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    sql += " ORDER BY (ts IS NULL) ASC, ts ASC, id ASC LIMIT ?"
    params.append(int(limit))
    return _fetch_events(sql, params)


def count_search_events(
    ids=None,
    between_ids=None,
    name=None,
    item=None,
    event_type=None,
    min_money=None,
    max_money=None,
    ts_from: str | None = None,
    ts_to: str | None = None,
) -> int:
    sql, params = build_search_query(
        ids=ids,
        between_ids=between_ids,
        name=name,
        item=item,
        event_type=event_type,
        min_money=min_money,
        max_money=max_money,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    count_sql = "SELECT COUNT(*) c FROM (" + sql + ")"

    conn = get_db()
    cur = conn.cursor()
    count = cur.execute(count_sql, params).fetchone()["c"]
    conn.close()
    return int(count)


def fetch_events_for_id(pid: str, ts_from: str | None = None, ts_to: str | None = None, limit: int | None = None) -> list[Event]:
    where = ["src_id=? OR dst_id=?"]
    params: list[object] = [pid, pid]
    if ts_from:
        where.append("ts >= ?")
        params.append(ts_from)
    if ts_to:
        where.append("ts <= ?")
        params.append(ts_to)

    sql = f"SELECT {', '.join(EVENT_COLUMNS)} FROM events WHERE " + " AND ".join(where)
    sql += " ORDER BY (ts IS NULL) ASC, ts ASC, id ASC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _fetch_events(sql, params)


def fetch_event_type_counts_for_id(pid: str) -> list[tuple[str, int]]:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT event_type, COUNT(*) c
        FROM events
        WHERE src_id=? OR dst_id=?
        GROUP BY event_type
        ORDER BY c DESC
        """,
        (pid, pid),
    ).fetchall()
    conn.close()
    return [(r["event_type"], r["c"]) for r in rows]


def fetch_money_totals_for_id(pid: str) -> tuple[int, int]:
    conn = get_db()
    cur = conn.cursor()
    money_out = cur.execute(
        """
        SELECT COALESCE(SUM(money),0) s
        FROM events
        WHERE src_id=? AND money IS NOT NULL
        """,
        (pid,),
    ).fetchone()["s"]

    money_in = cur.execute(
        """
        SELECT COALESCE(SUM(money),0) s
        FROM events
        WHERE dst_id=? AND money IS NOT NULL
        """,
        (pid,),
    ).fetchone()["s"]
    conn.close()
    return int(money_in or 0), int(money_out or 0)


def fetch_top_partners(pid: str, limit: int = 15) -> list[PartnerStat]:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
          CASE WHEN src_id=? THEN dst_id ELSE src_id END partner_id,
          CASE WHEN src_id=? THEN dst_name ELSE src_name END partner_name,
          COUNT(*) c
        FROM events
        WHERE (src_id=? OR dst_id=?)
        GROUP BY partner_id, partner_name
        HAVING partner_id IS NOT NULL AND partner_id != ?
        ORDER BY c DESC
        LIMIT ?
        """,
        (pid, pid, pid, pid, pid, int(limit)),
    ).fetchall()
    conn.close()
    return [PartnerStat(r["partner_id"], r["partner_name"], int(r["c"])) for r in rows]


def fetch_storage_events(
    pid: str,
    container_filter: str | None = None,
    ts_from: str | None = None,
    ts_to: str | None = None,
) -> list[Event]:
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
        f"SELECT {', '.join(EVENT_COLUMNS)} FROM events WHERE " + " AND ".join(where)
    )
    return _fetch_events(sql, params)


def fetch_flow_events(event_types: Iterable[str], item_filter: str | None = None) -> list[Event]:
    event_types = list(event_types)
    sql = f"SELECT {', '.join(EVENT_COLUMNS)} FROM events WHERE event_type IN ({','.join(['?'] * len(event_types))})"
    params: list[object] = list(event_types)
    if item_filter:
        sql += " AND item LIKE ?"
        params.append(f"%{item_filter}%")

    sql += " ORDER BY CASE WHEN ts IS NULL THEN 1 ELSE 0 END, ts ASC, raw_log_id ASC, id ASC"
    return _fetch_events(sql, params)


def fetch_trace_events(event_types: Iterable[str], item_filter: str | None = None) -> list[Event]:
    event_types = list(event_types)
    sql = f"SELECT {', '.join(EVENT_COLUMNS)} FROM events WHERE event_type IN ({','.join(['?'] * len(event_types))})"
    params: list[object] = list(event_types)
    if item_filter:
        sql += " AND item LIKE ?"
        params.append(f"%{item_filter}%")

    sql += " ORDER BY (ts IS NULL) ASC, ts ASC, id ASC"
    return _fetch_events(sql, params)


def fetch_identities(pid: str) -> list[IdentityRecord]:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT name, ip, sightings, player_id
        FROM identities
        WHERE player_id=?
        ORDER BY sightings DESC
        """,
        (pid,),
    ).fetchall()
    conn.close()
    return [IdentityRecord(r["player_id"], r["name"], r["ip"], int(r["sightings"])) for r in rows]


def fetch_directional_events(
    pid: str,
    direction: str,
    types: list[str],
    ts_from: str | None,
    ts_to: str | None,
    limit: int,
) -> list[Event]:
    where = ["event_type IN (%s)" % ",".join(["?"] * len(types))]
    params: list[object] = list(types)

    if direction == "out":
        where.append("src_id = ?")
        params.append(pid)
    else:
        where.append("dst_id = ?")
        params.append(pid)

    if ts_from:
        where.append("ts >= ?")
        params.append(ts_from)
    if ts_to:
        where.append("ts <= ?")
        params.append(ts_to)

    sql = f"SELECT {', '.join(EVENT_COLUMNS)} FROM events WHERE {' AND '.join(where)}"
    sql += " ORDER BY (ts IS NULL) ASC, ts ASC, id ASC LIMIT ?"
    params.append(int(limit))
    return _fetch_events(sql, params)


def fetch_normalized_lines():
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT raw_log_id, line_no, ts, ts_raw, text
        FROM normalized_lines
        ORDER BY (ts IS NULL) ASC, ts ASC, raw_log_id ASC, line_no ASC
        """
    ).fetchall()
    conn.close()
    return rows


def fetch_raw_log_sources() -> dict[int, str]:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT id, source_file FROM raw_logs").fetchall()
    conn.close()
    return {r["id"]: r["source_file"] for r in rows}
