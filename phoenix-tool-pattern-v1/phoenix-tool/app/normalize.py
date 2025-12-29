import re
from datetime import datetime, timedelta, timezone
from rich.console import Console
from rich.panel import Panel
from .db import get_db

console = Console()

# Relative discord markers: — 9:40 PM   OR   — Today at 10:07 PM
RE_REL_TS = re.compile(
    r"—\s*(?:(Today|Yesterday)\s+at\s+)?(\d{1,2}):(\d{2})\s*(AM|PM)",
    re.I
)

# Relative 24h markers: — 13:07   OR   — Today at 13:07
RE_REL_24H = re.compile(
    r"—\s*(?:(Today|Yesterday)\s+at\s+)?(\d{1,2}):(\d{2})\b",
    re.I
)

# Absolute markers: — 20.12.2025 18:32  OR  — 2025-12-20 18:32
RE_ABS_TS_DMY = re.compile(r"—\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2})")
RE_ABS_TS_YMD = re.compile(r"—\s*(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})")

# Absolute markers (US-style slash): — 12/19/2025 12:02 AM
RE_ABS_TS_MDY12 = re.compile(
    r"—\s*(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)",
    re.I,
)

# Base date from filename (priority #1)
RE_FILE_DATE_DMY = re.compile(r"(\d{1,2})[.\-_](\d{1,2})[.\-_](\d{4})")
RE_FILE_DATE_YMD = re.compile(r"(\d{4})[.\-_](\d{1,2})[.\-_](\d{1,2})")

NOISE_EXACT = {
    "Freaks Logs",
    "PHOENIX LOGS",
    "APP",
    "Transfera Item",
    "Ofera Item",
    "Ofera Bani",
    "Depunere Banca",
    "Retragere Banca",
    "💵 Telefon",
    "🚘 Showroom",
    "🚗 Remat",
}

NOISE_PREFIXES = (
    "Made by ",
    "Made by Synked",
    "Made by zJu1C3",
    "FILE:",
    "RAW_LOG_ID:",
    "@",          # Discord mentions
    "<@",         # Discord mentions
)



def _iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _base_date_from_loaded_at(loaded_at: str | None) -> datetime | None:
    if not loaded_at:
        return None
    try:
        ts = loaded_at
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except Exception:
        return None


def _base_date_from_filename(source_file: str | None) -> datetime | None:
    """
    Priority base date: infer explicit date from filename if present.
    Supports: DD.MM.YYYY / DD-MM-YYYY / DD_MM_YYYY and YYYY-MM-DD / YYYY.MM.DD / YYYY_MM_DD
    """
    if not source_file:
        return None

    m = RE_FILE_DATE_DMY.search(source_file)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d, 0, 0, tzinfo=timezone.utc)
        except Exception:
            pass

    m = RE_FILE_DATE_YMD.search(source_file)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d, 0, 0, tzinfo=timezone.utc)
        except Exception:
            pass

    return None


def _parse_marker(ts_raw: str, base_date: datetime | None) -> str | None:
    """
    Parse marker line -> ISO timestamp (UTC Z) or None.
    Supports absolute (self-contained) and relative (needs base_date).
    """

    # Absolute: DD.MM.YYYY HH:MM
    m = RE_ABS_TS_DMY.search(ts_raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hh, mm = int(m.group(4)), int(m.group(5))
        try:
            return _iso_z(datetime(y, mo, d, hh, mm, tzinfo=timezone.utc))
        except Exception:
            return None

    # Absolute: YYYY-MM-DD HH:MM
    m = RE_ABS_TS_YMD.search(ts_raw)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hh, mm = int(m.group(4)), int(m.group(5))
        try:
            return _iso_z(datetime(y, mo, d, hh, mm, tzinfo=timezone.utc))
        except Exception:
            return None

    # Absolute: MM/DD/YYYY HH:MM AM/PM (common in Discord exports)
    m = RE_ABS_TS_MDY12.search(ts_raw)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hh, mm = int(m.group(4)), int(m.group(5))
        ap = m.group(6).upper()
        if ap == "PM" and hh != 12:
            hh += 12
        if ap == "AM" and hh == 12:
            hh = 0
        try:
            return _iso_z(datetime(y, mo, d, hh, mm, tzinfo=timezone.utc))
        except Exception:
            return None

    # Relative 12h: Today/Yesterday at 1:07 PM OR 1:07 PM
    m = RE_REL_TS.search(ts_raw)
    if m and base_date is not None:
        rel = (m.group(1) or "").lower()
        hh = int(m.group(2))
        mm = int(m.group(3))
        ap = m.group(4).upper()

        if ap == "PM" and hh != 12:
            hh += 12
        if ap == "AM" and hh == 12:
            hh = 0

        day = base_date.date()
        if rel == "yesterday":
            day = (base_date - timedelta(days=1)).date()

        try:
            dt = datetime(day.year, day.month, day.day, hh, mm, tzinfo=timezone.utc)
            return _iso_z(dt)
        except Exception:
            return None

    # Relative 24h: Today/Yesterday at 13:07 OR 13:07
    m = RE_REL_24H.search(ts_raw)
    if m and base_date is not None:
        rel = (m.group(1) or "").lower()
        hh = int(m.group(2))
        mm = int(m.group(3))

        day = base_date.date()
        if rel == "yesterday":
            day = (base_date - timedelta(days=1)).date()

        try:
            dt = datetime(day.year, day.month, day.day, hh, mm, tzinfo=timezone.utc)
            return _iso_z(dt)
        except Exception:
            return None

    return None


def normalize_all():
    conn = get_db()
    cur = conn.cursor()

    # rebuild deterministically
    cur.execute("DELETE FROM normalized_lines")

    # Support older DBs where raw_logs may not have loaded_at yet
    cols = {row[1] for row in cur.execute("PRAGMA table_info(raw_logs)").fetchall()}
    has_loaded_at = "loaded_at" in cols

    if has_loaded_at:
        raws = cur.execute(
            "SELECT id, source_file, loaded_at, content FROM raw_logs ORDER BY id ASC"
        ).fetchall()
    else:
        raws = cur.execute(
            "SELECT id, source_file, content FROM raw_logs ORDER BY id ASC"
        ).fetchall()

    inserted = 0

    for r in raws:
        raw_id = r["id"]
        source_file = r["source_file"]
        loaded_at = r["loaded_at"] if has_loaded_at else None

        # base date priority: filename -> loaded_at -> None
        base_dt = _base_date_from_filename(source_file) or _base_date_from_loaded_at(loaded_at)

        last_ts_raw = None
        last_ts_iso = None

        # normalized sequence line number (1..N per raw_log)
        norm_no = 0

        for _raw_ln_no, line in enumerate(r["content"].splitlines(), 1):
            s = line.strip()

            if set(s) <= {"="} and len(s) >= 10:
                continue

            if s.startswith("RAW_LOG_ID:") or s.startswith("FILE:"):
                continue


            if not s:
                continue

            # noise removal
            if s in NOISE_EXACT:
                continue
            if any(s.startswith(p) for p in NOISE_PREFIXES):
                continue

            # timestamp marker line updates context, not inserted
            if "—" in s and (
                RE_REL_TS.search(s)
                or RE_REL_24H.search(s)
                or RE_ABS_TS_DMY.search(s)
                or RE_ABS_TS_YMD.search(s)
                or RE_ABS_TS_MDY12.search(s)
            ):
                last_ts_raw = s
                last_ts_iso = _parse_marker(s, base_dt)  # may be None; ts_raw still kept
                continue

            # insert meaningful normalized line
            norm_no += 1
            cur.execute(
                """
                INSERT INTO normalized_lines(raw_log_id, line_no, ts, ts_raw, text)
                VALUES (?,?,?,?,?)
                """,
                (raw_id, norm_no, last_ts_iso, last_ts_raw, s),
            )
            inserted += 1

    conn.commit()
    conn.close()
    console.print(Panel(f"Normalized lines inserted: {inserted}", title="NORMALIZE"))
    return inserted

