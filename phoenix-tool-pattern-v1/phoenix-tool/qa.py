"""Quick regression checks.

Run:
  python .\qa.py

These checks are intentionally simple and fast.
"""

from __future__ import annotations

from app.db import init_db, get_db
from app.util import normalize_qty


def _assert(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def test_normalize_qty():
    _assert(normalize_qty("13.000.000") == 13000000, "qty dot separators failed")
    _assert(normalize_qty("(x7.825)") == 7825, "qty parens failed")
    _assert(normalize_qty("x39") == 39, "qty prefix x failed")


def test_db_schema():
    init_db()
    conn = get_db()
    cur = conn.cursor()
    # verify events table has needed columns
    cols = [r[1] for r in cur.execute("PRAGMA table_info(events)").fetchall()]
    for c in ("event_type", "src_id", "dst_id", "item", "qty", "container", "ts", "ts_raw"):
        _assert(c in cols, f"events missing column: {c}")
    conn.close()


def main():
    tests = [
        ("normalize_qty", test_normalize_qty),
        ("db_schema", test_db_schema),
    ]

    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"[OK] {name}")
        except Exception as e:
            failed += 1
            print(f"[FAIL] {name}: {e}")

    if failed:
        raise SystemExit(1)
    print("All QA checks passed")


if __name__ == "__main__":
    main()
