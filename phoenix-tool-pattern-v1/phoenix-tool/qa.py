"""Quick regression checks.

Run:
  python .\qa.py

These checks are intentionally simple and fast.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from app import cli
from app import db as app_db
from app.db import init_db, get_db
from app.util import parse_int_ro, normalize_qty


def _assert(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def test_normalize_qty():
    _assert(normalize_qty("13.000.000") == 13000000, "qty dot separators failed")
    _assert(normalize_qty("(x7.825)") == 7825, "qty parens failed")
    _assert(normalize_qty("x39") == 39, "qty prefix x failed")
    _assert(parse_int_ro("x482.708") == 482708, "parse_int_ro x482.708 failed")


def test_db_schema():
    init_db()
    conn = get_db()
    cur = conn.cursor()
    # verify events table has needed columns
    cols = [r[1] for r in cur.execute("PRAGMA table_info(events)").fetchall()]
    for c in ("event_type", "src_id", "dst_id", "item", "qty", "container", "ts", "ts_raw", "timestamp_quality", "line_no", "source_file"):
        _assert(c in cols, f"events missing column: {c}")
    conn.close()


def test_cli_smoke():
    fixture_dir = Path(__file__).resolve().parent / "tests" / "fixtures"
    if not fixture_dir.exists():
        raise AssertionError(f"Fixture directory missing: {fixture_dir}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        app_db.DATA_DIR = tmpdir_path
        app_db.DB_PATH = tmpdir_path / "phoenix.db"

        init_db()
        rc = cli.main(["load", str(fixture_dir)])
        _assert(rc == 0, "load command failed")
        rc = cli.main(["normalize"])
        _assert(rc == 0, "normalize command failed")
        rc = cli.main(["parse"])
        _assert(rc == 0, "parse command failed")
        rc = cli.main(["search", "id=101", "limit=5"])
        _assert(rc == 0, "search command failed")
        rc = cli.main(["search", "between=101,202", "limit=5"])
        _assert(rc == 0, "between search command failed")
        rc = cli.main(["storages", "101"])
        _assert(rc == 0, "storages command failed")
        rc = cli.main(["summary", "101"])
        _assert(rc == 0, "summary command failed")
        rc = cli.main(["report", "101"])
        _assert(rc == 0, "report command failed")


def main():
    tests = [
        ("normalize_qty", test_normalize_qty),
        ("db_schema", test_db_schema),
        ("cli_smoke", test_cli_smoke),
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
