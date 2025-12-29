from app.db import get_db


def test_relative_without_anchor_is_null(loaded_db):
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT ts, ts_raw, timestamp_quality
        FROM normalized_lines nl
        JOIN raw_logs rl ON rl.id = nl.raw_log_id
        WHERE rl.source_file LIKE '%logs_relative.txt'
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["ts"] is None
    assert row["timestamp_quality"] == "RELATIVE"
    assert "Today at" in (row["ts_raw"] or "")


def test_relative_with_filename_anchor_is_anchored(loaded_db):
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT ts, ts_raw, timestamp_quality
        FROM normalized_lines nl
        JOIN raw_logs rl ON rl.id = nl.raw_log_id
        WHERE rl.source_file LIKE '%logs_20.12.2025.txt'
          AND (ts_raw LIKE '%Today at%' OR ts_raw LIKE '%Yesterday at%')
        """
    ).fetchall()
    conn.close()

    assert rows
    for row in rows:
        assert row["ts"] is not None
        assert row["timestamp_quality"] == "ANCHORED"
