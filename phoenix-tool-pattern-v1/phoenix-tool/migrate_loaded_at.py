import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect("data/phoenix.db")
cur = conn.cursor()

# Inspect columns
cols = [r[1] for r in cur.execute("PRAGMA table_info(raw_logs)").fetchall()]
print("raw_logs columns BEFORE:", cols)

if "loaded_at" not in cols:
    print("Adding loaded_at column...")
    cur.execute("ALTER TABLE raw_logs ADD COLUMN loaded_at TEXT")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cur.execute(
        "UPDATE raw_logs SET loaded_at=? WHERE loaded_at IS NULL OR loaded_at=''",
        (now,),
    )
    conn.commit()
    print("loaded_at column added and backfilled.")
else:
    print("loaded_at column already exists.")

cols_after = [r[1] for r in cur.execute("PRAGMA table_info(raw_logs)").fetchall()]
print("raw_logs columns AFTER:", cols_after)

conn.close()
