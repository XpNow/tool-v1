import sqlite3
from datetime import datetime, timezone
from app.db import DB_PATH

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cols = [r[1] for r in cur.execute("PRAGMA table_info(raw_logs)").fetchall()]
print("raw_logs columns BEFORE:", cols)

if "loaded_at" not in cols:
    cur.execute("ALTER TABLE raw_logs ADD COLUMN loaded_at TEXT")
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cur.execute(
        "UPDATE raw_logs SET loaded_at=? WHERE loaded_at IS NULL OR loaded_at=''",
        (now,),
    )
    conn.commit()
    print("✅ loaded_at added + backfilled:", now)
else:
    print("loaded_at already exists.")

cols2 = [r[1] for r in cur.execute("PRAGMA table_info(raw_logs)").fetchall()]
print("raw_logs columns AFTER:", cols2)

conn.close()
