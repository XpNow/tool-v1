import sqlite3
from app.db import DB_PATH
from app.parse import RE_BANK_TRANSFER
from app.util import normalize_money

c = sqlite3.connect(DB_PATH)
c.row_factory = sqlite3.Row
cur = c.cursor()

cur.execute("DELETE FROM events")

rows = cur.execute(
    "SELECT raw_log_id, ts, ts_raw, text FROM normalized_lines WHERE text LIKE ?",
    ("%transferat%",),
).fetchall()

inserted = 0
for r in rows:
    m = RE_BANK_TRANSFER.search(r["text"])
    if not m:
        continue
    cur.execute(
        """
        INSERT INTO events(ts, ts_raw, event_type, src_id, src_name, dst_id, dst_name, money, raw_log_id)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            r["ts"], r["ts_raw"], "bank_transfer",
            m.group("src_id"), m.group("src_name").strip(),
            m.group("dst_id"), m.group("dst_name").strip(),
            normalize_money(m.group("amount")),
            r["raw_log_id"],
        ),
    )
    inserted += 1

c.commit()
print("inserted:", inserted)
print("events_total:", cur.execute("SELECT COUNT(1) FROM events").fetchone()[0])
