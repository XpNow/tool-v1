import sqlite3
from app.db import DB_PATH

c = sqlite3.connect(DB_PATH)
c.row_factory = sqlite3.Row
cur = c.cursor()

print("events_total:", cur.execute("SELECT COUNT(1) FROM events").fetchone()[0])

row = cur.execute("""
    SELECT event_type, src_id, src_name, dst_id, dst_name, container
    FROM events
    LIMIT 5
""").fetchall()

print("sample events:")
for r in row:
    print(dict(r))

print("src_id non-null:", cur.execute("SELECT COUNT(1) FROM events WHERE src_id IS NOT NULL AND src_id != ''").fetchone()[0])
print("dst_id non-null:", cur.execute("SELECT COUNT(1) FROM events WHERE dst_id IS NOT NULL AND dst_id != ''").fetchone()[0])

print("identities_total:", cur.execute("SELECT COUNT(1) FROM identities").fetchone()[0])
