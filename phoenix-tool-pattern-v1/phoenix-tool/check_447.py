import sqlite3

conn = sqlite3.connect("data/phoenix.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

rows = cur.execute("""
SELECT
    id,
    ts,
    event_type,
    src_id,
    src_name,
    dst_id,
    dst_name,
    money
FROM events
WHERE src_id = '447' OR dst_id = '447'
ORDER BY id ASC
LIMIT 20
""").fetchall()

print("ROWS FOUND:", len(rows))
for r in rows:
    print(dict(r))
