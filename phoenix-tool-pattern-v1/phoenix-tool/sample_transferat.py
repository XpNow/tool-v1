import sqlite3
from app.db import DB_PATH

c = sqlite3.connect(DB_PATH)
c.row_factory = sqlite3.Row
cur = c.cursor()

rows = cur.execute(
    "SELECT text FROM normalized_lines WHERE text LIKE ? LIMIT 20",
    ("%transferat%",),
).fetchall()

print("SAMPLES:")
for r in rows:
    print("-", r["text"])
