import sqlite3
from app.db import DB_PATH

c = sqlite3.connect(DB_PATH)
c.row_factory = sqlite3.Row
cur = c.cursor()

r = cur.execute("SELECT id, source_file, LENGTH(content) AS n, loaded_at FROM raw_logs LIMIT 5").fetchall()
for row in r:
    print(dict(row))
