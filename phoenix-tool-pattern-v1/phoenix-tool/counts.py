import sqlite3
from app.db import DB_PATH

c = sqlite3.connect(DB_PATH)
cur = c.cursor()

print("raw_logs", cur.execute("select count(*) from raw_logs").fetchone()[0])
print("normalized_lines", cur.execute("select count(*) from normalized_lines").fetchone()[0])
print("events", cur.execute("select count(*) from events").fetchone()[0])
print("identities", cur.execute("select count(*) from identities").fetchone()[0])
