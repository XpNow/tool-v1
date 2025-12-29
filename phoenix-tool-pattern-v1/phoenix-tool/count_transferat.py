import sqlite3
from app.db import DB_PATH

c = sqlite3.connect(DB_PATH)
cur = c.cursor()

n = cur.execute(
    "SELECT COUNT(1) FROM normalized_lines WHERE text LIKE ?",
    ("%transferat%",),
).fetchone()[0]

print("normalized_lines containing 'transferat':", n)
