import sqlite3
from app.db import DB_PATH

print("DB_PATH =", DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

tables = cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()

print("TABLES:", tables)
