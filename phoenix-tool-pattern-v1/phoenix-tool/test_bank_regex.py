import sqlite3
from app.db import DB_PATH
from app.parse import RE_BANK_TRANSFER

c = sqlite3.connect(DB_PATH)
c.row_factory = sqlite3.Row
cur = c.cursor()

rows = cur.execute(
    "SELECT text FROM normalized_lines WHERE text LIKE ? LIMIT 50",
    ("%transferat%",),
).fetchall()

hits = 0
for r in rows:
    s = r["text"]
    if RE_BANK_TRANSFER.search(s):
        hits += 1

print("sample_lines:", len(rows))
print("regex_hits:", hits)

# print first 5 that fail (so we can see what's different)
shown = 0
for r in rows:
    s = r["text"]
    if not RE_BANK_TRANSFER.search(s):
        print("FAIL:", s)
        shown += 1
        if shown >= 5:
            break
