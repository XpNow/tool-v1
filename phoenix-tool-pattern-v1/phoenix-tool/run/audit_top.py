import re
from collections import Counter
from pathlib import Path

p = Path("output/audit/audit_samples.txt")
text = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# Grab only the actual audit log lines (skip separators + headers)
lines = []
for ln in text:
    ln = ln.strip()
    if not ln or ln.startswith("[raw_log_id=") or ln.startswith("-"*10):
        continue
    lines.append(ln)

def normalize_signature(s: str) -> str:
    # reduce noise so similar lines group together
    s = re.sub(r"\d{1,2}:\d{2}(:\d{2})?", "<TIME>", s)
    s = re.sub(r"\b\d+\b", "<N>", s)
    s = re.sub(r"\[[0-9]+\]", "[<ID>]", s)
    s = re.sub(r"\([^)]+\)", "(...)", s)  # collapse parentheses
    s = re.sub(r"\s+", " ", s).strip()
    return s[:160]

sig = Counter(normalize_signature(x) for x in lines)

print("TOP 30 UNPARSED PATTERNS:\n")
for i, (k, v) in enumerate(sig.most_common(30), 1):
    print(f"{i:02d}. ({v}x) {k}")
