from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import re

from .parse import matches_any_known_pattern
from .repository import fetch_normalized_lines, fetch_raw_log_sources

BASE_DIR = Path(__file__).resolve().parents[1]
AUDIT_DIR = BASE_DIR / "output" / "audit"

RE_NUM = re.compile(r"\d+")
RE_WS = re.compile(r"\s+")


def _signature(line: str) -> str:
    s = (line or "").strip()
    s = RE_WS.sub(" ", s)
    s = RE_NUM.sub("<n>", s)
    return s[:140]


def audit_unparsed(limit_groups: int = 50, sample_per_group: int = 8):
    """
    Collect lines from normalized_lines that do not match any known parser pattern.
    Groups them by a simplified signature so new log types can be added incrementally.
    """
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    rows = fetch_normalized_lines()
    file_map = fetch_raw_log_sources()

    groups = defaultdict(lambda: {"count": 0, "samples": []})

    for r in rows:
        line = (r["text"] or "").strip()
        if not line:
            continue
        if matches_any_known_pattern(line):
            continue

        sig = _signature(line)
        g = groups[sig]
        g["count"] += 1
        if len(g["samples"]) < sample_per_group:
            ts = r["ts"] or ""
            ts_raw = r["ts_raw"] or ""
            ctx = ts if ts else (f"(ts: {ts_raw})" if ts_raw else "")
            header = (
                f"{ctx} | raw_log_id={r['raw_log_id']} line_no={r['line_no']} | "
                f"file={file_map.get(r['raw_log_id'], '')}"
            )
            g["samples"].append(header + "\n" + line)

    sorted_groups = sorted(groups.items(), key=lambda kv: kv[1]["count"], reverse=True)[:limit_groups]

    out = AUDIT_DIR / "audit_unparsed.txt"
    with out.open("w", encoding="utf-8") as f:
        f.write(f"UNPARSED GROUPS: {len(sorted_groups)} (top {limit_groups})\n\n")
        for i, (sig, info) in enumerate(sorted_groups, 1):
            f.write("=" * 90 + "\n")
            f.write(f"GROUP #{i} | count={info['count']}\n")
            f.write(f"SIGNATURE: {sig}\n")
            f.write("-" * 90 + "\n")
            for s in info["samples"]:
                f.write(s + "\n\n")

    return str(out), len(sorted_groups), limit_groups
