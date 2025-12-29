from __future__ import annotations

import re

from .response import WarningItem

_WARNING_MAP = {
    "RELATIVE timestamps": "RELATIVE_TIMESTAMPS",
    "UNKNOWN qty": "UNKNOWN_QTY",
    "UNKNOWN container": "UNKNOWN_CONTAINER",
    "negative storage likely missing history": "NEGATIVE_STORAGE",
}


def warnings_from_lines(lines: list[str]) -> list[WarningItem]:
    results: list[WarningItem] = []
    for line in lines:
        text = line.strip()
        if not text:
            continue
        count = 0
        if ":" in text:
            prefix, rest = text.split(":", 1)
            prefix = prefix.strip()
            match = re.search(r"(-?\d+)", rest)
            if match:
                count = int(match.group(1))
            code = _WARNING_MAP.get(prefix, prefix.upper().replace(" ", "_"))
            results.append(WarningItem(code=code, message=text, count=count))
        else:
            results.append(WarningItem(code=text.upper().replace(" ", "_"), message=text, count=0))
    return results
