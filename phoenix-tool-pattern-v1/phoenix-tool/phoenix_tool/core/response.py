from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import get_db_path


@dataclass(frozen=True)
class WarningItem:
    code: str
    message: str
    count: int
    items: list | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_response(
    command: str,
    params: dict[str, Any],
    data: Any,
    warnings: list[WarningItem] | None = None,
    ok: bool = True,
) -> dict[str, Any]:
    return {
        "ok": ok,
        "command": command,
        "params": params,
        "warnings": [w.__dict__ for w in warnings or []],
        "data": data,
        "meta": {
            "version": "1.0",
            "db_path": str(get_db_path()),
            "generated_at": _now_iso(),
        },
    }


def build_error(command: str, params: dict[str, Any], message: str) -> dict[str, Any]:
    warn = WarningItem(code="ERROR", message=message, count=1)
    return build_response(command, params, data=None, warnings=[warn], ok=False)
