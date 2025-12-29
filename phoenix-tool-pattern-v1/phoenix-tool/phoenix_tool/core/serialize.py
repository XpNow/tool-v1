from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from app.models import Event


def to_dict(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Event):
        return asdict(value)
    if isinstance(value, dict):
        return {k: to_dict(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_dict(v) for v in value]
    return value
