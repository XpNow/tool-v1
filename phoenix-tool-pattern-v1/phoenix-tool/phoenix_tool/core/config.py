from __future__ import annotations

import os
from pathlib import Path


def get_db_path() -> Path:
    env_path = os.environ.get("PHOENIX_DB")
    if env_path:
        return Path(env_path).expanduser().resolve()
    base_dir = Path(__file__).resolve().parents[2]
    return base_dir / "data" / "phoenix.db"
