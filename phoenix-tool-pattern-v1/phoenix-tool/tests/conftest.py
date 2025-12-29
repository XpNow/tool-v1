from __future__ import annotations

import os
import time
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import db as app_db
from app.db import init_db
from app.ingest import load_logs
from app.normalize import normalize_all
from app.parse import parse_events


@pytest.fixture(autouse=True)
def force_utc_tz(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    try:
        time.tzset()
    except AttributeError:
        pass


@pytest.fixture
def temp_db(tmp_path):
    old_data_dir = app_db.DATA_DIR
    old_db_path = app_db.DB_PATH
    app_db.DATA_DIR = tmp_path
    app_db.DB_PATH = tmp_path / "phoenix.db"
    init_db()
    try:
        yield tmp_path
    finally:
        app_db.DATA_DIR = old_data_dir
        app_db.DB_PATH = old_db_path


@pytest.fixture
def loaded_db(temp_db):
    fixture_dir = Path(__file__).resolve().parent / "fixtures"
    load_logs(str(fixture_dir))
    normalize_all()
    parse_events()
    return temp_db
