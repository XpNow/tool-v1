from pathlib import Path

from rich.console import Console

from app import report, search, storages, summary
from app.search import count_search_events, search_events


SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshots"


def _assert_snapshot(name: str, text: str):
    snap_path = SNAPSHOT_DIR / name
    expected = snap_path.read_text(encoding="utf-8")
    assert text == expected


def _capture_console(monkeypatch, module):
    console = Console(record=True, width=120, force_terminal=False)
    monkeypatch.setattr(module, "console", console)
    return console


def test_snapshot_search_id(loaded_db, monkeypatch):
    rows = search_events(ids=["101"], limit=5)
    matched = count_search_events(ids=["101"])
    meta = {
        "title": "SEARCH — pattern view",
        "query": "id=101 limit=5",
        "window": "ALL → ALL",
        "limit": 5,
        "collapse": "smart",
        "focus_id": "101",
        "matched": matched,
    }
    console = _capture_console(monkeypatch, search)
    search.render_search(rows, meta)
    _assert_snapshot("search_id.txt", console.export_text())


def test_snapshot_search_between(loaded_db, monkeypatch):
    rows = search_events(between_ids=["101", "202"], limit=5)
    matched = count_search_events(between_ids=["101", "202"])
    meta = {
        "title": "SEARCH — pattern view",
        "query": "between=101,202 limit=5",
        "window": "ALL → ALL",
        "limit": 5,
        "collapse": "smart",
        "between_ids": ["101", "202"],
        "matched": matched,
    }
    console = _capture_console(monkeypatch, search)
    search.render_search(rows, meta)
    _assert_snapshot("search_between.txt", console.export_text())


def test_snapshot_storages(loaded_db, monkeypatch):
    console = _capture_console(monkeypatch, storages)
    storages.render_storages("101")
    _assert_snapshot("storages.txt", console.export_text())


def test_snapshot_storages_container(loaded_db, monkeypatch):
    console = _capture_console(monkeypatch, storages)
    storages.render_storages("101", container_filter="Locker A")
    _assert_snapshot("storages_container.txt", console.export_text())


def test_snapshot_summary(loaded_db, monkeypatch):
    console = _capture_console(monkeypatch, summary)
    summary.summary_for_id("101")
    _assert_snapshot("summary.txt", console.export_text())


def test_report_files_created(loaded_db, monkeypatch, tmp_path):
    console = _capture_console(monkeypatch, report)
    case_dir = report.build_case_file("101")
    assert Path(case_dir).exists()
    assert (Path(case_dir) / "identity_and_totals.txt").exists()
    assert (Path(case_dir) / "received.txt").exists()
    assert (Path(case_dir) / "given.txt").exists()
    assert (Path(case_dir) / "storage.txt").exists()
    assert (Path(case_dir) / "banking.txt").exists()
    assert (Path(case_dir) / "dropped.txt").exists()
    assert (Path(case_dir) / "other.txt").exists()
