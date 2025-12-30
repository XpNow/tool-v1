import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

_ENV_DB = os.environ.get("PHOENIX_DB")
DB_PATH = Path(_ENV_DB).expanduser().resolve() if _ENV_DB else DATA_DIR / "phoenix.db"


def _configure_conn(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    _configure_conn(conn)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT NOT NULL,
                content TEXT NOT NULL,
                content_hash TEXT NOT NULL UNIQUE,
                loaded_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS normalized_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_log_id INTEGER NOT NULL,
                line_no INTEGER NOT NULL,
                ts TEXT,
                ts_raw TEXT,
                timestamp_quality TEXT,
                text TEXT NOT NULL,
                FOREIGN KEY(raw_log_id) REFERENCES raw_logs(id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS identities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT,
                name TEXT,
                ip TEXT,
                sightings INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        # --- schema repair for older DBs (identities missing sightings) ---
        cols = {row[1] for row in cur.execute("PRAGMA table_info(identities)").fetchall()}
        if "sightings" not in cols:
            cur.execute("DROP TABLE IF EXISTS identities")
            cur.execute(
                """
                CREATE TABLE identities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id TEXT,
                    name TEXT,
                    ip TEXT,
                    sightings INTEGER NOT NULL DEFAULT 1
                )
                """
            )

        cols = {r[1] for r in cur.execute("PRAGMA table_info(raw_logs)").fetchall()}
        if "loaded_at" not in cols:
            cur.execute("ALTER TABLE raw_logs ADD COLUMN loaded_at TEXT")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                ts_raw TEXT,
                timestamp_quality TEXT,
                event_type TEXT NOT NULL,
                src_id TEXT,
                src_name TEXT,
                dst_id TEXT,
                dst_name TEXT,
                item TEXT,
                qty INTEGER,
                money INTEGER,
                container TEXT,
                raw_log_id INTEGER,
                line_no INTEGER,
                source_file TEXT,
                FOREIGN KEY(raw_log_id) REFERENCES raw_logs(id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_findings (
                tag TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_norm_raw_line ON normalized_lines(raw_log_id, line_no)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_source ON raw_logs(source_file)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_norm_raw ON normalized_lines(raw_log_id)")
        cols = {row[1] for row in cur.execute("PRAGMA table_info(normalized_lines)").fetchall()}
        if "timestamp_quality" not in cols:
            cur.execute("ALTER TABLE normalized_lines ADD COLUMN timestamp_quality TEXT")

        cols = {row[1] for row in cur.execute("PRAGMA table_info(events)").fetchall()}
        if "timestamp_quality" not in cols:
            cur.execute("ALTER TABLE events ADD COLUMN timestamp_quality TEXT")
        if "line_no" not in cols:
            cur.execute("ALTER TABLE events ADD COLUMN line_no INTEGER")
        if "source_file" not in cols:
            cur.execute("ALTER TABLE events ADD COLUMN source_file TEXT")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_norm_ts ON normalized_lines(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_src ON events(src_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_dst ON events(dst_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_item ON events(item)")

        conn.commit()
