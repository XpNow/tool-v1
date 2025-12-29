import zipfile
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from .db import DB_PATH

console = Console()
BASE_DIR = Path(__file__).resolve().parents[1]
DEBUG_DIR = BASE_DIR / "output" / "debug"

def make_debug_bundle():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    zpath = DEBUG_DIR / "debug_bundle.zip"

    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        if DB_PATH.exists():
            z.write(DB_PATH, arcname="data/phoenix.db")

        hub = BASE_DIR / "output" / "hub" / "index.html"
        if hub.exists():
            z.write(hub, arcname="output/hub/index.html")

        audit = BASE_DIR / "output" / "audit" / "audit_samples.txt"
        if audit.exists():
            z.write(audit, arcname="output/audit/audit_samples.txt")

    console.print(Panel(str(zpath), title="DEBUG BUNDLE CREATED"))
