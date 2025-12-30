import json
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from .save import load_payload
from .util import utc_now_iso

console = Console()

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output"
EXPORT_DIR = OUTPUT_DIR / "exports"

def export_tag(tag: str, fmt: str = "txt", silent: bool = False):
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    rec = load_payload(tag)
    if not rec:
        if not silent:
            console.print(f"[red]No saved tag:[/red] {tag}")
        return

    payload = json.loads(rec["payload"])
    stamp = utc_now_iso().replace(":", "").replace("-", "")
    out_base = EXPORT_DIR / f"{tag}_{stamp}"

    if fmt == "json":
        p = out_base.with_suffix(".json")
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if not silent:
            console.print(Panel(str(p), title="EXPORTED JSON"))
        return

    if fmt == "txt":
        p = out_base.with_suffix(".txt")
        p.write_text(_payload_to_txt(rec, payload), encoding="utf-8")
        if not silent:
            console.print(Panel(str(p), title="EXPORTED TXT"))
        return

    if fmt == "html":
        p = out_base.with_suffix(".html")
        p.write_text(_payload_to_html(rec, payload), encoding="utf-8")
        if not silent:
            console.print(Panel(str(p), title="EXPORTED HTML"))
        return

    if not silent:
        console.print("[red]Unknown format[/red] Use: txt|html|json")

def _payload_to_txt(rec, payload):
    lines = []
    lines.append(f"TAG: {rec['tag']}")
    lines.append(f"KIND: {rec['kind']}")
    lines.append(f"CREATED: {rec['created_at']}")
    lines.append("")
    lines.append(json.dumps(payload, ensure_ascii=False, indent=2))
    return "\n".join(lines)

def _payload_to_html(rec, payload):
    body = f"<h2>Tag: {rec['tag']}</h2><p><b>Kind:</b> {rec['kind']}<br><b>Created:</b> {rec['created_at']}</p>"
    body += "<pre>" + _esc(json.dumps(payload, ensure_ascii=False, indent=2)) + "</pre>"

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{_esc(rec['tag'])}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; }}
pre {{ background:#f6f6f6; padding:12px; border-radius:8px; overflow:auto; }}
</style>
</head>
<body>
{body}
</body>
</html>"""

def _esc(s: str) -> str:
    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
