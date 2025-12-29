from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()
BASE_DIR = Path(__file__).resolve().parents[1]
HUB_DIR = BASE_DIR / "output" / "hub"

HELP_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Phoenix Tool Hub</title>
<style>
body{font-family:Arial;margin:24px;max-width:1000px}
code,pre{background:#f6f6f6;padding:2px 6px;border-radius:6px}
pre{padding:12px}
h2{margin-top:28px}
</style></head><body>
<h1>Phoenix Tool Hub</h1>
<p>Local manual + examples.</p>

<h2>Core workflow</h2>
<pre>
1) Put logs in /logs
2) python main.py load logs/
3) python main.py identity-rebuild
4) python main.py parse
</pre>

<h2>Commands</h2>
<ul>
<li><b>search</b>: <code>python main.py search id=633</code></li>
<li><b>trace</b>: <code>python main.py trace 633 depth=2</code></li>
<li><b>flow</b>: <code>python main.py flow 633 dir=in depth=4 window=120</code></li>
<li><b>summary</b>: <code>python main.py summary 633</code></li>
<li><b>save</b>: <code>python main.py save tag=t633 kind=trace id=633 depth=2</code></li>
<li><b>export</b>: <code>python main.py export t633 fmt=html</code></li>
<li><b>hub</b>: <code>python main.py hub</code></li>
<li><b>audit</b>: <code>python main.py audit</code></li>
<li><b>debug</b>: <code>python main.py debug</code></li>
</ul>

</body></html>"""

def build_hub():
    HUB_DIR.mkdir(parents=True, exist_ok=True)
    p = HUB_DIR / "index.html"
    p.write_text(HELP_HTML, encoding="utf-8")
    console.print(Panel(str(p), title="HUB BUILT"))
