# Phoenix Investigation Tool

Phoenix is a Python CLI and local web UI for ingesting Discord logs, parsing them into SQLite, and running investigation commands.

## Setup

```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## CLI usage

Run the ingest pipeline:

```bash
python main.py load path/to/logs.txt
python main.py normalize
python main.py parse
```

Example queries:

```bash
python main.py search id=101 limit=10
python main.py summary 101
python main.py storages 101 container=Trunk
python main.py flow 101 dir=both depth=3
python main.py trace 101 depth=2
python main.py between a=101 b=202
```

### JSON output

Every command supports JSON mode:

```bash
python main.py search id=101 --format json
python main.py summary 101 --format json
```

JSON output prints exactly one object with this shape:

```json
{
  "ok": true,
  "command": "search",
  "params": {},
  "warnings": [],
  "data": {},
  "meta": {"version": "1.0", "db_path": "...", "generated_at": "..."}
}
```

## Web UI (local)

Start the FastAPI server:

```bash
python -m phoenix_tool.api.server
```

Open http://127.0.0.1:8000 in your browser.

## Configuration

To point at a different database path, set `PHOENIX_DB`:

```bash
set PHOENIX_DB=C:\path\to\phoenix.db  # Windows (cmd)
$env:PHOENIX_DB = "C:\path\to\phoenix.db"  # PowerShell
export PHOENIX_DB=/path/to/phoenix.db     # Linux/macOS
```
