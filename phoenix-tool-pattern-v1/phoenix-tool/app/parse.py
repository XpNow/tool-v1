import re
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from .db import get_db
from .util import normalize_money, normalize_qty

console = Console()

# -------------------------
# Regex patterns
# -------------------------

RE_BANK_TRANSFER = re.compile(
    r"""
    (?P<src_name>.+?)\[(?P<src_id>\d+)\]
    \s+a\s+transferat\s+
    (?P<amount>[\d\.,]+)\$
    \s+lui\s+
    (?P<dst_name>.+?)\[(?P<dst_id>\d+)\]
    \.?
    """,
    re.I | re.X,
)

# Deposit bank: "name[id] a depozitat 1.900.000$."
RE_BANK_DEPOSIT = re.compile(
    r"(?P<name>.+?)\[(?P<id>\d+)\]\s+a\s+depozitat\s+(?P<amount>[\d\.,]+)\$\s*\.?",
    re.I,
)

# Withdraw bank: "name[id] a retras 4.900.000$."
RE_BANK_WITHDRAW = re.compile(
    r"(?P<name>.+?)\[(?P<id>\d+)\]\s+a\s+retras\s+(?P<amount>[\d\.,]+)\$\s*\.?",
    re.I,
)

RE_OFERA_ITEM = re.compile(
    r"Jucatorul\s+(?P<src_name>.+?)\[(?P<src_id>\d+)\]\s+i-a oferit lui\s+(?P<dst_name>.+?)\[(?P<dst_id>\d+)\]\s+-\s+(?P<item>.+?)\(x(?P<qty>[\d\.,\s]+)\)\.",
    re.I,
)

RE_OFERA_BANI = re.compile(
    r"Jucatorul\s+(?P<src_name>.+?)\[(?P<src_id>\d+)\]\s+i-a oferit lui\s+(?P<dst_name>.+?)\[(?P<dst_id>\d+)\]\s+suma de\s+(?P<amount>[\d\.,]+)\$\.",
    re.I,
)

RE_PHONE_ADD = re.compile(
    r"Jucătorului:\s+(?P<name>.+?)\s*\(\s*(?P<id>\d+)\s*\)\s+i-au fost adaugati\s+(?P<amount>[\d\.,]+)\s*\$",
    re.I,
)

RE_PHONE_REMOVE = re.compile(
    r"Jucătorului:\s+(?P<name>.+?)\s*\(\s*(?P<id>\d+)\s*\)\s+i-au fost luati\s+(?P<amount>[\d\.,]+)\s*\$",
    re.I,
)

RE_DROP_ITEM = re.compile(
    r"Jucător:\s+(?P<name>.+?)\s*\(\s*(?P<id>\d+)\s*\)\s+a aruncat pe jos\s+(?P<qty>[\d\.,\s]+)x\s+(?P<item>.+)",
    re.I,
)

RE_CONTAINER_PUT = re.compile(
    r"\[TRANSFER\]\s+Jucatorul\s+(?P<name>.+?)\[(?P<id>\d+)\]\s+a pus in\s+(?P<container>.+?)\s+item-ul\s+(?P<item>.+?)\(x(?P<qty>[\d\.,\s]+)\)\.",
    re.I,
)

RE_CONTAINER_REMOVE = re.compile(
    r"\[REMOVE\]\s+Jucatorul\s+(?P<name>.+?)\[(?P<id>\d+)\]\s+a scos din\s+(?P<container>.+?)\s+item-ul\s+(?P<item>.+?)\(x(?P<qty>[\d\.,\s]+)\)\.",
    re.I,
)

# Perchezitie (robbery/search): "[PERCHEZITIE] Jucatorul Name[2] a scos din 787 item-ul Gadget Pistol(x1)."
RE_PERCHEZITIE = re.compile(
    r"\[PERCHEZITIE\]\s+Jucatorul\s+(?P<src_name>.+?)\[(?P<src_id>\d+)\]\s+a\s+scos\s+din\s+(?P<dst_id>\d+)\s+item-ul\s+(?P<item>.+?)\(x(?P<qty>[\d\.,\s]+)\)\.",
    re.I,
)


# Vehicle sell (Remat): "Jucător: Name (123) a vandut vehiculul Model [code] pentru suma de X$ | GARAGE: Y"
RE_VEHICLE_SELL_REMAT = re.compile(
    r"Jucător:\s+(?P<name>.+?)\s+\((?P<id>\d+)\)\s+a vandut vehiculul\s+(?P<veh>.+?)\s+\[(?P<veh_code>.*?)\]\s+pentru suma de\s+(?P<amount>[\d\.,]+)\$\s+\|\s+GARAGE:\s*(?P<garage>.+)$",
    re.I,
)

# Vehicle buy (Showroom): "Jucător: Name (123) a achizitionat vehiculul: ... pentru suma de X$ !"
RE_VEHICLE_BUY_SHOWROOM = re.compile(
    r"Jucător:\s+(?P<name>.+?)\s+\((?P<id>\d+)\)\s+a achizitionat vehiculul:\s+(?P<veh>.+?)\s+\[(?P<veh_code>.*?)\]\s+pentru suma de\s+(?P<amount>[\d\.,]+)\$\s*!?\s*",
    re.I,
)

# Player-to-player vehicle sale: "Jucător: Alberto a vandut vehiculul nerossmk2 lui [8296] NAME pentru suma de 1$!"
RE_VEHICLE_SELL_TO_PLAYER = re.compile(
    r"Jucător:\s+(?P<src_name>.+?)\s+a vandut vehiculul\s+(?P<veh_code>[A-Za-z0-9_]+)\s+lui\s+\[(?P<dst_id>\d+)\]\s+(?P<dst_name>.+?)\s+pentru suma de\s+(?P<amount>[\d\.,]+)\$\!",
    re.I,
)

RE_CONNECT = re.compile(
    r"(?P<name>[A-Za-z0-9_?]+)\[(?P<id>\d+)\]\s+se\s+conecteaz(?:ă|a)\s+cu\s+succes.*?\(ip:\s*(?P<ip>[\d\.]+)\*\*\)",
    re.I,
)

RE_DISCONNECT = re.compile(
    r"(?P<name>[A-Za-z0-9_?]+)\[(?P<id>\d+)\]\s+s-a\s+deconectat\s+cu\s+succes.*?\(ip:\s*(?P<ip>[^)]+)\)",
    re.I,
)


RE_DEPOSIT = re.compile(r"(?P<name>.+?)\[(?P<id>\d+)\]\s+a\s+depozitat\s+(?P<amount>[\d\.,]+)\$\s*\.?", re.I)
RE_WITHDRAW = re.compile(r"(?P<name>.+?)\[(?P<id>\d+)\]\s+a\s+retras\s+(?P<amount>[\d\.,]+)\$\s*\.?", re.I)

# -------------------------
# Audit handling
# -------------------------

AUDIT_PATH = Path("output/audit/audit_samples.txt")

SKIP_AUDIT_PREFIXES = ("Made by ", "Made by Synked")
SKIP_AUDIT_EXACT = {"APP", "Freaks Logs", "PHOENIX LOGS", "Depunere Banca", "Retragere Banca", "Ofera Item", "Ofera Bani", "Transfera Item", "Transfer (Bancar)", "🚗 Remat", "🚘 Showroom", "💵 Telefon"}

def should_audit(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return False
    if s in SKIP_AUDIT_EXACT:
        return False
    if any(s.startswith(p) for p in SKIP_AUDIT_PREFIXES):
        return False
    if set(s) <= {"="} and len(s) >= 10:
        return False
    if s.startswith("RAW_LOG_ID:") or s.startswith("FILE:"):
        return False
    # ban blocks are usually not useful for money tracing; ignore by default
    if s.startswith("[ID]") and "BAN" in s.upper():
        return False
    return True

def _audit_unparsed(raw_log_id: int, ts: str | None, ts_raw: str | None, text: str):
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[raw_log_id={raw_log_id}] ts={ts or 'NULL'} ts_raw={ts_raw or 'NULL'}\n")
        f.write(text.strip() + "\n")
        f.write("-" * 60 + "\n")

# -------------------------
# Parser
# -------------------------

def parse_events():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("DELETE FROM events")

    rows = cur.execute("""
        SELECT
            nl.raw_log_id,
            nl.line_no,
            nl.ts,
            nl.ts_raw,
            nl.timestamp_quality,
            nl.text,
            rl.source_file
        FROM normalized_lines nl
        JOIN raw_logs rl ON rl.id = nl.raw_log_id
        ORDER BY
            CASE WHEN nl.ts IS NULL THEN 1 ELSE 0 END,
            nl.ts ASC,
            nl.raw_log_id ASC,
            nl.line_no ASC
    """).fetchall()

    inserted = 0
    unparsed = 0

    for r in rows:
        raw_id = r["raw_log_id"]
        line_no = r["line_no"]
        ts = r["ts"]
        ts_raw = r["ts_raw"]
        ts_quality = r["timestamp_quality"]
        source_file = r["source_file"]
        line = (r["text"] or "").strip()

        if not line:
            continue

        # --- Transfers ---
        m = RE_BANK_TRANSFER.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,dst_id,dst_name,
                    money,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "bank_transfer",
                m.group("src_id"), (m.group("src_name") or "").strip(),
                m.group("dst_id"), (m.group("dst_name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # --- Deposits / Withdraws ---
        m = RE_BANK_DEPOSIT.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    dst_id,dst_name,money,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "bank_deposit",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_BANK_WITHDRAW.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,money,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "bank_withdraw",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # --- Ofera ---
        m = RE_OFERA_ITEM.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,dst_id,dst_name,
                    item,qty,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "ofera_item",
                m.group("src_id"), (m.group("src_name") or "").strip(),
                m.group("dst_id"), (m.group("dst_name") or "").strip(),
                (m.group("item") or "").strip(),
                normalize_qty(m.group("qty")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_OFERA_BANI.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,dst_id,dst_name,
                    money,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "ofera_bani",
                m.group("src_id"), (m.group("src_name") or "").strip(),
                m.group("dst_id"), (m.group("dst_name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # --- Phone ---
        m = RE_PHONE_ADD.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,money,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "phone_add",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_PHONE_REMOVE.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,money,raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "phone_remove",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # --- Items ---
        m = RE_DROP_ITEM.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,item,qty,
                    raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "drop_item",
                m.group("id"), (m.group("name") or "").strip(),
                (m.group("item") or "").strip(),
                normalize_qty(m.group("qty")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_CONTAINER_PUT.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,item,qty,container,
                    raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "container_put",
                m.group("id"), (m.group("name") or "").strip(),
                (m.group("item") or "").strip(),
                normalize_qty(m.group("qty")),
                (m.group("container") or "").strip(),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_CONTAINER_REMOVE.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,item,qty,container,
                    raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "container_remove",
                m.group("id"), (m.group("name") or "").strip(),
                (m.group("item") or "").strip(),
                normalize_qty(m.group("qty")),
                (m.group("container") or "").strip(),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # --- Perchezitie (robbery/search) ---
        m = RE_PERCHEZITIE.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    src_id,src_name,dst_id,item,qty,
                    raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "perchezitie_remove",
                m.group("src_id"), (m.group("src_name") or "").strip(),
                m.group("dst_id"),
                (m.group("item") or "").strip(),
                normalize_qty(m.group("qty")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

                # --- Vehicles ---

        # Remat: "Jucător: Name (16747) a vandut vehiculul Truffade Nero Custom [nero2] pentru suma de 4.875.000$ | GARAGE: Public"
        m = RE_VEHICLE_SELL_REMAT.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts, ts_raw, timestamp_quality, event_type,
                    src_id, src_name,
                    money, item, container,
                    raw_log_id, line_no, source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "vehicle_sell_remat",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                f"{(m.group('veh') or '').strip()} [{(m.group('veh_code') or '').strip()}]",
                (m.group("garage") or "").strip(),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # Showroom buy: "Jucător: Alberto (9568) a achizitionat vehiculul: Emperor [Ronin] pentru suma de 7.250.000$ !"
        m = RE_VEHICLE_BUY_SHOWROOM.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts, ts_raw, timestamp_quality, event_type,
                    dst_id, dst_name,
                    money, item,
                    raw_log_id, line_no, source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "vehicle_buy_showroom",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                f"{(m.group('veh') or '').strip()} [{(m.group('veh_code') or '').strip()}]",
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # Player-to-player sell: "Jucător: Alberto a vandut vehiculul nerossmk2 lui [8296] NAME pentru suma de 1$!"
        # NOTE: src_id is not present in text, so we store src_id=NULL and keep src_name.
        m = RE_VEHICLE_SELL_TO_PLAYER.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts, ts_raw, timestamp_quality, event_type,
                    src_id, src_name,
                    dst_id, dst_name,
                    money, item,
                    raw_log_id, line_no, source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "vehicle_sell_to_player",
                None, (m.group("src_name") or "").strip(),
                m.group("dst_id"), (m.group("dst_name") or "").strip(),
                normalize_money(m.group("amount")),
                (m.group("veh_code") or "").strip(),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue


        # --- Connect / disconnect ---
        m = RE_CONNECT.search(line)
        if m:
            ip = (m.group("ip") or "").strip().replace("**", "")
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    dst_id,dst_name,container,
                    raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "connect",
                m.group("id"), (m.group("name") or "").strip(),
                ip, raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_DISCONNECT.search(line)
        if m:
            ip_raw = (m.group("ip") or "").strip().replace("**", "")
            ip = None if ip_raw.lower() in ("nil", "") else ip_raw
            cur.execute("""
                INSERT INTO events(
                    ts,ts_raw,timestamp_quality,event_type,
                    dst_id,dst_name,container,
                    raw_log_id,line_no,source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "disconnect",
                m.group("id"), (m.group("name") or "").strip(),
                ip, raw_id, line_no, source_file
            ))
            inserted += 1
            continue


        m = RE_DEPOSIT.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts, ts_raw, timestamp_quality, event_type,
                    dst_id, dst_name, money,
                    raw_log_id, line_no, source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "bank_deposit",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        m = RE_WITHDRAW.search(line)
        if m:
            cur.execute("""
                INSERT INTO events(
                    ts, ts_raw, timestamp_quality, event_type,
                    src_id, src_name, money,
                    raw_log_id, line_no, source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, ts_raw, ts_quality, "bank_withdraw",
                m.group("id"), (m.group("name") or "").strip(),
                normalize_money(m.group("amount")),
                raw_id, line_no, source_file
            ))
            inserted += 1
            continue

        # --- Audit ---
        if should_audit(line):
            _audit_unparsed(raw_id, ts, ts_raw, line)
            unparsed += 1

    conn.commit()
    conn.close()

    console.print(Panel(f"Events inserted: {inserted}\nUnparsed lines: {unparsed}", title="EVENT PARSE"))
    return inserted


# -------------------------
# Public helpers (audit)
# -------------------------

KNOWN_PATTERNS = [
    RE_BANK_TRANSFER,
    RE_BANK_DEPOSIT,
    RE_BANK_WITHDRAW,
    RE_OFERA_ITEM,
    RE_OFERA_BANI,
    RE_PHONE_ADD,
    RE_PHONE_REMOVE,
    RE_DROP_ITEM,
    RE_CONTAINER_PUT,
    RE_CONTAINER_REMOVE,
    RE_PERCHEZITIE,
    RE_VEHICLE_SELL_REMAT,
    RE_VEHICLE_BUY_SHOWROOM,
    RE_VEHICLE_SELL_TO_PLAYER,
    RE_CONNECT,
    RE_DISCONNECT,
    RE_DEPOSIT,
    RE_WITHDRAW,
]


def matches_any_known_pattern(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return False
    for rx in KNOWN_PATTERNS:
        try:
            if rx.search(s):
                return True
        except Exception:
            continue
    return False
