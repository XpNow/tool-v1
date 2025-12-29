import hashlib
import re
from datetime import datetime, timezone

# -------------------------
# Time helpers
# -------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_maybe(ts: str):
    """Parse ISO timestamp string (optionally Z) to datetime. Returns None if invalid."""
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        # Normalize to UTC if tz-aware
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def format_ts_display(ts_iso: str | None, ts_raw: str | None = None) -> str:
    """
    Display timestamp in *local machine timezone* (matches how Discord shows times to the viewer).
    If ts_iso can't be parsed, fall back to ts_raw (Option B), else empty.
    Output format: DD.MM.YYYY HH:MM
    """
    dt = parse_iso_maybe(ts_iso or "")
    if dt is not None:
        local_tz = datetime.now().astimezone().tzinfo
        dt_local = dt.astimezone(local_tz) if local_tz else dt
        return dt_local.strftime("%d.%m.%Y %H:%M")
    if ts_raw:
        return f"(ts: {ts_raw})"
    return ""


# -------------------------
# Hashing
# -------------------------

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


# -------------------------
# Normalizers (parsing)
# -------------------------

def parse_int_ro(value_str: str | None) -> int | None:
    """
    Parse a Romanian-formatted integer string.

    Handles:
      - dots/commas/space separators: 13.000.000 / 13,000,000 / 13 000 000
      - prefixes/suffixes like x482.708, (x7.825)
      - unicode spaces
    Returns None if no digits are present.
    """
    if value_str is None:
        return None
    s = str(value_str).strip()
    if not s:
        return None
    s = s.replace("$", "")
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None


def normalize_money(s: str) -> int:
    value = parse_int_ro(s)
    return int(value or 0)


def normalize_qty(s: str):
    """
    Normalize quantity strings into an integer (or None if unknown).
    """
    return parse_int_ro(s)

# -------------------------
# Display formatting (output only)
# -------------------------

def format_money_ro(amount: int | None) -> str:
    """Return money like 5.500.000$ (output only)."""
    n = int(amount or 0)
    # Python grouping uses commas; switch to dots and append $
    return f"{n:,}".replace(",", ".") + "$"


def actor_label(name: str | None, pid: str | None) -> str:
    if pid and name:
        return f"{name}[{pid}]"
    if pid:
        return f"[{pid}]"
    if name:
        return name
    return ""


def _read_event_value(ev, key, default=None):
    if isinstance(ev, dict):
        return ev.get(key, default)
    return getattr(ev, key, default)


def render_event_line(ev) -> str:
    """
    Produce the locked narrative line format:
    DD.MM.YYYY HH:MM - Name[ID] <verb> ... (mixed EN/RO allowed)
    """
    t = format_ts_display(_read_event_value(ev, "ts"), _read_event_value(ev, "ts_raw")).strip()
    if not t:
        t = format_ts_display(None, _read_event_value(ev, "ts_raw")).strip()

    et = _read_event_value(ev, "event_type") or ""
    src = actor_label(_read_event_value(ev, "src_name"), _read_event_value(ev, "src_id"))
    dst = actor_label(_read_event_value(ev, "dst_name"), _read_event_value(ev, "dst_id"))
    item = _read_event_value(ev, "item") or ""
    qty_val = _read_event_value(ev, "qty")
    qty = int(qty_val) if qty_val is not None else None
    money = int(_read_event_value(ev, "money") or 0)
    container = _read_event_value(ev, "container") or "UNKNOWN"

    prefix = f"{t} - " if t else ""

    # Money movements
    if et == "bank_transfer":
        return f"{prefix}{src} transferred {format_money_ro(money)} to {dst}".strip()

    if et == "bank_deposit":
        actor = src or dst
        return f"{prefix}{actor} deposited {format_money_ro(money)} (bank)".strip()

    if et == "bank_withdraw":
        actor = src or dst
        return f"{prefix}{actor} withdrew {format_money_ro(money)} (bank)".strip()

    if et == "ofera_bani":
        return f"{prefix}{src} offered {format_money_ro(money)} to {dst}".strip()

    # Items
    if et == "ofera_item":
        q = f"{qty}x" if (qty is not None and qty != 0) else "?x"
        return f"{prefix}{src} offered {q} {item} to {dst}".strip()

    if et == "perchezitie_remove":
        # victim id is stored in dst_id
        q = f"{qty}x" if (qty is not None and qty != 0) else "?x"
        victim = _read_event_value(ev, "dst_id") or "UNKNOWN"
        return f"{prefix}{src} took {q} {item} from ID {victim} (PERCHEZITIE)".strip()

    if et == "drop_item":
        q = f"{qty}x" if (qty is not None and qty != 0) else "?x"
        return f"{prefix}{src} dropped {q} {item}".strip()

    # Containers
    if et == "container_put":
        # For money sometimes item may represent money-type; keep generic
        if money:
            return f"{prefix}{src} deposited {format_money_ro(money)} into {container}".strip()
        q = f"{qty}x" if (qty is not None and qty != 0) else "?x"
        return f"{prefix}{src} deposited {q} {item} into {container}".strip()

    if et == "container_remove":
        if money:
            return f"{prefix}{src} withdrew {format_money_ro(money)} from {container}".strip()
        q = f"{qty}x" if (qty is not None and qty != 0) else "?x"
        return f"{prefix}{src} withdrew {q} {item} from {container}".strip()

    # Banking/phone
    if et == "phone_add":
        actor = src or dst
        return f"{prefix}{actor} phone +{format_money_ro(money)}".strip()

    if et == "phone_remove":
        actor = src or dst
        return f"{prefix}{actor} phone -{format_money_ro(money)}".strip()

    # Vehicles / assets
    if et in ("showroom_buy", "showroom_sell", "remat", "vehicle_buy_showroom", "vehicle_sell_remat", "vehicle_sell_to_player"):
        # Keep Romanian asset names/items if present
        detail = item if item else et
        if money:
            return f"{prefix}{src} {et} {detail} for {format_money_ro(money)}".strip()
        return f"{prefix}{src} {et} {detail}".strip()

    # Context
    if et in ("connect", "disconnect", "respawn", "revive"):
        return f"{prefix}{src} {et}".strip()

    # Fallback: show raw event type with whatever fields exist
    if src and dst:
        return f"{prefix}{src} -> {dst} ({et})".strip()
    if src:
        return f"{prefix}{src} ({et})".strip()
    return f"{prefix}{et}".strip()


def build_warning_lines(
    relative_count: int = 0,
    unknown_qty_count: int = 0,
    unknown_container_count: int = 0,
    negative_storage_count: int = 0,
) -> list[str]:
    return [
        f"RELATIVE timestamps: {relative_count}",
        f"UNKNOWN qty: {unknown_qty_count}",
        f"UNKNOWN container: {unknown_container_count}",
        f"negative storage likely missing history: {negative_storage_count}",
    ]


def last_known_location_from_chain(chain: list, direction: str) -> str:
    """Return last proven holder/container for a flow chain (per locked rule)."""
    if not chain:
        return "UNKNOWN"
    last = chain[-1]
    et = _read_event_value(last, "event_type")

    if et in ("container_put", "container_remove"):
        return _read_event_value(last, "container") or "UNKNOWN"

    if et == "perchezitie_remove":
        # victim id is stored in dst_id; location = robbed from victim
        victim = _read_event_value(last, "dst_id") or "UNKNOWN"
        return f"PERCHEZITIE_FROM_{victim}"

    if et == "drop_item":
        return "DROPPED_ON_GROUND"

    if et in ("vehicle_buy_showroom", "vehicle_sell_remat", "vehicle_sell_to_player", "remat", "showroom_buy", "showroom_sell"):
        return "VEHICLE_ENDPOINT"

    # otherwise last proven holder is the counterparty at the end of the chain
    holder = _read_event_value(last, "dst_id") if direction.lower() == "out" else _read_event_value(last, "src_id")
    if holder:
        return f"with ID {holder}"
    return "UNKNOWN"
