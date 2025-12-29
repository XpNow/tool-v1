from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Event:
    id: int | None
    ts: str | None
    ts_raw: str | None
    timestamp_quality: str | None
    event_type: str | None
    src_id: str | None
    src_name: str | None
    dst_id: str | None
    dst_name: str | None
    item: str | None
    qty: int | None
    money: int | None
    container: str | None
    raw_log_id: int | None
    line_no: int | None
    source_file: str | None


@dataclass(frozen=True)
class PartnerStat:
    partner_id: str | None
    partner_name: str | None
    count: int


@dataclass(frozen=True)
class StorageItemSummary:
    item: str
    current: int
    total_in: int
    total_out: int


@dataclass(frozen=True)
class StorageContainerSummary:
    container: str
    items: list[StorageItemSummary]
    puts: int
    removes: int


@dataclass(frozen=True)
class IdentityRecord:
    player_id: str | None
    name: str | None
    ip: str | None
    sightings: int
