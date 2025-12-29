from __future__ import annotations

from typing import Iterable

from app import repository as app_repo
from app.models import Event, IdentityRecord, PartnerStat, StorageContainerSummary


def search_events(**kwargs):
    return app_repo.search_events(**kwargs)


def count_search_events(**kwargs):
    return app_repo.count_search_events(**kwargs)


def fetch_events_for_id(pid: str):
    return app_repo.fetch_events_for_id(pid)


def fetch_event_type_counts_for_id(pid: str):
    return app_repo.fetch_event_type_counts_for_id(pid)


def fetch_money_totals_for_id(pid: str):
    return app_repo.fetch_money_totals_for_id(pid)


def fetch_top_partners(pid: str) -> list[PartnerStat]:
    return app_repo.fetch_top_partners(pid)


def fetch_storage_events(pid: str, container_filter: str | None, ts_from: str | None, ts_to: str | None):
    return app_repo.fetch_storage_events(pid=pid, container_filter=container_filter, ts_from=ts_from, ts_to=ts_to)


def fetch_flow_events(event_types: Iterable[str], item_filter: str | None = None) -> list[Event]:
    return app_repo.fetch_flow_events(event_types, item_filter=item_filter)


def fetch_trace_events(event_types: Iterable[str], item_filter: str | None = None) -> list[Event]:
    return app_repo.fetch_trace_events(event_types, item_filter=item_filter)


def fetch_identities(pid: str) -> list[IdentityRecord]:
    return app_repo.fetch_identities(pid)


def fetch_directional_events(pid: str, direction: str, types: list[str], ts_from: str | None, ts_to: str | None, limit: int):
    return app_repo.fetch_directional_events(pid, direction, types, ts_from, ts_to, limit)


def fetch_normalized_lines():
    return app_repo.fetch_normalized_lines()


def fetch_event_counts():
    return app_repo.fetch_event_counts()


def fetch_recent_entities(limit: int = 10) -> list[dict]:
    return app_repo.fetch_recent_entities(limit=limit)


def search_entities(query: str, limit: int = 20) -> list[dict]:
    return app_repo.search_entities(query=query, limit=limit)
