from __future__ import annotations

from .repository import search_events as repo_search_events
from .repository import count_search_events as repo_count_search_events


def search_events(
    ids=None,
    between_ids=None,
    name=None,
    item=None,
    event_type=None,
    min_money=None,
    max_money=None,
    ts_from: str | None = None,
    ts_to: str | None = None,
):
    return repo_search_events(
        ids=ids,
        between_ids=between_ids,
        name=name,
        item=item,
        event_type=event_type,
        min_money=min_money,
        max_money=max_money,
        ts_from=ts_from,
        ts_to=ts_to,
        limit=limit,
    )


def count_search_events(
    ids=None,
    between_ids=None,
    name=None,
    item=None,
    event_type=None,
    min_money=None,
    max_money=None,
    ts_from: str | None = None,
    ts_to: str | None = None,
):
    return repo_count_search_events(
        ids=ids,
        between_ids=between_ids,
        name=name,
        item=item,
        event_type=event_type,
        min_money=min_money,
        max_money=max_money,
        ts_from=ts_from,
        ts_to=ts_to,
    )
