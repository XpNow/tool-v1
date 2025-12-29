from __future__ import annotations

from collections import defaultdict

from .models import StorageContainerSummary, StorageItemSummary
from .repository import fetch_storage_events
from .util import build_warning_lines


def compute_storage_summary(
    pid: str,
    container_filter: str | None = None,
    ts_from: str | None = None,
    ts_to: str | None = None,
):
    events = fetch_storage_events(pid=pid, container_filter=container_filter, ts_from=ts_from, ts_to=ts_to)

    containers_meta: dict[str, dict[str, int]] = defaultdict(lambda: {"puts": 0, "removes": 0})
    balances: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    totals_in: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    totals_out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    rel = 0
    unk_qty = 0
    unk_container = 0

    for ev in events:
        if (ev.timestamp_quality or "").upper() == "RELATIVE":
            rel += 1
        if ev.item and ev.qty is None:
            unk_qty += 1
        if ev.event_type in ("container_put", "container_remove") and not (ev.container or "").strip():
            unk_container += 1

        et = (ev.event_type or "").strip()
        container = (ev.container or "UNKNOWN").strip()
        item = (ev.item or "").strip()
        qty = ev.qty
        if not item:
            continue
        if qty is None:
            if et == "container_put":
                containers_meta[container]["puts"] += 1
            elif et == "container_remove":
                containers_meta[container]["removes"] += 1
            continue

        q = int(qty)
        if et == "container_put":
            containers_meta[container]["puts"] += 1
            balances[container][item] += q
            totals_in[container][item] += q
        elif et == "container_remove":
            containers_meta[container]["removes"] += 1
            balances[container][item] -= q
            totals_out[container][item] += q

    negative_count = sum(
        1
        for container_items in balances.values()
        for qty in container_items.values()
        if qty < 0
    )

    warnings = build_warning_lines(
        relative_count=rel,
        unknown_qty_count=unk_qty,
        unknown_container_count=unk_container,
        negative_storage_count=negative_count,
    )

    containers: list[StorageContainerSummary] = []
    for container, items in balances.items():
        item_summaries: list[StorageItemSummary] = []
        for item, qty in sorted(items.items(), key=lambda kv: kv[0]):
            item_summaries.append(
                StorageItemSummary(
                    item=item,
                    current=int(qty),
                    total_in=int(totals_in[container].get(item, 0)),
                    total_out=int(totals_out[container].get(item, 0)),
                )
            )
        meta = containers_meta.get(container, {"puts": 0, "removes": 0})
        containers.append(
            StorageContainerSummary(
                container=container,
                items=item_summaries,
                puts=int(meta.get("puts", 0)),
                removes=int(meta.get("removes", 0)),
            )
        )

    containers.sort(key=lambda c: c.container)
    return containers, warnings, negative_count
