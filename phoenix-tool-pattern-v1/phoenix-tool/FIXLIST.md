# Phase 1 Fix Tracker

## Open

- Qty shows as `?x` for some item events (qty stored as NULL). Repro: `search id=<id> item="..."`.
- Timestamp ambiguity for Discord `Today/Yesterday` exports (input-side). Repro: copy logs next day.

## Fixed in this build

- Added `storages` command (container_put minus container_remove) for best-effort container state.

## Regression checks

1) `python .\main.py search between=787,8296 limit=50` returns only those two IDs.
2) `python .\main.py storages 787 container=retreiver-787` prints contents without crashing.
