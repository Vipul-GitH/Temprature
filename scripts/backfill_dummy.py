"""
Backfill dummy data for all configured rooms/tables between two datetimes.

Usage:
  python -m scripts.backfill_dummy --start 2025-08-01 --end 2026-02-04 --step-hours 1 --rooms "Main Store" "Microlab room 1"

Defaults:
  start: 2025-08-01 00:00
  end: today 00:00
  step: 1 hour
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from app.routers.temperature import ROOM_DEFINITIONS, insert_dummy_reading_at


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def main():
    parser = argparse.ArgumentParser(description="Backfill dummy readings for all rooms.")
    parser.add_argument("--start", type=parse_dt, default=datetime(2025, 8, 1), help="ISO datetime start (default 2025-08-01)")
    parser.add_argument("--end", type=parse_dt, default=datetime.now(), help="ISO datetime end (default now)")
    parser.add_argument("--step-hours", type=int, default=1, help="Step in hours (default 1)")
    parser.add_argument(
        "--rooms",
        nargs="*",
        default=None,
        help="Optional list of room names to backfill (case-insensitive). If omitted, all rooms run.",
    )
    args = parser.parse_args()

    start = args.start
    end = args.end
    step = timedelta(hours=max(1, args.step_hours))

    selected_rooms = None
    if args.rooms:
        selected_rooms = {name.strip().lower() for name in args.rooms if name.strip()}

    total_attempts = 0
    total_inserted = 0
    ts = start
    while ts <= end:
        for config in ROOM_DEFINITIONS:
            if selected_rooms and config.get("name", "").strip().lower() not in selected_rooms:
                continue
            total_attempts += 1
            if insert_dummy_reading_at(config, ts):
                total_inserted += 1
        ts += step

    print(f"Backfill complete: attempted={total_attempts}, inserted={total_inserted}, start={start}, end={end}, step_hours={args.step_hours}")


if __name__ == "__main__":
    main()
