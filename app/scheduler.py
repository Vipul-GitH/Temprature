import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_thread_lock = threading.Lock()
_dummy_thread: threading.Thread | None = None
_dummy_lock = threading.Lock()
_dummy_state_lock = threading.Lock()
_alert_state_lock = threading.Lock()

SCHEDULER_ENABLED = os.getenv("ENABLE_ALERT_SCHEDULER", "true").lower() not in {"0", "false", "no"}
SCHEDULER_INTERVAL_MINUTES = max(1, int(os.getenv("ALERT_SCAN_INTERVAL_MINUTES", "60")))
DUMMY_SCHEDULER_ENABLED = os.getenv("ENABLE_DUMMY_SCHEDULER", "false").lower() in {"1", "true", "yes"}
DUMMY_INTERVAL_MINUTES = max(1, int(os.getenv("DUMMY_INSERT_INTERVAL_MINUTES", "30")))
DUMMY_INTERVAL_SECONDS = max(0, int(os.getenv("DUMMY_INSERT_INTERVAL_SECONDS", "0")))
DUMMY_TARGET_ROOM = os.getenv("DUMMY_TARGET_ROOM", "").strip()
DUMMY_TARGET_TABLE = os.getenv("DUMMY_TARGET_TABLE", "").strip()
_dummy_enabled = DUMMY_SCHEDULER_ENABLED
_dummy_table_flags: dict[str, bool] = {}
_alert_enabled = SCHEDULER_ENABLED


def _scheduler_loop():
    logger.info(
        "Room alert scheduler started (interval=%s minutes, enabled=%s)",
        SCHEDULER_INTERVAL_MINUTES,
        SCHEDULER_ENABLED,
    )
    interval_seconds = SCHEDULER_INTERVAL_MINUTES * 60
    while True:
        started = time.time()
        try:
            from app.routers.temperature import scan_rooms_for_alerts
            with _alert_state_lock:
                do_run = _alert_enabled
            if do_run:
                scan_rooms_for_alerts()
        except Exception:
            logger.exception("Room alert scheduler iteration failed")
        elapsed = time.time() - started
        sleep_for = max(1, interval_seconds - int(elapsed))
        time.sleep(sleep_for)


def start_alert_scheduler(*, force: bool = False):
    """Spawn a background thread that evaluates rooms and raises tickets."""
    if not SCHEDULER_ENABLED and not force:
        logger.info("Room alert scheduler disabled via ENABLE_ALERT_SCHEDULER")
        return None
    global _scheduler_thread
    with _thread_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return _scheduler_thread
        _scheduler_thread = threading.Thread(
            target=_scheduler_loop,
            name="room-alert-scheduler",
            daemon=True,
        )
        _scheduler_thread.start()
        return _scheduler_thread

def get_alert_scheduler_state() -> dict:
    with _alert_state_lock:
        return {
            "enabled": _alert_enabled,
            "interval_minutes": SCHEDULER_INTERVAL_MINUTES,
        }

def set_alert_scheduler_enabled(enabled: bool) -> None:
    global _alert_enabled
    with _alert_state_lock:
        _alert_enabled = bool(enabled)

def _dummy_scheduler_loop():
    logger.info(
        "Dummy insert scheduler started (interval=%s minutes, enabled=%s)",
        DUMMY_INTERVAL_MINUTES,
        DUMMY_SCHEDULER_ENABLED,
    )
    interval_seconds = max(1, DUMMY_INTERVAL_SECONDS if DUMMY_INTERVAL_SECONDS > 0 else DUMMY_INTERVAL_MINUTES * 60)
    while True:
        started = time.time()
        try:
            from app.routers.temperature import insert_dummy_for_targets
            room = DUMMY_TARGET_ROOM or None
            table = DUMMY_TARGET_TABLE or None
            with _dummy_state_lock:
                enabled = _dummy_enabled
                table_flags = dict(_dummy_table_flags)
            if enabled:
                if table_flags:
                    enabled_tables = [name for name, on in table_flags.items() if on]
                    for table_name in enabled_tables:
                        insert_dummy_for_targets(room_name=room, table_name=table_name)
                else:
                    insert_dummy_for_targets(room_name=room, table_name=table)
        except Exception:
            logger.exception("Dummy insert scheduler iteration failed")
        elapsed = time.time() - started
        sleep_for = max(1, interval_seconds - int(elapsed))
        time.sleep(sleep_for)

def start_dummy_scheduler(*, force: bool = False):
    """Spawn a background thread that inserts dummy readings."""
    if not DUMMY_SCHEDULER_ENABLED and not force:
        logger.info("Dummy insert scheduler disabled via ENABLE_DUMMY_SCHEDULER")
        return None
    global _dummy_thread
    with _dummy_lock:
        if _dummy_thread and _dummy_thread.is_alive():
            return _dummy_thread
        _dummy_thread = threading.Thread(
            target=_dummy_scheduler_loop,
            name="dummy-insert-scheduler",
            daemon=True,
        )
        _dummy_thread.start()
        return _dummy_thread

def get_dummy_scheduler_state() -> dict:
    with _dummy_state_lock:
        return {
            "enabled": _dummy_enabled,
            "interval_minutes": DUMMY_INTERVAL_MINUTES,
            "interval_seconds": DUMMY_INTERVAL_SECONDS if DUMMY_INTERVAL_SECONDS > 0 else DUMMY_INTERVAL_MINUTES * 60,
            "table_flags": dict(_dummy_table_flags),
        }

def set_dummy_scheduler_enabled(enabled: bool) -> None:
    global _dummy_enabled
    with _dummy_state_lock:
        _dummy_enabled = bool(enabled)

def set_dummy_table_enabled(table_name: str, enabled: bool) -> None:
    if not table_name:
        return
    with _dummy_state_lock:
        _dummy_table_flags[table_name] = bool(enabled)


__all__ = [
    "start_alert_scheduler",
    "start_dummy_scheduler",
    "get_dummy_scheduler_state",
    "set_dummy_scheduler_enabled",
    "set_dummy_table_enabled",
    "get_alert_scheduler_state",
    "set_alert_scheduler_enabled",
]
