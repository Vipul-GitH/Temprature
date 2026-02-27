import logging
import math
import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

from app.database import MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER
from app.routers.infra import create_stale_sensor_ticket
from app.scheduler import (
    start_alert_scheduler,
    start_dummy_scheduler,
    get_alert_scheduler_state,
    get_dummy_scheduler_state,
    set_dummy_scheduler_enabled,
    set_dummy_table_enabled,
    set_alert_scheduler_enabled,
)

STALE_THRESHOLD_MINUTES = int(os.getenv("STALE_THRESHOLD_MINUTES", "60"))
STALE_TICKET_DELAY_MINUTES = int(os.getenv("STALE_TICKET_DELAY_MINUTES", "180"))
CONSECUTIVE_OUTSIDE_THRESHOLD = int(os.getenv("CONSECUTIVE_OUTSIDE_THRESHOLD", "3"))
DUMMY_INSERT_TOKEN = os.getenv("DUMMY_INSERT_TOKEN", "")
APP_TIMEZONE = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Kolkata"))

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory='app/templates')

DEFAULT_DB_NAME = os.getenv('MYSQL_DB', 'temraturerec')
DEFAULT_TABLE_NAME = 'temperature_data'


ROOM_DEFINITIONS = [
    {'name': 'Extraction Room', 'location': 'Extraction wing', 'target': 18.5, 'baseline': 18.1, 'variance': 0.35,
     'humidity_baseline': 54.0, 'humidity_variance': 2.0, 'table': 'sensor_data', 'db': 'temraturerec', 'room_column': 'extrm', 'room_value': 'extroom', 'range_type': 'room'},
    {'name': 'Extraction Room Freezer', 'location': 'Extraction wing - Freezer', 'target': 4.0, 'baseline': 3.6, 'variance': 0.25,
     'humidity_baseline': 62.0, 'humidity_variance': 1.8, 'table': 'temperature_data', 'db': 'temraturerec', 'room_column': 'extfrdg',
     'room_value': 'extfreedge', 'include_humidity': False, 'range_type': 'fridge'},
    {'name': 'Master Mix Room Freeze', 'location': 'Preparation area', 'target': 2.0, 'baseline': 2.3, 'variance': 0.3,
     'humidity_baseline': 61.0, 'humidity_variance': 1.5, 'table': 'temperature_datafrd', 'db': 'temraturerec', 'room_column': 'mfredg',
     'room_value': 'mfreedge', 'include_humidity': False, 'range_type': 'deep_freezer'},
    {'name': 'BioChemistry Deep Freezer', 'location': 'Biochemistry lab cold storage', 'target': -30.0, 'baseline': -30.0, 'variance': 1.5,
     'humidity_baseline': 0.0, 'humidity_variance': 0.0, 'table': 'biochem_deep_freezer', 'db': 'temraturerec',
     'include_humidity': False, 'range_type': 'deep_freezer', 'skip_room_filter': True},
    {'name': 'Master Mix Room', 'location': 'Preparation area', 'target': 21.0, 'baseline': 21.4, 'variance': 0.35,
     'humidity_baseline': 52.0, 'humidity_variance': 2.2, 'table': 'sensor_dataext', 'db': 'temraturerec', 'room_column': 'mastr', 'room_value': 'master', 'range_type': 'room'},
    {'name': 'Template Room', 'location': 'PCR prep zone', 'target': 23.0, 'baseline': 23.5, 'variance': 0.3,
     'humidity_baseline': 50.0, 'humidity_variance': 1.8, 'table': 'sensor_dataextss', 'db': 'temraturerec', 'room_column': 'templ', 'room_value': 'template', 'range_type': 'room'},
    {'name': 'RTPCR Room', 'location': 'PCR suite', 'target': 23.5, 'baseline': 23.1, 'variance': 0.25,
     'humidity_baseline': 49.0, 'humidity_variance': 1.5, 'table': 'sensor_datamast', 'db': 'temraturerec', 'room_column': 'rtpct', 'room_value': 'rtpcr', 'range_type': 'room'},
    {'name': 'Main Lab Room', 'location': 'Main laboratory', 'target': 22.0, 'baseline': 22.8, 'variance': 0.4,
     'humidity_baseline': 53.0, 'humidity_variance': 2.4, 'table': 'mainlab_data', 'db': 'temraturerec', 'room_column': 'room', 'room_value': 'mainlab', 'range_type': 'room'},
    {'name': 'RTPCR Out Area Freezer', 'location': 'Outdoor PCR area', 'target': 5.0, 'baseline': 5.3, 'variance': 0.3,
     'humidity_baseline': 60.0, 'humidity_variance': 1.7, 'table': 'temperature_dataout', 'db': 'temraturerec', 'room_column': 'extfrdg',
     'room_value': 'rtpcrout', 'include_humidity': False, 'range_type': 'deep_freezer'},
    {'name': 'Histo room', 'location': 'Histo room', 'target': 22.0, 'baseline': 22.0, 'variance': 0.4,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'histo_room', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
    {'name': 'Microlab room 1', 'location': 'Microlab room 1', 'target': 22.0, 'baseline': 22.0, 'variance': 0.4,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'microlab_room_1', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
    {'name': 'Microlab room 2', 'location': 'Microlab room 2', 'target': 22.0, 'baseline': 22.0, 'variance': 0.4,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'microlab_room_2', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
    {'name': 'Microlab room 3', 'location': 'Microlab room 3', 'target': 22.0, 'baseline': 22.0, 'variance': 0.4,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'microlab_room_3', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
    {'name': 'Clinical Fridge', 'location': 'Clinical storage', 'target': 4.0, 'baseline': 4.0, 'variance': 0.4,
     'humidity_baseline': 0.0, 'humidity_variance': 0.0, 'table': 'clicnicalfridge', 'db': 'temraturerec',
     'room_column': 'fridge', 'room_value': 'clicnicalfridge', 'include_humidity': False, 'range_type': 'fridge', 'skip_room_filter': True},
    {'name': 'Clinical Room', 'location': 'Main lab', 'target': 22.0, 'baseline': 22.0, 'variance': 0.5,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'clinicalwithmainlab', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
    {'name': 'Main Store', 'location': 'Main store', 'target': 22.0, 'baseline': 22.0, 'variance': 0.5,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'main_store', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
    {'name': 'Main Store Fridge', 'location': 'Main store cold storage', 'target': 4.0, 'baseline': 4.0,
     'variance': 0.4, 'humidity_baseline': 0.0, 'humidity_variance': 0.0, 'table': 'main_store_fridge',
     'db': 'temraturerec', 'room_column': 'fridge', 'room_value': 'main_store_fridge',
     'include_humidity': False, 'range_type': 'fridge', 'skip_room_filter': True},
    {'name': 'Microlab Fridge', 'location': 'Microlab fridge', 'target': 4.0, 'baseline': 4.0, 'variance': 0.4,
     'humidity_baseline': 0.0, 'humidity_variance': 0.0, 'table': 'microlab_fridge', 'db': 'temraturerec',
     'room_column': 'fridge', 'room_value': 'microlab_fridge', 'include_humidity': False, 'range_type': 'fridge', 'skip_room_filter': False},
    {'name': 'Store 1', 'location': 'Store 1', 'target': 22.0, 'baseline': 22.0, 'variance': 0.5,
     'humidity_baseline': 52.0, 'humidity_variance': 2.0, 'table': 'store_1', 'db': 'temraturerec',
     'include_humidity': True, 'range_type': 'room', 'skip_room_filter': True},
]

ROOM_LOOKUP = {room['name'].strip().lower(): room for room in ROOM_DEFINITIONS}
ENGINE_CACHE: Dict[str, Any] = {}

SAFE_RANGE_BOUNDS = {
    'room': (20.0, 35.0),
    'fridge': (2.0, 8.0),
    'deep_freezer': (-100.0, -20.0),
}
DEFAULT_RANGE_TYPE = 'room'
RANGE_LABELS = {
    'room': 'Room',
    'fridge': 'Fridge',
    'deep_freezer': 'Deep Freezer',
}

def now_local() -> datetime:
    # store naive datetime aligned to configured timezone so DB timestamps remain consistent
    return datetime.now(APP_TIMEZONE).replace(tzinfo=None)

def _clamp(value: float, low: float | None, high: float | None) -> float:
    if low is not None:
        value = max(value, low)
    if high is not None:
        value = min(value, high)
    return value

def _random_in_range(center: float | None, variance: float | None, low: float | None, high: float | None) -> float:
    if center is None:
        if low is not None and high is not None:
            center = (low + high) / 2
        else:
            center = 22.0
    if variance is None:
        if low is not None and high is not None:
            variance = max(0.1, (high - low) / 12)
        else:
            variance = 0.5
    value = random.uniform(center - variance, center + variance)
    return _clamp(value, low, high)

def normalize_room_key(name: str) -> str:
    return (name or '').strip().lower()

def get_room_source(room_name: str) -> Dict[str, Any]:
    normalized = normalize_room_key(room_name)
    room = ROOM_LOOKUP.get(normalized)
    if room:
        return room
    return {
        'name': room_name or 'Room',
        'location': room_name or 'Unknown',
        'target': None,
        'baseline': 22.0,
        'variance': 0.5,
        'humidity_baseline': 52.0,
        'humidity_variance': 2.0,
        'table': DEFAULT_TABLE_NAME,
        'db': DEFAULT_DB_NAME,
        'room_column': 'mfredg',
        'room_value': room_name,
        'include_humidity': True,
        'range_type': DEFAULT_RANGE_TYPE,
    }
def get_db_engine(db_name: str):
    if db_name in ENGINE_CACHE:
        return ENGINE_CACHE[db_name]
    url = URL.create(
        drivername="mysql+mysqlconnector",
        username=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        database=db_name,
    )
    engine = create_engine(url, pool_pre_ping=True, echo=False)
    ENGINE_CACHE[db_name] = engine
    return engine

def _select_room_configs(room_name: str | None = None, table_name: str | None = None) -> List[Dict[str, Any]]:
    if table_name:
        return [room for room in ROOM_DEFINITIONS if room.get('table') == table_name]
    if room_name:
        normalized = normalize_room_key(room_name)
        room = ROOM_LOOKUP.get(normalized)
        return [room] if room else []
    return list(ROOM_DEFINITIONS)

def _get_table_names() -> List[str]:
    tables = {room.get('table') for room in ROOM_DEFINITIONS if room.get('table')}
    return sorted(tables)

def _validate_dummy_token(request: Request) -> bool:
    if not DUMMY_INSERT_TOKEN:
        return False
    token = request.headers.get('x-dummy-token', '')
    return token == DUMMY_INSERT_TOKEN

def _build_dummy_payload(config: Dict[str, Any], ts: datetime | None = None) -> Dict[str, Any]:
    range_type = config.get('range_type', DEFAULT_RANGE_TYPE)
    low, high = SAFE_RANGE_BOUNDS.get(range_type, SAFE_RANGE_BOUNDS[DEFAULT_RANGE_TYPE])
    temperature = _random_in_range(config.get('baseline'), config.get('variance'), low, high)
    include_humidity = config.get('include_humidity', True)
    ts = ts or now_local()
    payload = {
        'temperature': round(temperature, 2),
        'timestamp': ts,
    }
    if include_humidity:
        humidity = _random_in_range(
            config.get('humidity_baseline'),
            config.get('humidity_variance'),
            0.0,
            100.0,
        )
        payload['humidity'] = round(humidity, 1)
    return payload

def insert_dummy_reading(config: Dict[str, Any]) -> bool:
    payload = _build_dummy_payload(config)
    engine = get_db_engine(config.get('db', DEFAULT_DB_NAME))
    table = config.get('table', DEFAULT_TABLE_NAME)
    room_column = config.get('room_column')
    room_value = config.get('room_value')
    skip_room_filter = config.get('skip_room_filter', False)

    # Skip insert if a record already exists for the same hour (per table + room filter if applicable)
    start_window = now_local().replace(minute=0, second=0, microsecond=0)
    end_window = start_window + timedelta(hours=1)
    where_clauses = ["timestamp >= :start_ts", "timestamp < :end_ts"]
    params: Dict[str, Any] = {"start_ts": start_window, "end_ts": end_window}
    if room_column and room_value and not skip_room_filter:
        where_clauses.append(f"{room_column} = :room_value")
        params["room_value"] = room_value
    check_sql = f"SELECT COUNT(*) AS cnt FROM `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` WHERE " + " AND ".join(where_clauses)
    with engine.connect() as conn:
        existing = conn.execute(text(check_sql), params).scalar()
    if existing and existing > 0:
        # If existing data within this window is out of the safe range, replace it with the dummy reading.
        range_type = config.get('range_type', DEFAULT_RANGE_TYPE)
        low, high = SAFE_RANGE_BOUNDS.get(range_type, SAFE_RANGE_BOUNDS[DEFAULT_RANGE_TYPE])
        fetch_sql = f"SELECT temperature FROM `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` WHERE " + " AND ".join(where_clauses)
        with engine.connect() as conn:
            rows = conn.execute(text(fetch_sql), params).all()
        temps = [row[0] for row in rows if row and row[0] is not None]

        def _is_out_of_range(value: float | None) -> bool:
            if value is None:
                return False
            if low is not None and value < low:
                return True
            if high is not None and value > high:
                return True
            return False

        out_of_range = any(_is_out_of_range(temp) for temp in temps)
        if not out_of_range:
            logger.info(
                "[dummy] skip insert for %s (table=%s) existing rows are within safe range",
                config.get('name'),
                table,
            )
            return False

        # Delete the out-of-range rows in this window so we can replace with dummy data
        delete_sql = f"DELETE FROM `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` WHERE " + " AND ".join(where_clauses)
        with engine.connect() as conn:
            conn.execute(text(delete_sql), params)
            conn.commit()
        logger.info(
            "[dummy] removed out-of-range data for %s (table=%s); inserting dummy row",
            config.get('name'),
            table,
        )

    def _try_insert(columns: List[str]) -> bool:
        cols = ", ".join(f"`{col}`" for col in columns)
        vals = ", ".join(f":{col}" for col in columns)
        stmt = text(f"INSERT INTO `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` ({cols}) VALUES ({vals})")
        params = {col: payload[col] for col in columns}
        with engine.connect() as conn:
            conn.execute(stmt, params)
            conn.commit()
        logger.info(
            "[dummy] inserted into %s.%s | cols=%s | data=%s",
            config.get('db', DEFAULT_DB_NAME),
            table,
            columns,
            {k: payload[k] for k in columns},
        )
        return True

    columns = list(payload.keys())
    try:
        return _try_insert(columns)
    except SQLAlchemyError as exc:
        error_msg = str(exc).lower()
        dropped = False
        for col in list(columns):
            if f"unknown column '{col.lower()}'" in error_msg or f"unknown column `{col.lower()}`" in error_msg:
                columns.remove(col)
                dropped = True
        if dropped and columns:
            try:
                return _try_insert(columns)
            except SQLAlchemyError as exc2:
                logger.warning(
                    "[dummy] insert failed after column drop for %s | cols=%s | err=%s",
                    config.get('name'),
                    columns,
                    exc2,
                )
                return False
        logger.warning(
            "[dummy] insert failed for %s | cols=%s | err=%s",
            config.get('name'),
            columns,
            exc,
        )
        return False


def insert_dummy_reading_at(config: Dict[str, Any], ts: datetime) -> bool:
    """Insert dummy reading for a specific timestamp, skipping if that hour already has data."""
    payload = _build_dummy_payload(config, ts=ts)
    engine = get_db_engine(config.get('db', DEFAULT_DB_NAME))
    table = config.get('table', DEFAULT_TABLE_NAME)
    room_column = config.get('room_column')
    room_value = config.get('room_value')
    skip_room_filter = config.get('skip_room_filter', False)

    start_window = ts.replace(minute=0, second=0, microsecond=0)
    end_window = start_window + timedelta(hours=1)
    where_clauses = ["timestamp >= :start_ts", "timestamp < :end_ts"]
    params: Dict[str, Any] = {"start_ts": start_window, "end_ts": end_window}
    if room_column and room_value and not skip_room_filter:
        where_clauses.append(f"{room_column} = :room_value")
        params["room_value"] = room_value
    check_sql = f"SELECT COUNT(*) AS cnt FROM `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` WHERE " + " AND ".join(where_clauses)
    with engine.connect() as conn:
        existing = conn.execute(text(check_sql), params).scalar()
    if existing and existing > 0:
        range_type = config.get('range_type', DEFAULT_RANGE_TYPE)
        low, high = SAFE_RANGE_BOUNDS.get(range_type, SAFE_RANGE_BOUNDS[DEFAULT_RANGE_TYPE])
        fetch_sql = f"SELECT temperature FROM `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` WHERE " + " AND ".join(where_clauses)
        with engine.connect() as conn:
            rows = conn.execute(text(fetch_sql), params).all()
        temps = [row[0] for row in rows if row and row[0] is not None]

        def _is_out_of_range(value: float | None) -> bool:
            if value is None:
                return False
            if low is not None and value < low:
                return True
            if high is not None and value > high:
                return True
            return False

        out_of_range = any(_is_out_of_range(temp) for temp in temps)
        if not out_of_range:
            logger.info(
                "[dummy] skip backfill for %s (table=%s) ts=%s; existing rows within safe range",
                config.get('name'),
                table,
                start_window.isoformat(),
            )
            return False

        delete_sql = f"DELETE FROM `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` WHERE " + " AND ".join(where_clauses)
        with engine.connect() as conn:
            conn.execute(text(delete_sql), params)
            conn.commit()
        logger.info(
            "[dummy] replaced out-of-range data for %s (table=%s) ts=%s; inserting dummy row",
            config.get('name'),
            table,
            start_window.isoformat(),
        )

    columns = list(payload.keys())

    def _try_insert(columns: List[str]) -> bool:
        cols = ", ".join(f"`{col}`" for col in columns)
        vals = ", ".join(f":{col}" for col in columns)
        stmt = text(f"INSERT INTO `{config.get('db', DEFAULT_DB_NAME)}`.`{table}` ({cols}) VALUES ({vals})")
        params = {col: payload[col] for col in columns}
        with engine.connect() as conn:
            conn.execute(stmt, params)
            conn.commit()
        logger.info(
            "[dummy] backfill inserted into %s.%s | ts=%s | cols=%s | data=%s",
            config.get('db', DEFAULT_DB_NAME),
            table,
            payload['timestamp'],
            columns,
            {k: payload[k] for k in columns},
        )
        return True

    try:
        return _try_insert(columns)
    except SQLAlchemyError as exc:
        error_msg = str(exc).lower()
        dropped = False
        for col in list(columns):
            if f"unknown column '{col.lower()}'" in error_msg or f"unknown column `{col.lower()}`" in error_msg:
                columns.remove(col)
                dropped = True
        if dropped and columns:
            try:
                return _try_insert(columns)
            except SQLAlchemyError as exc2:
                logger.warning(
                    "[dummy] backfill failed after column drop for %s | cols=%s | err=%s",
                    config.get('name'),
                    columns,
                    exc2,
                )
                return False
        logger.warning(
            "[dummy] backfill failed for %s | cols=%s | err=%s",
            config.get('name'),
            columns,
            exc,
        )
        return False

def insert_dummy_for_targets(room_name: str | None = None, table_name: str | None = None) -> Dict[str, Any]:
    configs = _select_room_configs(room_name, table_name)
    if not configs:
        return {'inserted': 0, 'targets': 0}
    inserted = 0
    for config in configs:
        if insert_dummy_reading(config):
            inserted += 1
    return {'inserted': inserted, 'targets': len(configs)}
def fetch_readings_from_db(
    room_name: str,
    start_dt: datetime | None,
    end_dt: datetime | None,
    limit: int | None = None,
    offset: int | None = None,
) -> List[Dict[str, Any]]:
    source = get_room_source(room_name)
    engine = get_db_engine(source.get('db', DEFAULT_DB_NAME))
    clause: List[str] = []
    params: Dict[str, Any] = {}
    skip_room_filter = source.get('skip_room_filter', False)
    if not skip_room_filter:
        room_column = source.get('room_column')
        room_value = source.get('room_value') or room_name
        if room_column and room_value:
            clause.append(f'{room_column} = :room_value')
            params['room_value'] = room_value
        elif room_name:
            clause.append('mfredge = :room')
            params['room'] = room_name
    if start_dt:
        clause.append('timestamp >= :start_dt')
        params['start_dt'] = start_dt
    if end_dt:
        clause.append('timestamp <= :end_dt')
        params['end_dt'] = end_dt
    include_humidity = source.get('include_humidity', True)
    columns = ['id', 'temperature']
    if include_humidity:
        columns.append('humidity')
    columns.append('timestamp')
    table = source.get('table', DEFAULT_TABLE_NAME)

    def run_query(cols: List[str]):
        stmt = (
            f"SELECT {', '.join(cols)} "
            f"FROM `{source.get('db', DEFAULT_DB_NAME)}`.`{table}` "
        )
        if clause:
            stmt += f"WHERE {' AND '.join(clause)} "
        stmt += 'ORDER BY timestamp DESC'
        if limit is not None:
            stmt += ' LIMIT :limit'
            params['limit'] = limit
        if offset is not None:
            stmt += ' OFFSET :offset'
            params['offset'] = offset
        with engine.connect() as conn:
            return conn.execute(text(stmt), params).mappings().all()

    try:
        rows = run_query(columns)
    except SQLAlchemyError as exc:
        error_msg = str(exc).lower()
        if include_humidity and 'unknown column' in error_msg and 'humidity' in error_msg:
            columns = [col for col in columns if col != 'humidity']
            try:
                rows = run_query(columns)
            except SQLAlchemyError as exc2:
                logger.warning('Failed to load readings for %s: %s', room_name, exc2)
                return []
        else:
            logger.warning('Failed to load readings for %s: %s', room_name, exc)
            return []
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return [
        {
            'id': row['id'],
            'temperature': _to_float(row.get('temperature')),
            'humidity': _to_float(row.get('humidity')),
            'timestamp': row['timestamp'],
        }
        for row in rows
    ]
def build_room_snapshot(
    config: Dict[str, Any],
    *,
    enable_ticket_notifications: bool = False,
) -> Dict[str, Any]:
    readings = fetch_readings_from_db(config['name'], None, None, limit=50)
    if not readings:
        return {
            'name': config['name'],
            'location': config['location'],
            'target': config['target'],
            'encoded_name': quote(config['name'], safe=''),
            'current': None,
            'trend': None,
            'status': 'No data',
            'badge': 'badge-muted',
            'avg': None,
            'max': None,
            'min': None,
            'humidity': None,
            'hourly': [],
            'history': [],
            'range': 1,
            'is_stale': True,
            'latest_timestamp': None,
            'stale_message': 'No readings yet',
            'temp_alert': False,
            'temp_alert_message': '',
        }

    history = []
    for row in reversed(readings):
        history.append(
            {
                'timestamp': row['timestamp'],
                'temp': round(row['temperature'], 1),
                'humidity': round(row['humidity'], 1) if row['humidity'] is not None else None,
                'time': row['timestamp'].strftime('%d %b %I %p'),
                'hour_label': row['timestamp'].strftime('%I %p'),
                'iso': row['timestamp'].isoformat(),
            }
        )
    latest = readings[0]
    latest_ts = latest['timestamp']
    age = now_local() - latest_ts
    is_stale = age >= timedelta(minutes=STALE_THRESHOLD_MINUTES)
    stale_message = (
        f'No new readings since {latest_ts.strftime("%d %b %Y %I:%M %p")}'
        if is_stale
        else ''
    )
    ticket_delay_met = age >= timedelta(minutes=STALE_TICKET_DELAY_MINUTES)
    if enable_ticket_notifications and ticket_delay_met and stale_message:
        create_stale_sensor_ticket(config['name'], stale_message, department=config['name'])
    delta = None
    if len(readings) > 1:
        delta = round(latest['temperature'] - readings[1]['temperature'], 1)
    current_temp = round(latest['temperature'], 1)
    current_humidity = round(latest['humidity'], 1) if latest['humidity'] is not None else None
    temps = [point['temp'] for point in history]
    humidity_vals = [point['humidity'] for point in history if point['humidity'] is not None]
    hourly = history[-10:] if history else []
    range_type = config.get('range_type', DEFAULT_RANGE_TYPE)
    low, high = SAFE_RANGE_BOUNDS.get(range_type, SAFE_RANGE_BOUNDS[DEFAULT_RANGE_TYPE])
    safe_low, safe_high = low, high
    range_label = RANGE_LABELS.get(range_type, RANGE_LABELS[DEFAULT_RANGE_TYPE])
    status, badge = classify_room(current_temp, range_type)
    temp_alert = False
    temp_alert_message = ''
    if current_temp is not None:
        if high is not None and current_temp > high:
            temp_alert = True
            if range_type == 'deep_freezer':
                temp_alert_message = f'Current {current_temp:.1f}C exceeds safe deep freezer limit (<= {high}C)'
            else:
                temp_alert_message = f'Current {current_temp:.1f}C outside expected {low:.1f}C - {high:.1f}C range'
        elif low is not None and current_temp < low:
            temp_alert = True
            if range_type == 'deep_freezer':
                temp_alert_message = f'Current {current_temp:.1f}C is below expected deep freezer lower bound ({low}C)'
            else:
                temp_alert_message = f'Current {current_temp:.1f}C outside expected {low:.1f}C - {high:.1f}C range'
    latest_temp = latest.get('temperature')
    sensor_failure = False
    sensor_alert_message = ''

    # Rule 1: last 3 entries missing/invalid -> sensor not working
    last_three = readings[:3] if readings else []
    missing_last_three = last_three and all(row.get('temperature') is None for row in last_three)
    if missing_last_three:
        sensor_failure = True
        sensor_alert_message = f"{config['name']} sensor not working (last 3 readings missing)"
    # Rule 2: latest reading outside safe range
    elif latest_temp is not None and (latest_temp < low or latest_temp > high):
        sensor_failure = True
        sensor_alert_message = (
            f"{config['name']} sensor reading {latest_temp:.1f}°C outside expected "
            f"{low:.1f}°C - {high:.1f}°C"
        )

    if sensor_failure and enable_ticket_notifications and sensor_alert_message:
        create_stale_sensor_ticket(config['name'], sensor_alert_message, department=config['name'])
    range_issue = False
    range_ticket_message = ''
    if low is not None and high is not None:
        consecutive = 0
        for row in readings[: min(CONSECUTIVE_OUTSIDE_THRESHOLD, len(readings))]:
            value = row['temperature']
            if value is None:
                break
            if value < low or value > high:
                consecutive += 1
            else:
                break
        range_issue = consecutive >= CONSECUTIVE_OUTSIDE_THRESHOLD
        if range_issue:
            latest_value = latest['temperature']
            range_ticket_message = (
                f"{config['name']} readings {latest_value:.1f}°C outside expected "
                f"{low:.1f}°C - {high:.1f}°C for {CONSECUTIVE_OUTSIDE_THRESHOLD} consecutive updates"
            )
            if enable_ticket_notifications:
                create_stale_sensor_ticket(config['name'], range_ticket_message, department=config['name'])
    return {
        'name': config['name'],
        'location': config['location'],
        'target': config['target'],
        'encoded_name': quote(config['name'], safe=''),
        'current': current_temp,
        'trend': delta,
        'status': status,
        'badge': badge,
        'range_label': range_label,
        'safe_low': safe_low,
        'safe_high': safe_high,
        'is_stale': is_stale,
        'latest_timestamp': latest_ts,
        'stale_message': stale_message,
        'avg': round(sum(temps) / len(temps), 1) if temps else current_temp,
        'max': max(temps) if temps else current_temp,
        'min': min(temps) if temps else current_temp,
        'humidity': round(sum(humidity_vals) / len(humidity_vals), 1) if humidity_vals else current_humidity,
        'hourly': hourly,
        'history': history,
        'range_type': range_type,
        'range': (max(temps) - min(temps)) if temps else 1,
        'temp_alert': temp_alert,
        'temp_alert_message': temp_alert_message,
        'sensor_failure': sensor_failure,
        'sensor_alert_message': sensor_alert_message,
    }
def build_rooms_payload() -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    rooms: List[Dict[str, Any]] = [build_room_snapshot(defn) for defn in ROOM_DEFINITIONS]
    temps = [room['current'] for room in rooms if room['current'] is not None]
    humidities = [room['humidity'] for room in rooms if room['humidity'] is not None]
    last_updated_ts = max((history[-1]['timestamp'] for room in rooms if room['history'] for history in [room['history']]), default=datetime.now())
    summary = {
        'rooms': len(rooms),
        'avg_temp': round(sum(temps) / len(temps), 1) if temps else 0,
        'max_temp': max(temps) if temps else 0,
        'min_temp': min(temps) if temps else 0,
        'avg_humidity': round(sum(humidities) / len(humidities), 1) if humidities else 0,
        'hours_tracked': max((len(room['history']) for room in rooms), default=0),
        'last_updated': last_updated_ts.strftime('%d %b %Y, %I:%M %p'),
    }
    max_window = max((len(room['history']) for room in rooms if room['history']), default=0)
    window = min(10, max_window)
    hourly_rollup: List[Dict[str, Any]] = []
    for idx in range(window):
        temps_at_idx = [room['history'][idx]['temp'] for room in rooms if len(room['history']) > idx]
        label = next((room['history'][idx]['hour_label'] for room in rooms if len(room['history']) > idx), '')
        hourly_rollup.append(
            {
                'label': label,
                'avg': round(sum(temps_at_idx) / len(temps_at_idx), 1) if temps_at_idx else 0,
                'peak': max(temps_at_idx) if temps_at_idx else 0,
            }
        )
    return rooms, summary, hourly_rollup


def scan_rooms_for_alerts() -> None:
    """Evaluate all configured rooms and raise infra tickets when needed."""
    logger.info("Running scheduled room scan for %s rooms", len(ROOM_DEFINITIONS))
    for config in ROOM_DEFINITIONS:
        try:
            build_room_snapshot(config, enable_ticket_notifications=True)
        except Exception:
            logger.exception("Failed to evaluate room %s", config.get('name'))
def classify_room(temp: float | None, range_type: str | None) -> Tuple[str, str]:
    if temp is None:
        return 'No data', 'badge-muted'
    low, high = SAFE_RANGE_BOUNDS.get(range_type or DEFAULT_RANGE_TYPE, SAFE_RANGE_BOUNDS[DEFAULT_RANGE_TYPE])
    if low is not None and temp < low:
        return 'Out of Range', 'badge-oor'
    if high is not None and temp > high:
        return 'Out of Range', 'badge-oor'
    return 'In Range', 'badge-inrange'

@router.get("/temperature", response_class=HTMLResponse)
def temperature_dashboard(request: Request, room: str = '') -> HTMLResponse:
    rooms, summary, hourly_rollup = build_rooms_payload()
    room_filter = room.strip()
    available_rooms = [r['name'] for r in rooms]
    if room_filter:
        filtered = [r for r in rooms if r['name'].lower() == room_filter.lower()]
        if filtered:
            rooms = filtered
            current_values = [r['current'] for r in rooms]
            summary = {
                'rooms': len(rooms),
                'avg_temp': round(sum(current_values) / len(current_values), 1),
                'max_temp': max(current_values),
                'min_temp': min(current_values),
                'avg_humidity': round(sum(r['humidity'] for r in rooms if r['humidity'] is not None) / len(rooms), 1) if rooms else 0,
                'hours_tracked': len(rooms[0]['history']),
                'last_updated': summary['last_updated'],
            }
            hourly_rollup = []
        else:
            rooms = []
            hourly_rollup = []
            summary = {
                'rooms': 0,
                'avg_temp': 0,
                'max_temp': 0,
                'min_temp': 0,
                'avg_humidity': 0,
                'hours_tracked': 0,
                'last_updated': summary['last_updated'],
            }
    return templates.TemplateResponse(
        'temperature_dashboard.html',
        {
            'request': request,
            'rooms': rooms,
            'summary': summary,
            'hourly_rollup': hourly_rollup,
            'available_rooms': available_rooms,
            'room_filter': room_filter,
            'dummy_tables': _get_table_names(),
        },
    )
def _parse_dt(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        return None

@router.get("/temperature/history/{room_name}", response_class=HTMLResponse)
def temperature_history(
    request: Request,
    room_name: str,
    search: str = '',
    start: str | None = Query(None, description='ISO date or datetime'),
    end: str | None = Query(None, description='ISO date or datetime'),
    page: int = 1,
    per_page: int = 10,
) -> HTMLResponse:
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    readings = fetch_readings_from_db(room_name, start_dt, end_dt)
    if not readings:
        return RedirectResponse(url='/temperature', status_code=302)
    history = [
        {
            'id': row['id'],
            'temp': round(row['temperature'], 1),
            'humidity': round(row['humidity'], 1) if row['humidity'] is not None else None,
            'time': row['timestamp'].strftime('%d %b %I %p'),
            'display_time': row['timestamp'].strftime('%d %b %Y %I:%M %p'),
            'iso': row['timestamp'].isoformat(),
            'hour_label': row['timestamp'].strftime('%I %p'),
        }
        for row in readings
    ]
    scoped_history = history
    query = (search or '').strip().lower()
    if query:
        filtered_history = [
            point for point in scoped_history
            if query in str(point['id']).lower()
            or query in point['time'].lower()
            or query in f"{point['temp']:.1f}".lower()
            or (point['humidity'] is not None and query in f"{point['humidity']:.1f}".lower())
        ]
    else:
        filtered_history = scoped_history
    page = max(1, page)
    per_page = max(5, min(per_page, 200))
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paged_history = filtered_history[start_idx:end_idx]
    total_filtered = len(filtered_history)
    total_pages = (total_filtered + per_page - 1) // per_page if filtered_history else 1
    room_meta = get_room_source(room_name)
    current_temp = history[0]['temp'] if history else 0
    target_temp = room_meta.get('target') if room_meta.get('target') is not None else current_temp
    return templates.TemplateResponse(
        'temperature_history.html',
        {
            'request': request,
            'room': {
                'name': room_meta['name'],
                'location': room_meta['location'],
                'target': target_temp,
                'current': current_temp,
            },
            'target_temp': target_temp,
            'history': paged_history,
            'start': start or '',
            'end': end or '',
            'search': search,
            'page': page,
            'total_pages': total_pages,
            'per_page': per_page,
            'total_records': total_filtered,
        },
    )
@router.get("/api/temperature")
def api_temperature(
    room: str = '',
    start: str | None = Query(None, description='ISO date/time'),
    end: str | None = Query(None, description='ISO date/time'),
    page: int = 1,
    per_page: int = 50,
    search: str = '',
) -> Dict[str, Any]:
    page = max(1, page)
    per_page = max(5, min(per_page, 200))
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    readings = fetch_readings_from_db(room or '', start_dt, end_dt)
    query = (search or '').strip().lower()
    history = [
        {
            'id': row['id'],
            'temp': round(row['temperature'], 1),
            'humidity': row['humidity'],
            'time': row['timestamp'].strftime('%d %b %I %p'),
            'timestamp': row['timestamp'].isoformat(),
        }
        for row in readings
    ]
    if query:
        filtered = [
            point for point in history
            if query in str(point['id']).lower()
            or query in point['time'].lower()
            or query in f"{point['temp']:.1f}".lower()
            or (point['humidity'] is not None and query in f"{point['humidity']:.1f}".lower())
        ]
    else:
        filtered = history
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paged_history = filtered[start_idx:end_idx]
    total_filtered = len(filtered)
    return {
        'page': page,
        'per_page': per_page,
        'total': total_filtered,
        'data': [
            {
                'id': point['id'],
                'temperature': point['temp'],
                'humidity': point['humidity'],
                'timestamp': point['timestamp'],
                'room': (room or '').strip() or room,
            }
            for point in paged_history
        ],
    }

@router.get("/api/temperature/latest")
def api_temperature_latest(room: str = '') -> Dict[str, Any]:
    rows = fetch_readings_from_db(room or '', None, None, limit=1, offset=0)
    if not rows:
        return {'data': None}
    row = rows[0]
    return {
        'data': {
            'id': row['id'],
            'temperature': row['temperature'],
            'humidity': row['humidity'],
            'timestamp': row['timestamp'].isoformat(),
            'room': (room or '').strip() or room,
        }
    }

@router.post("/api/dummy/insert")
def api_dummy_insert(
    request: Request,
    room: str | None = Body(None, embed=True),
    table: str | None = Body(None, embed=True),
) -> Dict[str, Any]:
    if not DUMMY_INSERT_TOKEN:
        return {'error': 'dummy insert token not configured'}
    token = request.headers.get('x-dummy-token', '')
    if token != DUMMY_INSERT_TOKEN:
        return {'error': 'unauthorized'}
    result = insert_dummy_for_targets(room_name=room, table_name=table)
    return {
        'inserted': result['inserted'],
        'targets': result['targets'],
        'room': room or '',
        'table': table or '',
    }

@router.get("/api/dummy/scheduler/status")
def api_dummy_scheduler_status(request: Request) -> Dict[str, Any]:
    if not _validate_dummy_token(request):
        return {'error': 'unauthorized'}
    state = get_dummy_scheduler_state()
    return {
        'enabled': state['enabled'],
        'interval_minutes': state['interval_minutes'],
        'interval_seconds': state['interval_seconds'],
        'table_flags': state['table_flags'],
        'tables': _get_table_names(),
    }

@router.post("/api/dummy/scheduler/toggle")
def api_dummy_scheduler_toggle(
    request: Request,
    enabled: bool = Body(..., embed=True),
) -> Dict[str, Any]:
    if not _validate_dummy_token(request):
        return {'error': 'unauthorized'}
    set_dummy_scheduler_enabled(enabled)
    if enabled:
        start_dummy_scheduler(force=True)
    state = get_dummy_scheduler_state()
    return {'enabled': state['enabled']}

@router.post("/api/dummy/scheduler/table")
def api_dummy_scheduler_table(
    request: Request,
    table: str = Body(..., embed=True),
    enabled: bool = Body(..., embed=True),
) -> Dict[str, Any]:
    if not _validate_dummy_token(request):
        return {'error': 'unauthorized'}
    if table not in _get_table_names():
        return {'error': 'unknown table'}
    set_dummy_table_enabled(table, enabled)
    state = get_dummy_scheduler_state()
    return {
        'table': table,
        'enabled': state['table_flags'].get(table, False),
        'table_flags': state['table_flags'],
    }

@router.get("/api/scheduler/status")
def api_scheduler_status(request: Request) -> Dict[str, Any]:
    if not _validate_dummy_token(request):
        return {'error': 'unauthorized'}
    alert_state = get_alert_scheduler_state()
    dummy_state = get_dummy_scheduler_state()
    return {
        'alert_enabled': alert_state['enabled'],
        'alert_interval_minutes': alert_state['interval_minutes'],
        'dummy_enabled': dummy_state['enabled'],
        'dummy_interval_minutes': dummy_state['interval_minutes'],
        'dummy_interval_seconds': dummy_state['interval_seconds'],
        'table_flags': dummy_state['table_flags'],
        'tables': _get_table_names(),
    }

@router.post("/api/scheduler/alert/toggle")
def api_scheduler_alert_toggle(
    request: Request,
    enabled: bool = Body(..., embed=True),
) -> Dict[str, Any]:
    if not _validate_dummy_token(request):
        return {'error': 'unauthorized'}
    set_alert_scheduler_enabled(enabled)
    if enabled:
        start_alert_scheduler(force=True)
    state = get_alert_scheduler_state()
    return {'enabled': state['enabled']}
