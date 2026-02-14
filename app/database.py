# app/database.py

import os
from dataclasses import dataclass
from typing import Dict

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import declarative_base, sessionmaker


@dataclass(frozen=True)
class DatabaseConfig:
    username: str
    password: str
    host: str
    port: int
    database: str
    driver: str = "mysql+mysqlconnector"

    @property
    def url(self) -> URL:
        return URL.create(
            drivername=self.driver,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    @classmethod
    def from_env(
        cls,
        prefix: str = "MYSQL",
        defaults: Dict[str, str] | None = None,
    ) -> "DatabaseConfig":
        defaults = defaults or {}
        def _env(name: str, fallback: str) -> str:
            return os.getenv(f"{prefix}_{name}", defaults.get(name, fallback))

        return cls(
            username=_env("USER", "sahil"),
            password=_env("PASSWORD", "sahil@123"),
            host=_env("HOST", "192.168.0.167"),
            port=int(_env("PORT", "3306")),
            database=_env("DB", "temraturerec"),
            driver=_env("DRIVER", "mysql+mysqlconnector"),
        )


ENGINE_REGISTRY: Dict[str, Engine] = {}


def get_engine(
    config: DatabaseConfig,
    *,
    label: str | None = None,
    pool_pre_ping: bool = True,
    echo: bool = True,
) -> Engine:
    key = label or f"{config.driver}:{config.host}:{config.port}:{config.database}:{config.username}"
    if key not in ENGINE_REGISTRY:
        ENGINE_REGISTRY[key] = create_engine(
            config.url,
            pool_pre_ping=pool_pre_ping,
            echo=echo,
        )
    return ENGINE_REGISTRY[key]


DEFAULT_DB_CONFIG = DatabaseConfig.from_env()

MYSQL_USER = DEFAULT_DB_CONFIG.username
MYSQL_PASSWORD = DEFAULT_DB_CONFIG.password
MYSQL_HOST = DEFAULT_DB_CONFIG.host
MYSQL_PORT = str(DEFAULT_DB_CONFIG.port)
MYSQL_DB = DEFAULT_DB_CONFIG.database


DATABASE_URL = DEFAULT_DB_CONFIG.url

engine = get_engine(DEFAULT_DB_CONFIG)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# help connect a second MySQL deployment (override via SECONDARY_MYSQL_*
# env vars or supply defaults below)
SECONDARY_DB_DEFAULTS = {
    "USER": "root",
    "PASSWORD": "",
    "HOST": "192.168.0.110",
    "PORT": "3306",
    "DB": "it_tickets",
}

SECONDARY_DB_CONFIG = DatabaseConfig.from_env(
    prefix="SECONDARY_MYSQL",
    defaults=SECONDARY_DB_DEFAULTS,
)

SECONDARY_ENGINE = get_engine(SECONDARY_DB_CONFIG, label="secondary")

SecondarySessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=SECONDARY_ENGINE,
)


def get_secondary_db():
  
    db = SecondarySessionLocal()
    try:
        yield db
    finally:
        db.close()
