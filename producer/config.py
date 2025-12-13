import os
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor

@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def env_db_config() -> DBConfig:
    return DBConfig(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "appdb"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres")
    )

def connect(db: DBConfig):
    return psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.dbname,
        user=db.user,
        password=db.password,
        cursor_factory=RealDictCursor
    )

