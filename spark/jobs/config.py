import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/appdb")
PG_HOST = os.getenv("PGHOST", "postgres")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "appdb")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "postgres")
