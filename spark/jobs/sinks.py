import psycopg2
from typing import Iterable
from pyspark.sql import DataFrame
from config import PG_URL, PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

def write_dlq(batch_df: DataFrame, _batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    (batch_df.write
        .format("jdbc")
        .mode("append")
        .option("url", PG_URL)
        .option("dbtable", "dlq_events")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .save()
    )

def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )

def _upsert_customers_partition(rows: Iterable):
    conn = _pg_conn()
    cur = conn.cursor()
    sql = """
    INSERT INTO dim_customers (customer_id, email, country, first_seen_at, last_seen_at, source_lsn)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO UPDATE SET
        email = EXCLUDED.email,
        country = EXCLUDED.country,
        first_seen_at = LEAST(dim_customers.first_seen_at, EXCLUDED.first_seen_at),
        last_seen_at  = GREATEST(dim_customers.last_seen_at, EXCLUDED.last_seen_at),
        source_lsn    = EXCLUDED.source_lsn
    WHERE dim_customers.source_lsn IS NULL OR EXCLUDED.source_lsn > dim_customers.source_lsn
    """
    batch = []
    for r in rows:
        first_seen = r.created_at
        last_seen = r.updated_at or r.created_at
        batch.append((r.customer_id, r.email, r.country, first_seen, last_seen, r.source_lsn))
        if len(batch) >= 500:
            cur.executemany(sql, batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)

    conn.commit()
    cur.close()
    conn.close()

def write_customers_warehouse(batch_df: DataFrame, _batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    batch_df.foreachPartition(_upsert_customers_partition)

def _upsert_orders_partition(rows: Iterable):
    conn = _pg_conn()
    cur = conn.cursor()
    sql = """
    INSERT INTO fact_orders (order_id, customer_id, status, amount_eur, created_at, updated_at, is_deleted, event_time, source_lsn)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        status      = EXCLUDED.status,
        amount_eur  = EXCLUDED.amount_eur,
        created_at  = EXCLUDED.created_at,
        updated_at  = EXCLUDED.updated_at,
        is_deleted  = EXCLUDED.is_deleted,
        event_time  = EXCLUDED.event_time,
        source_lsn  = EXCLUDED.source_lsn
    WHERE fact_orders.source_lsn IS NULL OR EXCLUDED.source_lsn > fact_orders.source_lsn
    """
    batch = []
    for r in rows:
        batch.append((r.order_id, r.customer_id, r.status, r.amount_eur,
                      r.created_at, r.updated_at, r.is_deleted,
                      r.event_time, r.source_lsn))
        if len(batch) >= 500:
            cur.executemany(sql, batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)

    conn.commit()
    cur.close()
    conn.close()

def write_orders_warehouse(batch_df: DataFrame, _batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    batch_df.foreachPartition(_upsert_orders_partition)
