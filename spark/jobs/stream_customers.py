import os
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when, isnull
from pyspark.sql.types import StructType, StructField, StringType, LongType
import psycopg2


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("CUSTOMERS_TOPIC", "dbserver1.public.customers")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/customers_warehouse")
DLQ_CHECKPOINT_DIR = os.getenv("DLQ_CHECKPOINT_DIR", "/tmp/spark-checkpoints/customers_dlq")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/appdb")
PG_HOST = os.getenv("PGHOST", "postgres")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "appdb")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "postgres")

customer_payload_schema = StructType([
    StructField("id", LongType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

source_schema = StructType([
    StructField("version", StringType(), True),
    StructField("connector", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("db", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
    StructField("lsn", LongType(), True),
])

debezium_value_schema = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("before", customer_payload_schema, True),
    StructField("after", customer_payload_schema, True),
    StructField("source", source_schema, True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("customers-cdc-console")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_str = col("value").cast("string")
    raw_payload = json_str.alias("raw_payload")

    parsed = raw.withColumn("raw_payload", raw_payload).withColumn("v", from_json(json_str, debezium_value_schema))

    is_invalid = isnull(col("v")) | isnull(col("v.op")) | isnull(
        when(col("v.op") == lit("d"), col("v.before")).otherwise(col("v.after"))
    )

    invalid_records = parsed.filter(is_invalid).select(
        col("topic"),
        col("partition"),
        col("offset").alias("kafka_offset"),
        lit("Failed to parse JSON or missing required fields").alias("error"),
        col("raw_payload")
    )

    flattened = (
        parsed
        .filter(~is_invalid)
        .withColumn("op", col("v.op"))
        .withColumn("ts_ms", col("v.ts_ms"))
        .withColumn(
            "row",
            when(col("v.op") == lit("d"), col("v.before")).otherwise(col("v.after"))
        )
        .withColumn("is_deleted", (col("v.op") == lit("d")))
        .select(
            col("row.id").alias("customer_id"),
            col("row.email"),
            col("row.country"),
            to_timestamp(col("row.created_at")).alias("created_at"),
            to_timestamp(col("row.updated_at")).alias("updated_at"),
            to_timestamp((col("ts_ms") / 1000).cast("double")).alias("event_time"),
            col("v.source.lsn").alias("source_lsn"),
        )
    )

    def write_dlq(batch_df, batch_id):
        if batch_df.count() > 0:
            batch_df.write.format("jdbc").mode("append").option("url", PG_URL).option(
                "dbtable", "dlq_events"
            ).option("user", PG_USER).option("password", PG_PASSWORD).save()

    dlq_query = (
        invalid_records.writeStream
        .foreachBatch(write_dlq)
        .option("checkpointLocation", DLQ_CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    def write_warehouse(batch_df, batch_id):
        if batch_df.count() > 0:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASSWORD
            )
            cur = conn.cursor()
            rows = batch_df.collect()
            for row in rows:
                cur.execute("""
                    INSERT INTO dim_customers (customer_id, email, country, first_seen_at, last_seen_at, source_lsn)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        email = EXCLUDED.email,
                        country = EXCLUDED.country,
                        first_seen_at = LEAST(dim_customers.first_seen_at, EXCLUDED.first_seen_at),
                        last_seen_at = GREATEST(dim_customers.last_seen_at, EXCLUDED.last_seen_at),
                        source_lsn = EXCLUDED.source_lsn
                """, (
                    row.customer_id, row.email, row.country,
                    row.created_at, row.updated_at or row.created_at,
                    row.source_lsn
                ))
            conn.commit()
            cur.close()
            conn.close()

    query = (
        flattened.writeStream
        .foreachBatch(write_warehouse)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    def await_dlq():
        dlq_query.awaitTermination()

    threading.Thread(target=await_dlq, daemon=True).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()

