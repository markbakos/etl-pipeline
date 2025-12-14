import os
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when, isnull
from pyspark.sql.types import StructType, StructField, StringType, LongType


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("CUSTOMERS_TOPIC", "dbserver1.public.customers")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/customers_console")
DLQ_CHECKPOINT_DIR = os.getenv("DLQ_CHECKPOINT_DIR", "/tmp/spark-checkpoints/customers_dlq")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/appdb")
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
            col("op"),
            col("is_deleted"),
            to_timestamp((col("ts_ms") / 1000).cast("double")).alias("event_time"),
            col("row.id").alias("customer_id"),
            col("row.email"),
            col("row.country"),
            to_timestamp(col("row.created_at")).alias("created_at"),
            to_timestamp(col("row.updated_at")).alias("updated_at"),
            col("v.source.lsn").alias("source_lsn"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
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

    query = (
        flattened.writeStream
        .format("console")
        .option("truncate", "false")
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

