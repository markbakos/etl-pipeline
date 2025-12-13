import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("ORDERS_TOPIC", "dbserver1.public.orders")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/orders_console")

order_payload_schema = StructType([
    StructField("id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

debezium_value_schema = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("before", order_payload_schema, True),
    StructField("after", order_payload_schema, True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("orders-cdc-console")
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

    parsed = raw.withColumn("v", from_json(json_str, debezium_value_schema))

    flattened = (
        parsed
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
            col("row.id").alias("order_id"),
            col("row.customer_id"),
            col("row.status"),
            col("row.amount").cast(DecimalType(12, 2)).alias("amount"),
            col("row.currency"),
            to_timestamp(col("row.created_at")).alias("created_at"),
            to_timestamp(col("row.updated_at")).alias("updated_at"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
    )

    query = (
        flattened.writeStream
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
