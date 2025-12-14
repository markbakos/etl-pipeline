from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when, isnull
from pyspark.sql.types import DecimalType

def parse_debezium(raw_df: DataFrame, schema) -> DataFrame:
    json_str = col("value").cast("string")
    return raw_df.withColumn("raw_payload", json_str).withColumn(
        "v", from_json(json_str, schema)
    )

def split_valid_invalid(parsed_df: DataFrame):
    row_expr = when(col("v.op") == lit("d"), col("v.before")).otherwise(col("v.after"))

    is_invalid = (
        isnull(col("v")) |
        isnull(col("v.op")) |
        isnull(row_expr)
    )

    invalid_df = parsed_df.filter(is_invalid).select(
        col("topic"),
        col("partition"),
        col("offset").alias("kafka_offset"),
        lit("Failed to parse JSON or missing required fields").alias("error"),
        col("raw_payload"),
    )

    valid_df = parsed_df.filter(~is_invalid)
    return valid_df, invalid_df

def flatten_customers(valid_df: DataFrame) -> DataFrame:
    row_expr = when(col("v.op") == lit("d"), col("v.before")).otherwise(col("v.after"))

    return (valid_df
        .withColumn("ts_ms", col("v.ts_ms"))
        .withColumn("row", row_expr)
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

def flatten_orders(valid_df: DataFrame) -> DataFrame:
    row_expr = when(col("v.op") == lit("d"), col("v.before")).otherwise(col("v.after"))

    return (valid_df
        .withColumn("ts_ms", col("v.ts_ms"))
        .withColumn("op", col("v.op"))
        .withColumn("row", row_expr)
        .withColumn("is_deleted", (col("op") == lit("d")))
        .select(
            col("row.id").alias("order_id"),
            col("row.customer_id"),
            col("row.status"),
            col("row.amount").cast(DecimalType(12, 2)).alias("amount_eur"),
            to_timestamp(col("row.created_at")).alias("created_at"),
            to_timestamp(col("row.updated_at")).alias("updated_at"),
            col("is_deleted"),
            to_timestamp((col("ts_ms") / 1000).cast("double")).alias("event_time"),
            col("v.source.lsn").alias("source_lsn"),
        )
    )
