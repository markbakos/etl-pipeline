from pyspark.sql.types import StructType, StructField, StringType, LongType

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

customer_payload_schema = StructType([
    StructField("id", LongType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

order_payload_schema = StructType([
    StructField("id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

def debezium_value_schema(payload_schema: StructType) -> StructType:
    return StructType([
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("before", payload_schema, True),
        StructField("after", payload_schema, True),
        StructField("source", source_schema, True),
    ])
