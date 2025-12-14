import os
import sys
from pathlib import Path

jobs_dir = Path(__file__).parent
if str(jobs_dir) not in sys.path:
    sys.path.insert(0, str(jobs_dir))

from pyspark.sql import SparkSession
from config import KAFKA_BOOTSTRAP
from schemas import order_payload_schema, debezium_value_schema
from transforms import parse_debezium, split_valid_invalid, flatten_orders
from sinks import write_dlq, write_orders_warehouse

TOPIC = os.getenv("ORDERS_TOPIC", "dbserver1.public.orders")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/orders_warehouse")
DLQ_CHECKPOINT_DIR = os.getenv("DLQ_CHECKPOINT_DIR", "/tmp/spark-checkpoints/orders_dlq")

def main():
    spark = SparkSession.builder.appName("orders-cdc-warehouse").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    for module in ['config.py', 'schemas.py', 'transforms.py', 'sinks.py']:
        module_path = jobs_dir / module
        if module_path.exists():
            spark.sparkContext.addPyFile(str(module_path))

    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = parse_debezium(raw, debezium_value_schema(order_payload_schema))
    valid_df, invalid_df = split_valid_invalid(parsed)
    orders_df = flatten_orders(valid_df)

    dlq_query = (invalid_df.writeStream
        .foreachBatch(write_dlq)
        .option("checkpointLocation", DLQ_CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    warehouse_query = (orders_df.writeStream
        .foreachBatch(write_orders_warehouse)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    warehouse_query.awaitTermination()
    dlq_query.awaitTermination()

if __name__ == "__main__":
    main()
