import os
import sys
from pathlib import Path

jobs_dir = Path(__file__).parent
if str(jobs_dir) not in sys.path:
    sys.path.insert(0, str(jobs_dir))

from pyspark.sql import SparkSession
from config import KAFKA_BOOTSTRAP
from schemas import customer_payload_schema, debezium_value_schema
from transforms import parse_debezium, split_valid_invalid, flatten_customers
from sinks import write_dlq, write_customers_warehouse

TOPIC = os.getenv("CUSTOMERS_TOPIC", "dbserver1.public.customers")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/customers_warehouse")
DLQ_CHECKPOINT_DIR = os.getenv("DLQ_CHECKPOINT_DIR", "/tmp/spark-checkpoints/customers_dlq")

def main():
    spark = SparkSession.builder.appName("customers-cdc-warehouse").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    jobs_dir = Path(__file__).parent
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

    parsed = parse_debezium(raw, debezium_value_schema(customer_payload_schema))
    valid_df, invalid_df = split_valid_invalid(parsed)
    customers_df = flatten_customers(valid_df)

    dlq_query = (invalid_df.writeStream
        .foreachBatch(write_dlq)
        .option("checkpointLocation", DLQ_CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    warehouse_query = (customers_df.writeStream
        .foreachBatch(write_customers_warehouse)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    warehouse_query.awaitTermination()
    dlq_query.awaitTermination()

if __name__ == "__main__":
    main()

