# Real-Time ETL Pipeline with Change Data Capture (CDC)

A production-ready, end-to-end ETL pipeline that ingests and transforms change data in real-time using PostgreSQL, Debezium, Apache Kafka, Apache Spark (PySpark), and PostgreSQL as a data warehouse.

## Overview

This pipeline demonstrates a complete CDC-based ETL architecture that:
- Captures database changes in real-time using Debezium
- Decouples producers and consumers via Apache Kafka
- Processes and transforms events using Apache Spark Structured Streaming
- Loads transformed data into a dimensional warehouse schema
- Handles errors gracefully with a Dead Letter Queue (DLQ)

## Architecture

```
┌─────────────────┐
│  Source DB      │
│  (PostgreSQL)   │
│  - customers    │
│  - orders       │
└────────┬────────┘
         │ WAL (Write-Ahead Log)
         │
         ▼
┌─────────────────┐
│   Debezium      │
│   CDC Connector │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Kafka   │
│  - Topics:      │
│    dbserver1.   │
│    public.*     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Spark   │
│  (PySpark)      │
│  - Parse JSON   │
│  - Transform    │
│  - Validate     │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌─────────┐ ┌──────────────┐
│Warehouse│ │  Dead Letter │
│(Postgres)│ │     Queue    │
│         │ │   (dlq_events)│
└─────────┘ └──────────────┘
```

### Component Flow

1. **Source Database**: PostgreSQL with logical replication enabled (`wal_level=logical`)
2. **CDC Layer**: Debezium PostgreSQL connector captures changes via PostgreSQL's logical replication
3. **Message Queue**: Kafka topics store CDC events (one topic per table)
4. **Processing Layer**: Spark Structured Streaming jobs consume from Kafka, parse, validate, and transform
5. **Warehouse**: PostgreSQL dimensional schema (star schema with `dim_customers` and `fact_orders`)
6. **Error Handling**: Invalid events are routed to a Dead Letter Queue table

## Components

### 1. Source Database (PostgreSQL)

**Schema**: `public.customers` and `public.orders`

- **customers**: Customer dimension with email, country, timestamps
- **orders**: Transactional orders linked to customers via foreign key

**Features**:
- Automatic `updated_at` triggers
- Logical replication enabled for CDC
- Simulated writes via Python producer script

### 2. Change Data Capture (CDC) - Debezium

**Choice**: Debezium PostgreSQL Connector

**Rationale**:
- **Low Latency**: Uses PostgreSQL's native logical replication (WAL), providing sub-second latency
- **Consistency**: Captures all changes in transaction order via LSN (Log Sequence Number)
- **Operational Simplicity**: Mature, production-ready tool with Kafka Connect integration
- **Schema Evolution**: Handles schema changes gracefully with JSON payloads

**Trade-offs**:
- **Latency**: ~100-500ms (excellent for most use cases)
- **Consistency**: Strong consistency within transactions, eventual consistency across partitions
- **Operational Complexity**: Requires PostgreSQL replication slots, monitoring of slot lag
- **Resource Usage**: Minimal impact on source DB (reads WAL, doesn't query tables)

**Configuration**:
- Uses `pgoutput` plugin (PostgreSQL 10+)
- Publishes to Kafka topics: `dbserver1.public.customers` and `dbserver1.public.orders`
- Includes source metadata (LSN, timestamp, operation type)

### 3. Message Queue (Apache Kafka)

**Delivery Semantics**: At-least-once delivery

**Strategies**:
- **Retries**: Kafka Connect retries failed sends automatically
- **Ordering**: Single partition per topic ensures ordering (can be scaled with partitioning strategy)
- **Deduplication**: Uses `source_lsn` (Log Sequence Number) for idempotent writes to warehouse
- **Offset Management**: Spark Structured Streaming manages Kafka offsets via checkpointing

**Configuration**:
- Single broker setup (development)
- JSON serialization (no schema registry for simplicity)
- Auto topic creation enabled

### 4. Data Processing (Apache Spark - PySpark)

**Framework**: Apache Spark Structured Streaming (PySpark)

**Processing Steps**:
1. **Ingestion**: Read from Kafka topics using Spark's Kafka connector
2. **Parsing**: Parse Debezium JSON payloads using Spark SQL schemas
3. **Validation**: Split valid/invalid records (missing fields, null operations)
4. **Transformation**:
   - Flatten nested Debezium structure
   - Extract operation type (insert/update/delete)
   - Convert timestamps and data types
   - Handle deletes (soft delete via `is_deleted` flag)
5. **Sink**: Write to warehouse using custom `foreachBatch` with upsert logic

**Error Handling**:
- Malformed JSON → DLQ
- Missing required fields → DLQ
- Schema mismatches → DLQ (with raw payload preserved)

**Schema Evolution**:
- Uses flexible JSON parsing with nullable fields
- Invalid records are captured in DLQ for manual review
- Can be extended with schema registry for stricter validation

### 5. Data Warehouse Sink (PostgreSQL)

**Schema Design**: Star Schema

- **dim_customers**: Customer dimension table
  - Tracks `first_seen_at` and `last_seen_at` for temporal analysis
  - Uses `source_lsn` for idempotent updates (only applies newer changes)
  
- **fact_orders**: Order fact table
  - Includes `is_deleted` flag for soft deletes
  - Stores `event_time` (CDC timestamp) and `source_lsn`
  - Idempotent upserts based on `source_lsn`

**Upsert Strategy**:
- Uses PostgreSQL `ON CONFLICT` with `source_lsn` comparison
- Only updates if incoming LSN is greater than existing (prevents replay issues)
- Batch writes (500 rows per batch) for performance

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for producer script)
- Network access to pull Docker images

### Quick Start

1. **Start all services and Spark jobs**:
   ```bash
   ./startup.sh
   ```

   This script:
   - Starts PostgreSQL, Kafka, Zookeeper, Kafka Connect, and Spark cluster
   - Waits for services to be ready
   - Registers the Debezium connector
   - Verifies setup
   - Automatically starts both Spark streaming jobs (customers and orders) in the background

2. **Simulate data writes**:
   ```bash
   ./run_simulate.sh
   ```

   Or with custom parameters:
   ```bash
   ./run_simulate.sh --sleep-ms 200 --insert-p 0.7 --update-p 0.25 --delete-p 0.05
   ```

### Manual Setup

If you prefer to set up manually:

1. **Start Docker services**:
   ```bash
   docker-compose up -d
   ```

2. **Wait for services** (30-60 seconds):
   ```bash
   # Check PostgreSQL
   docker exec etl_postgres pg_isready -U postgres -d appdb
   
   # Check Kafka Connect
   curl http://localhost:8083/
   ```

3. **Register Debezium connector**:
   ```bash
   python3 cdc/register_connector.py
   ```

4. **Verify connector status**:
   ```bash
   curl http://localhost:8083/connectors/postgres-cdc/status
   ```

5. **Submit Spark jobs** (optional - `startup.sh` does this automatically):

   Manually submit jobs:
   ```bash
   docker exec -it etl_spark_master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.1 \
     --conf spark.cores.max=2 \
     --conf spark.executor.cores=2 \
     --conf spark.executor.memory=512m \
     /opt/spark-apps/jobs/stream_customers.py
   ```

### Verify Pipeline

1. **Check Kafka topics**:
   ```bash
   docker exec etl_kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

2. **View Kafka messages**:
   ```bash
   docker exec etl_kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic dbserver1.public.orders \
     --from-beginning
   ```

3. **Query warehouse**:
   ```bash
   docker exec -it etl_postgres psql -U postgres -d appdb
   ```
   ```sql
   -- Check customers
   SELECT * FROM dim_customers LIMIT 10;
   
   -- Check orders
   SELECT * FROM fact_orders LIMIT 10;
   
   -- Check DLQ
   SELECT * FROM dlq_events ORDER BY created_at DESC LIMIT 10;
   ```

4. **Monitor Spark UI**:
   - Open http://localhost:8080 (Spark Master UI)
   - Open http://localhost:8081 (Spark Worker UI)
   - Verify both streaming jobs are running (should see "customers-cdc-warehouse" and "orders-cdc-warehouse" applications)

### Managing Spark Jobs

**Check if jobs are running**:
```bash
# View Spark applications
curl http://localhost:8080/api/v1/applications

# Or check logs
docker logs etl_spark_master | grep -i "streaming"
```

**Restart a job** (if needed):
```bash
# Stop all Spark jobs
docker exec etl_spark_master pkill -f spark-submit

# Restart customers job
docker exec -d etl_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.1 \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=512m \
  /opt/spark-apps/jobs/stream_customers.py

# Restart orders job
docker exec -d etl_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.1 \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=512m \
  /opt/spark-apps/jobs/stream_orders.py
```

**Note**: The `startup.sh` script automatically starts both jobs in the background. If you need to restart them, use the commands above or run `startup.sh` again.

## Design Choices & Rationale

### Why Debezium over Custom Polling?

- **Performance**: Logical replication is more efficient than polling (no table scans)
- **Completeness**: Captures all changes, including deletes and schema changes
- **Low Latency**: Sub-second change detection
- **Maturity**: Production-proven at scale (used by Netflix, LinkedIn, etc.)

### Why Kafka over Direct DB Connection?

- **Decoupling**: Producers (Debezium) and consumers (Spark) operate independently
- **Buffering**: Handles backpressure and consumer lag gracefully
- **Replayability**: Can reprocess historical events
- **Multi-consumer**: Multiple Spark jobs can read from same topics

### Why Spark Structured Streaming?

- **Unified API**: Same API for batch and streaming
- **Fault Tolerance**: Automatic checkpointing and recovery
- **Scalability**: Horizontal scaling via Spark cluster
- **Rich Ecosystem**: Built-in Kafka connector, SQL API, MLlib

### Why PostgreSQL as Warehouse?

- **Simplicity**: Single database for source and warehouse (easy to demo)
- **ACID Guarantees**: Reliable upserts with transactions
- **SQL Compatibility**: Standard SQL for analytics queries

**Note**: In production, consider BigQuery, Redshift, or Snowflake for better analytics performance.

### Why LSN-based Idempotency?

- **Ordering**: LSNs are monotonically increasing, ensuring correct update order
- **Replay Safety**: Re-running jobs won't corrupt data (only newer LSNs apply)
- **CDC Native**: LSN is part of Debezium payload, no custom logic needed

## Edge Cases & Limitations

### Handled Edge Cases

1. **Malformed JSON**: Routed to DLQ with error message
2. **Missing Fields**: Validation catches null required fields → DLQ
3. **Delete Operations**: Handled via `is_deleted` flag (soft deletes)
4. **Duplicate Events**: Idempotent upserts via `source_lsn` comparison
5. **Schema Changes**: Flexible JSON parsing allows new fields (old fields become null)

### Current Limitations

1. **Single Partition**: Kafka topics have 1 partition (limits parallelism)
   - **Fix**: Increase partitions and use key-based partitioning
   
2. **No Schema Registry**: Schema evolution not enforced
   - **Fix**: Integrate Confluent Schema Registry
   
3. **PostgreSQL as Warehouse**: Not optimized for analytics at scale
   - **Fix**: Use columnar warehouse (BigQuery, Snowflake)
   
4. **No Backfill Strategy**: Only processes new events (`startingOffsets: latest`)
   - **Fix**: Add batch job for historical data backfill
   
5. **Single Spark Job per Topic**: One job per table
   - **Fix**: Multi-table job with dynamic topic subscription

## Scaling Considerations

### If Volume Grows 10x

**What Would Break First?**

1. **Kafka Throughput** (Likely First)
   - Single broker, single partition
   - **Solution**: Add Kafka brokers, increase partitions, tune batch sizes

2. **Spark Processing** (Second)
   - Current: 2 cores, 512MB memory per executor
   - **Solution**: Scale Spark cluster, increase parallelism, optimize transformations

3. **PostgreSQL Warehouse Writes** (Third)
   - Sequential upserts may become bottleneck
   - **Solution**: Batch writes, connection pooling, consider columnar warehouse

4. **Debezium Connector** (Least Likely)
   - Handles high throughput well, but monitor replication slot lag

### Scaling Strategies

1. **Horizontal Scaling**:
   - Add Kafka brokers and partitions
   - Scale Spark workers
   - Use distributed warehouse (BigQuery, Snowflake)

2. **Optimization**:
   - Increase Kafka batch sizes
   - Tune Spark micro-batch intervals
   - Use partitioning in warehouse tables

3. **Monitoring**:
   - Track Kafka consumer lag
   - Monitor Spark job metrics
   - Alert on DLQ growth

## 📊 Monitoring & Observability

### Key Metrics to Monitor

1. **Kafka**:
   - Consumer lag per topic
   - Message throughput
   - Topic size

2. **Spark**:
   - Processing time per batch
   - Input rate vs. processing rate
   - Failed batches

3. **PostgreSQL**:
   - Replication slot lag (source DB)
   - Warehouse write latency
   - DLQ event count

4. **Debezium**:
   - Connector status
   - LSN lag

### Logging

- Spark jobs log to console (can be redirected to files)
- Kafka Connect logs: `docker logs etl_connect`
- Check DLQ for data quality issues: `SELECT COUNT(*) FROM dlq_events`

## Testing

### Manual Testing

1. **Insert Test**:
   ```sql
   INSERT INTO customers (email, country) VALUES ('test@example.com', 'US');
   ```
   Verify in `dim_customers` within seconds.

2. **Update Test**:
   ```sql
   UPDATE customers SET country = 'CA' WHERE email = 'test@example.com';
   ```
   Verify `last_seen_at` and `country` updated.

3. **Delete Test**:
   ```sql
   DELETE FROM orders WHERE id = 1;
   ```
   Verify `is_deleted = true` in `fact_orders`.

### Error Testing

1. **Malformed Data**: Manually publish invalid JSON to Kafka topic
2. **Schema Mismatch**: Add unexpected field to source table
3. **Network Failure**: Stop Spark job, verify checkpoint recovery

## Project Structure

```
etl_pipeline/
├── cdc/
│   ├── debezium-connector.json    # Debezium connector config
│   └── register_connector.py      # Connector registration script
├── db/
│   └── init/
│       ├── 001_source_schema.sql   # Source database schema
│       ├── 002_warehouse_schema.sql # Warehouse schema
│       └── 003_add_source_lsn.sql  # LSN indexes
├── producer/
│   ├── config.py                   # DB connection config
│   ├── helper.py                   # Utility functions
│   ├── operations.py               # CRUD operations
│   └── simulate_writes.py          # Data simulation script
├── spark/
│   └── jobs/
│       ├── config.py               # Spark/Kafka config
│       ├── schemas.py              # Debezium JSON schemas
│       ├── transforms.py           # Data transformations
│       ├── sinks.py                # Warehouse write functions
│       ├── stream_customers.py     # Customers streaming job
│       └── stream_orders.py        # Orders streaming job
├── docker-compose.yml              # Infrastructure definition
├── startup.sh                      # Automated setup script
└── run_simulate.sh                 # Run data simulator
```

## Configuration

### Environment Variables

**Spark Jobs**:
- `KAFKA_BOOTSTRAP`: Kafka broker address (default: `kafka:9092`)
- `CUSTOMERS_TOPIC`: Kafka topic for customers (default: `dbserver1.public.customers`)
- `ORDERS_TOPIC`: Kafka topic for orders (default: `dbserver1.public.orders`)
- `CHECKPOINT_DIR`: Spark checkpoint directory
- `PG_URL`, `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`: Warehouse connection

**Producer**:
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`: Source DB connection
- `--sleep-ms`: Delay between operations (default: 500ms)
- `--insert-p`, `--update-p`, `--delete-p`: Operation probabilities

--- 
**Built with**: PostgreSQL, Debezium, Apache Kafka, Apache Spark (PySpark), Docker

