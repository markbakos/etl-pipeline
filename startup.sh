set -e

echo "=========================================="
echo "ETL Pipeline Startup Script"
echo "=========================================="

echo ""
echo "Step 1: Starting Docker services..."
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 15

echo ""
echo "Step 2: Waiting for PostgreSQL..."
until docker exec etl_postgres pg_isready -U postgres -d appdb > /dev/null 2>&1; do
    echo "  Waiting for PostgreSQL..."
    sleep 2
done
echo "✓ PostgreSQL is ready"

echo ""
echo "Step 3: Waiting for Kafka Connect..."
echo "  Waiting for Kafka to be ready..."
for i in {1..30}; do
    if docker exec etl_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo "  Waiting for Kafka Connect REST API..."
for i in {1..60}; do
    if curl -s http://localhost:8083/ > /dev/null 2>&1; then
        echo "✓ Kafka Connect is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "⚠ Kafka Connect did not become ready in time"
        echo "  Check logs: docker logs etl_connect"
        exit 1
    fi
    echo "  Waiting for Kafka Connect... ($i/60)"
    sleep 2
done

echo ""
echo "Step 4: Registering Debezium connector..."
python3 cdc/register_connector.py

echo ""
echo "Step 5: Waiting for connector to start..."
sleep 5

CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/postgres-cdc/status | python3 -c "import sys, json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")

if [ "$CONNECTOR_STATUS" = "RUNNING" ]; then
    echo "✓ Debezium connector is RUNNING"
else
    echo "⚠ Connector status: $CONNECTOR_STATUS"
    echo "  Check logs: docker logs etl_connect"
fi

echo ""
echo "Step 6: Creating Kafka topic (by inserting test data)..."
docker exec -i etl_postgres psql -U postgres -d appdb <<EOF > /dev/null 2>&1 || true
-- Ensure we have a customer
INSERT INTO customers (email, country) 
VALUES ('startup@test.com', 'US') 
ON CONFLICT (email) DO NOTHING;

-- Insert a test order to trigger CDC
INSERT INTO orders (customer_id, status, amount, currency)
SELECT id, 'startup', 1.00, 'USD'
FROM customers 
WHERE email = 'startup@test.com'
LIMIT 1
ON CONFLICT DO NOTHING;
EOF

echo ""
echo "Step 7: Waiting for Kafka topic to be created..."
for i in {1..10}; do
    if docker exec etl_kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q "dbserver1.public.orders"; then
        echo "✓ Kafka topic 'dbserver1.public.orders' exists"
        break
    fi
    echo "  Attempt $i/10: Topic not found yet..."
    sleep 2
done

echo ""
echo "Step 8: Verifying setup..."
TOPIC_EXISTS=$(docker exec etl_kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -c "dbserver1.public.orders" || echo "0")

if [ "$TOPIC_EXISTS" -eq "0" ]; then
    echo "⚠ Topic not found. Check Debezium connector logs:"
    echo "  docker logs etl_connect"
    exit 1
fi

echo "✓ Kafka topics verified"

echo ""
echo "Step 9: Waiting for Spark Master..."
for i in {1..30}; do
    if curl -s http://localhost:8080 > /dev/null 2>&1; then
        echo "✓ Spark Master is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "⚠ Spark Master did not become ready in time"
        echo "  Check logs: docker logs etl_spark_master"
        exit 1
    fi
    echo "  Waiting for Spark Master... ($i/30)"
    sleep 2
done

echo ""
echo "Step 10: Starting Spark streaming jobs..."
echo "  Starting customers job..."
docker exec -d etl_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.1 \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=512m \
  /opt/spark-apps/jobs/stream_customers.py

sleep 3

echo "  Starting orders job..."
docker exec -d etl_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.1 \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=512m \
  /opt/spark-apps/jobs/stream_orders.py

sleep 3

echo "✓ Spark streaming jobs started in background"
echo ""
echo "  Monitor jobs at: http://localhost:8080 (Spark Master UI)"
echo "  Worker UI: http://localhost:8081"

echo ""
echo "=========================================="
echo "Startup complete!"
echo "=========================================="
echo ""
echo "The pipeline is now running. To simulate data writes, run:"
echo "  ./run_simulate.sh"
echo ""
echo "To view logs:"
echo "  docker logs -f etl_spark_master"
echo ""




