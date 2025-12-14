docker run --rm -it \
  --network etl_pipeline_default \
  -v "$(pwd)/producer:/app/producer" \
  -v "$(pwd)/cdc:/app/cdc" \
  -w /app \
  -e PGHOST=postgres \
  -e PGPORT=5432 \
  -e PGDATABASE=appdb \
  -e PGUSER=postgres \
  -e PGPASSWORD=postgres \
  python:3.11-slim \
  bash -c "pip install -q psycopg2-binary && cd /app && python3 producer/simulate_writes.py $@"





