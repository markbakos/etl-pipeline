CREATE TABLE IF NOT EXISTS dim_customers (
  customer_id BIGINT PRIMARY KEY,
  email TEXT,
  country TEXT,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fact_orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  status TEXT,
  amount_eur NUMERIC(12,2),
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  is_deleted BOOLEAN NOT NULL DEFAULT false,
  event_time TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dlq_events (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT,
  partition INT,
  kafka_offset BIGINT,
  error TEXT,
  raw_payload TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
