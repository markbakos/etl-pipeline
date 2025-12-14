ALTER TABLE fact_orders ADD COLUMN IF NOT EXISTS source_lsn BIGINT;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS source_lsn BIGINT;

CREATE INDEX IF NOT EXISTS idx_fact_orders_source_lsn ON fact_orders(source_lsn);
CREATE INDEX IF NOT EXISTS idx_dim_customers_source_lsn ON dim_customers(source_lsn);

