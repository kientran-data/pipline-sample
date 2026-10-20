CREATE TABLE IF NOT EXISTS public.orders_processed (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);