
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT positive_amount CHECK (amount > 0),
    CONSTRAINT valid_status CHECK (
        status IN ('completed', 'pending', 'cancelled')
    )
);

INSERT INTO orders (
    order_id,
    customer_id,
    amount,
    status,
    created_at
)
VALUES
    (1001, 101, 250.00, 'completed', NOW() - INTERVAL '5 minutes'),
    (1002, 102, 450.50, 'completed', NOW() - INTERVAL '4 minutes'),
    (1003, 103, 120.00, 'pending', NOW() - INTERVAL '3 minutes'),
    (1004, 101, 300.00, 'cancelled', NOW() - INTERVAL '2 minutes'),
    (1005, 104, 780.00, 'completed', NOW() - INTERVAL '1 minute')
ON CONFLICT (order_id) DO NOTHING;
