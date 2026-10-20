
CREATE OR REPLACE VIEW public.orders_processed_sink AS
SELECT
    order_id,
    customer_id,
    amount,
    status,
    created_at_epoch_ms
FROM public.orders_processed;

CREATE OR REPLACE FUNCTION public.upsert_processed_order()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.orders_processed (
        order_id, customer_id, amount,
        status, created_at_epoch_ms
    )
    VALUES (
        NEW.order_id, NEW.customer_id, NEW.amount,
        NEW.status, NEW.created_at_epoch_ms
    )
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        amount = EXCLUDED.amount,
        status = EXCLUDED.status,
        created_at_epoch_ms = EXCLUDED.created_at_epoch_ms;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS orders_processed_sink_insert
ON public.orders_processed_sink;

CREATE TRIGGER orders_processed_sink_insert
INSTEAD OF INSERT ON public.orders_processed_sink
FOR EACH ROW
EXECUTE FUNCTION public.upsert_processed_order();
