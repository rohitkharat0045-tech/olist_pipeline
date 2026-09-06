CREATE OR REPLACE TEMP VIEW delivery_analysis_vw AS

SELECT
    order_id,
    customer_state,
    order_status,
    delivery_days,
    is_late
FROM analytics_orders
WHERE delivery_days IS NOT NULL;
