WITH status_summary AS (

    SELECT
        order_status,
        COUNT(*) AS total_orders
    FROM analytics_orders
    GROUP BY order_status

)

SELECT
    order_status,
    total_orders,

    ROUND(
        total_orders * 100.0 /
        SUM(total_orders) OVER (),
        2
    ) AS order_percentage

FROM status_summary

ORDER BY total_orders DESC;
