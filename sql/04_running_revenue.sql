WITH daily AS (

    SELECT
        TO_DATE(
            order_purchase_timestamp
        ) AS order_date,

        COUNT(*) AS daily_orders,

        ROUND(
            SUM(revenue),
            2
        ) AS daily_revenue

    FROM analytics_orders

    WHERE order_purchase_timestamp IS NOT NULL

    GROUP BY
        TO_DATE(
            order_purchase_timestamp
        )

),

running AS (

    SELECT
        order_date,

        daily_orders,

        daily_revenue,

        SUM(daily_revenue)
        OVER (
            ORDER BY order_date
            ROWS BETWEEN
                UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS cumulative_revenue,

        SUM(daily_revenue)
        OVER () AS total_revenue,

        AVG(daily_revenue)
        OVER (
            ORDER BY order_date
            ROWS BETWEEN
                6 PRECEDING
                AND CURRENT ROW
        ) AS seven_day_avg_revenue

    FROM daily

)

SELECT
    order_date,

    daily_orders,

    daily_revenue,

    ROUND(
        cumulative_revenue,
        2
    ) AS cumulative_revenue,

    ROUND(
        cumulative_revenue * 100.0 /
        total_revenue,
        2
    ) AS cumulative_revenue_percentage,

    ROUND(
        seven_day_avg_revenue,
        2
    ) AS seven_day_avg_revenue

FROM running

ORDER BY order_date;
