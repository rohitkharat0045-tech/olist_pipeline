WITH state_summary AS (

    SELECT
        customer_state,

        COUNT(*) AS total_orders,

        COUNT(
            DISTINCT customer_id
        ) AS total_customers,

        ROUND(
            SUM(revenue),
            2
        ) AS total_revenue,

        ROUND(
            AVG(revenue),
            2
        ) AS avg_order_value,

        ROUND(
            AVG(delivery_days),
            2
        ) AS avg_delivery_days

    FROM analytics_orders

    WHERE customer_state IS NOT NULL

    GROUP BY customer_state

),

ranked_states AS (

    SELECT
        *,

        DENSE_RANK()
        OVER (
            ORDER BY total_revenue DESC
        ) AS revenue_rank,

        NTILE(4)
        OVER (
            ORDER BY total_revenue DESC
        ) AS revenue_quartile,

        ROUND(
            total_revenue * 100.0 /
            SUM(total_revenue) OVER (),
            2
        ) AS revenue_share_percentage

    FROM state_summary

)

SELECT
    customer_state,

    total_orders,

    total_customers,

    total_revenue,

    avg_order_value,

    avg_delivery_days,

    revenue_rank,

    revenue_quartile,

    CASE

        WHEN revenue_quartile = 1
            THEN 'Top Performing'

        WHEN revenue_quartile = 2
            THEN 'High Performing'

        WHEN revenue_quartile = 3
            THEN 'Medium Performing'

        ELSE 'Low Performing'

    END AS performance_tier,

    revenue_share_percentage

FROM ranked_states

ORDER BY revenue_rank;
