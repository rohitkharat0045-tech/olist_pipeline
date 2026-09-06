WITH monthly AS (

    SELECT
        DATE_TRUNC(
            'month',
            order_purchase_timestamp
        ) AS order_month,

        COUNT(*) AS total_orders,

        ROUND(
            SUM(revenue),
            2
        ) AS monthly_revenue

    FROM analytics_orders

    WHERE order_purchase_timestamp IS NOT NULL

    GROUP BY
        DATE_TRUNC(
            'month',
            order_purchase_timestamp
        )

),

revenue_change AS (

    SELECT
        order_month,
        total_orders,
        monthly_revenue,

        LAG(monthly_revenue)
        OVER (
            ORDER BY order_month
        ) AS previous_month_revenue

    FROM monthly

)

SELECT
    YEAR(order_month) AS year,

    MONTH(order_month) AS month_number,

    DATE_FORMAT(
        order_month,
        'MMMM'
    ) AS month_name,

    total_orders,

    monthly_revenue,

    previous_month_revenue,

    ROUND(
        monthly_revenue -
        previous_month_revenue,
        2
    ) AS revenue_change,

    ROUND(
        CASE

            WHEN previous_month_revenue IS NULL
                 OR previous_month_revenue = 0

            THEN NULL

            ELSE
                (
                    monthly_revenue -
                    previous_month_revenue
                )
                * 100.0 /
                previous_month_revenue

        END,
        2
    ) AS growth_percentage

FROM revenue_change

ORDER BY order_month;
