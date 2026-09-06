SELECT
    customer_state,

    COUNT(*) AS delivered_orders,

    ROUND(
        AVG(delivery_days),
        2
    ) AS avg_delivery_days,

    SUM(
        CASE
            WHEN is_late = TRUE
            THEN 1
            ELSE 0
        END
    ) AS late_orders,

    SUM(
        CASE
            WHEN is_late = FALSE
            THEN 1
            ELSE 0
        END
    ) AS on_time_orders,

    ROUND(

        SUM(
            CASE
                WHEN is_late = TRUE
                THEN 1
                ELSE 0
            END
        )

        * 100.0 /

        COUNT(*),

        2

    ) AS late_delivery_percentage

FROM delivery_analysis_vw

WHERE customer_state IS NOT NULL

GROUP BY customer_state

ORDER BY late_delivery_percentage DESC;
