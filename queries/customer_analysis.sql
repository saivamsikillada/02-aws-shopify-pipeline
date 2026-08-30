-- Customer Analysis

SELECT
    customer_id,
    customer_name,
    customer_email,
    city,
    state,
    total_orders,
    total_spent
FROM dim_customer
ORDER BY total_spent DESC;