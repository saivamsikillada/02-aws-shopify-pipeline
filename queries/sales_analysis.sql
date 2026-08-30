-- Sales Analysis

SELECT
    order_date,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_sales,
    AVG(order_amount) AS average_order_value
FROM fact_orders
GROUP BY order_date
ORDER BY order_date;