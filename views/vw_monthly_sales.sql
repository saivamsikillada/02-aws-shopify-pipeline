-- Monthly Sales View

CREATE OR REPLACE VIEW vw_monthly_sales AS
SELECT
    year(order_date) AS sales_year,
    month(order_date) AS sales_month,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_sales,
    AVG(order_amount) AS average_order_value
FROM fact_orders
GROUP BY
    year(order_date),
    month(order_date)
ORDER BY
    sales_year,
    sales_month;