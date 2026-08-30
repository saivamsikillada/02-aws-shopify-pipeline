-- Customer Sales View

CREATE OR REPLACE VIEW vw_customer_sales AS
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    c.total_orders,
    c.total_spent,
    o.order_id,
    o.order_date,
    o.order_amount
FROM dim_customer c
INNER JOIN fact_orders o
ON c.customer_id = o.customer_id;