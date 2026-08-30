-- Product Performance Analysis

SELECT
    product_id,
    product_name,
    category,
    price,
    total_quantity_sold,
    total_revenue
FROM dim_product
ORDER BY total_revenue DESC;