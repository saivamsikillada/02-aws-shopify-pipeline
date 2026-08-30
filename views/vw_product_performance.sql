-- Product Performance View

CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.total_quantity_sold,
    p.total_revenue,
    i.available_stock,
    i.reserved_stock,
    i.total_stock
FROM dim_product p
INNER JOIN inventory_summary i
ON p.product_id = i.product_id;