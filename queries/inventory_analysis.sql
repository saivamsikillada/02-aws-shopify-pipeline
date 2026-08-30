-- Inventory Analysis

SELECT
    product_id,
    product_name,
    available_stock,
    reserved_stock,
    total_stock,
    warehouse_location
FROM inventory_summary
ORDER BY available_stock ASC;