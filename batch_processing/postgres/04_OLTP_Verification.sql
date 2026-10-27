-- 1. Verify foreign-key relationships
-- Query 1.1 — Check for orders without valid customers
SELECT COUNT(*) AS orders_without_customers
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Query 1.2 — Check for order items without valid orders or products
SELECT
    COUNT(*) FILTER (
        WHERE o.order_id IS NULL
    ) AS items_without_orders,
    COUNT(*) FILTER (
        WHERE p.product_id IS NULL
    ) AS items_without_products
FROM order_items oi
LEFT JOIN orders o
    ON oi.order_id = o.order_id
LEFT JOIN products p
    ON oi.product_id = p.product_id;

-- Query 1.3 — Check for products without inventory records
SELECT COUNT(*) AS products_without_inventory
FROM products p
LEFT JOIN inventory i
    ON p.product_id = i.product_id
WHERE i.product_id IS NULL;


-- Query 1.4 — Check for inventory records without products
SELECT COUNT(*) AS inventory_without_products
FROM inventory i
LEFT JOIN products p
    ON i.product_id = p.product_id
WHERE p.product_id IS NULL;

-- 2. Verify the order and order-item business logic
-- Query 2.1 — Check for invalid quantities or prices

SELECT
    COUNT(*) FILTER (
        WHERE quantity <= 0
    ) AS invalid_quantities,
    COUNT(*) FILTER (
        WHERE unit_price < 0
    ) AS negative_unit_prices,
    COUNT(*) FILTER (
        WHERE line_total < 0
    ) AS negative_line_totals
FROM order_items;

-- Query 2.2 — Verify that line totals are correct
select count(*) as incorrect_line_totals
from order_items oi 
where ABS(oi.line_total -(oi.quantity * oi.unit_price )) > 0;

-- Query 2.3 — Check whether order totals match their items
WITH item_totals AS (
    SELECT
        order_id,
        SUM(line_total) AS calculated_total
    FROM order_items
    GROUP BY order_id
)
SELECT COUNT(*) AS orders_with_incorrect_totals
FROM orders o
JOIN item_totals it
    ON o.order_id = it.order_id
WHERE ABS(o.order_total - it.calculated_total) > 0.01;  

-- Query 2.4 — Find orders without any items
SELECT COUNT(*) AS orders_without_items
FROM orders o
LEFT JOIN order_items oi
    ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL;

-- 3. Verify customer and product data quality
-- Query 3.1 — Check customer emails
SELECT
    COUNT(*) FILTER (
        WHERE email IS NULL OR TRIM(email) = ''
    ) AS missing_emails,
    COUNT(*) - COUNT(DISTINCT email) AS duplicate_email_count
FROM customers;

-- Query 3.2 — Check product prices
SELECT
    COUNT(*) FILTER (
        WHERE price IS NULL OR price <= 0
    ) AS invalid_prices,
    COUNT(*) FILTER (
        WHERE cost IS NULL OR cost <= 0
    ) AS invalid_costs
FROM products;

select * from products p 
where p.price <= 0;

-- Query 3.3 — Check inventory values
SELECT
    COUNT(*) FILTER (
        WHERE stock_quantity < 0
    ) AS negative_stock,
    COUNT(*) FILTER (
        WHERE reorder_level < 0
    ) AS negative_reorder_levels
FROM inventory;


-- 4. Verify the database constraints
SELECT
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
    ON tc.constraint_catalog = kcu.constraint_catalog
    AND tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_name = kcu.constraint_name
LEFT JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_catalog = ccu.constraint_catalog
    AND tc.constraint_schema = ccu.constraint_schema
    AND tc.constraint_name = ccu.constraint_name
WHERE tc.table_schema = 'public'
    AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
ORDER BY tc.table_name, tc.constraint_type;