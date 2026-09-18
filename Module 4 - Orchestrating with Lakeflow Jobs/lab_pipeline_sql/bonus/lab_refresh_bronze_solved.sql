-- lab_refresh_bronze_solved.sql
-- Solution version — used by the programmatic job creation in Section G.

REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.raw_orders');
REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.raw_products');
REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.customer_feedback');

SELECT 'Bronze refresh complete' AS status, current_timestamp() AS completed_at;
