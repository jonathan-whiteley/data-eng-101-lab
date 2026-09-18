-- demo_refresh_silver.sql
-- Refresh the silver-layer materialized view
-- orders_enriched joins orders, customers, and products (built in Demo 2.2)

REFRESH MATERIALIZED VIEW IDENTIFIER({{catalog}} || '.silver.orders_enriched');

-- Verify silver refresh completed
SELECT 'Silver refresh complete' AS status,
       current_timestamp() AS completed_at;
