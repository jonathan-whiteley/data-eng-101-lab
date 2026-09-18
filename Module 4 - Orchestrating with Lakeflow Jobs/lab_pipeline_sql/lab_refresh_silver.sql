-- lab_refresh_silver.sql
-- Refreshes the silver materialized view that enriches customer feedback
-- with order, product, and customer details.

REFRESH MATERIALIZED VIEW IDENTIFIER({{catalog}} || '.silver.customer_feedback_enriched');

SELECT 'Silver refresh complete' AS status, current_timestamp() AS completed_at;
