-- lab_refresh_gold.sql
-- Refreshes the gold materialized view that aggregates feedback metrics
-- by product category.

REFRESH MATERIALIZED VIEW IDENTIFIER({{catalog}} || '.gold.feedback_by_category');

SELECT 'Gold refresh complete' AS status, current_timestamp() AS completed_at;
