-- lab_refresh_cdc.sql
-- Refreshes the CDC streaming table that applies customer changes
-- into a current-state dimension table (SCD Type 1).

REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.demo_cdc.demo_cdc_customers_current');

SELECT 'CDC refresh complete' AS status, current_timestamp() AS completed_at;
