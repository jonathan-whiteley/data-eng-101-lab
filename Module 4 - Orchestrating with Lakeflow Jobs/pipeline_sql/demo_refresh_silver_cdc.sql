-- demo_refresh_silver_cdc.sql
-- Refresh the AUTO CDC streaming tables from Demo 3.2
-- SCD Type 1 (current state) and SCD Type 2 (full history)
REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.demo_cdc.demo_cdc_customers_current');
REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.demo_cdc.demo_cdc_customers_history');
-- Verify CDC refresh completed
SELECT 'CDC refresh complete' AS status,
       current_timestamp() AS completed_at;
