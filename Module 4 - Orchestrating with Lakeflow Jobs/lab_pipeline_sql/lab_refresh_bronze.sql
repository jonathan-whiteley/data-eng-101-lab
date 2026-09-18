-- lab_refresh_bronze.sql
-- Refreshes the three bronze streaming tables.
-- Uses the {{catalog}} job parameter with IDENTIFIER() for catalog portability.

-- TODO: Replace each FILL_IN with the correct IDENTIFIER reference
-- Pattern: IDENTIFIER({{catalog}} || '.schema.table_name')

REFRESH STREAMING TABLE FILL_IN;
REFRESH STREAMING TABLE FILL_IN;
REFRESH STREAMING TABLE FILL_IN;

SELECT 'Bronze refresh complete' AS status, current_timestamp() AS completed_at;
