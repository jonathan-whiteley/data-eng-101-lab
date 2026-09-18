-- lab_build_report.sql
-- Appends a parameterized report row set to gold.pipeline_report.
-- The {{report_dimension}} job parameter controls which column the report groups by.
--
-- The CASE expression maps the parameter value to the actual column:
--   'product_category' -> product_category
--   'product_brand'    -> product_brand
--   'customer_segment' -> customer_segment
--
-- Because the job engine wraps {{report_dimension}} in single quotes,
-- we use CASE to convert the string into a column reference.
-- Each run appends new rows — the table accumulates report history.

-- TODO: Replace each FILL_IN with the correct IDENTIFIER reference
-- Pattern: IDENTIFIER({{catalog}} || '.schema.table_name')

INSERT INTO FILL_IN
SELECT
  CASE {{report_dimension}}
    WHEN 'product_category' THEN product_category
    WHEN 'product_brand'    THEN product_brand
    WHEN 'customer_segment' THEN customer_segment
  END AS dimension_value,
  {{report_dimension}} AS dimension_name,
  COUNT(*) AS feedback_count,
  ROUND(AVG(rating), 2) AS avg_rating,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  current_timestamp() AS report_generated_at
FROM FILL_IN
GROUP BY 1, 2
ORDER BY feedback_count DESC;

SELECT * FROM IDENTIFIER({{catalog}} || '.gold.pipeline_report')
WHERE report_generated_at = (
  SELECT MAX(report_generated_at)
  FROM IDENTIFIER({{catalog}} || '.gold.pipeline_report')
);
