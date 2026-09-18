-- lab_build_report_single.sql
-- Used as the INNER task of a ForEach loop.
-- Receives a single dimension value from the ForEach input array via {{input}}.
-- Inserts one dimension's report rows into the shared pipeline_report table.

INSERT INTO IDENTIFIER({{catalog}} || '.gold.pipeline_report')
SELECT
  CASE {{input}}
    WHEN 'product_category' THEN product_category
    WHEN 'product_brand'    THEN product_brand
    WHEN 'customer_segment' THEN customer_segment
  END AS dimension_value,
  {{input}} AS dimension_name,
  COUNT(*) AS feedback_count,
  ROUND(AVG(rating), 2) AS avg_rating,
  ROUND(AVG(order_value), 2) AS avg_order_value,
  current_timestamp() AS report_generated_at
FROM IDENTIFIER({{catalog}} || '.silver.customer_feedback_enriched')
GROUP BY 1, 2
ORDER BY feedback_count DESC;

SELECT {{input}} AS dimension_completed, current_timestamp() AS completed_at;
