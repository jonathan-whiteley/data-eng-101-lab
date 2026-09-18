-- lab_build_report_solved.sql
-- Solution version — used by the programmatic job creation in Section G.

INSERT INTO IDENTIFIER({{catalog}} || '.gold.pipeline_report')
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
FROM IDENTIFIER({{catalog}} || '.silver.customer_feedback_enriched')
GROUP BY 1, 2
ORDER BY feedback_count DESC;

SELECT * FROM IDENTIFIER({{catalog}} || '.gold.pipeline_report')
WHERE report_generated_at = (
  SELECT MAX(report_generated_at)
  FROM IDENTIFIER({{catalog}} || '.gold.pipeline_report')
);
