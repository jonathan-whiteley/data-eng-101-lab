-- Databricks notebook source
BEGIN

  -- ---------------------------------------------------------------
  -- GOLD LAYER
  -- ---------------------------------------------------------------
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.gold.revenue_by_category');
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.gold.daily_sales_summary');
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.gold.feedback_by_category');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.gold.pipeline_report');

  -- ---------------------------------------------------------------
  -- SILVER LAYER
  -- ---------------------------------------------------------------
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.silver.orders_enriched');
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.silver.customer_feedback_enriched');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.silver.demo_customers_type1');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.silver.demo_customers_type2');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.silver.lab_customers_type1');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.silver.lab_customers_type2');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.silver.lab_customers_type1_deletes');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.silver.lab_customers_type2_tracked');

  -- ---------------------------------------------------------------
  -- BRONZE LAYER
  -- ---------------------------------------------------------------
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.bronze.raw_orders');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.bronze.raw_customers');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.bronze.raw_products');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.bronze.customer_feedback');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.bronze.customer_changes');

  -- ---------------------------------------------------------------
  -- DEMO_CDC SCHEMA
  -- ---------------------------------------------------------------
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.demo_cdc.demo_cdc_customers_current');
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.demo_cdc.demo_cdc_customers_history');

  -- ---------------------------------------------------------------
  -- DEFAULT SCHEMA (objects created via SQL Editor copy-paste)
  -- ---------------------------------------------------------------
  DROP TABLE            IF EXISTS IDENTIFIER(my_catalog || '.default.demo_1_orders');
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.default.demo_1_revenue_summary');
  DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER(my_catalog || '.default.lab_top_orders');

END;
