-- Databricks notebook source
-- MAGIC %run ./_config-course-catalog

-- COMMAND ----------

-- Create safe function for catalog name
CREATE OR REPLACE TEMPORARY FUNCTION safe_uc_name(value STRING)
RETURNS STRING
RETURN
  COALESCE(
    NULLIF(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(TRIM(value)),
          '[^a-z0-9_]',
          '_'
        ),
        '_+',
        '_'
      ),
      ''
    ),
    'user'
  );


-- Create temp view of all available catalogs
CREATE OR REPLACE TEMPORARY VIEW list_of_catalogs AS
SELECT LOWER(catalog_name) AS catalog_names
FROM system.information_schema.catalogs;

-- COMMAND ----------

-- Start SQL scripting block to create and or set catalog based on Workspace
BEGIN

-- =========================================
-- 1. Declare variables
-- =========================================

  -- catalog_forced is driven by the single knob in ./_config-course-catalog (session var course_catalog).
  -- Do NOT edit here; change the catalog in _config-course-catalog instead.
  DECLARE catalog_forced STRING DEFAULT 'NONE';


  DECLARE user_email STRING;
  DECLARE user_name STRING;
  -- Make the user name safe as a catalog name
  DECLARE safe_user_name STRING;
  -- Set's the vocareum catalog name 'labuser1234123423'
  DECLARE vocareum_catalog_name STRING DEFAULT 'NA';
  -- Catalog name to use outside of Vocareum
  DECLARE catalog_name STRING DEFAULT 'NA';
  -- Flag to determine if the catalog already exists
  DECLARE catalog_already_exists BOOLEAN DEFAULT FALSE;
  -- Create catalog statement to run
  DECLARE create_catalog_statement STRING DEFAULT '';
  -- View to create the catalog name as view to use outside of this script
  DECLARE create_view_with_catalog_name STRING DEFAULT '';


-- =========================================
-- 2. Capture current user
-- =========================================
  SET user_email = current_user();
  SET user_name = SPLIT(user_email, '@')[0];
  SET safe_user_name = (SELECT safe_uc_name(SPLIT(current_user(), '@')[0]));

  -- Pull the configured catalog from the single knob (./_config-course-catalog)
  SET catalog_forced = COALESCE(course_catalog, 'NONE');

-- ==================================================================================
-- 3. Determine Vocareum or Non Vocareum Workspace and set and/or create catalog (Non Vocareum)
-- ==================================================================================
  -- Check to see if the user is in Vocareum by email address then setting the catalog name to the labuser name.
  IF (LOWER(user_email) LIKE '%@vocareum.com')
    THEN 
      SET vocareum_catalog_name = safe_user_name;
      
      -- Check existence using the temp view of catalogs
      IF EXISTS (
        SELECT 1
        FROM list_of_catalogs
        WHERE catalog_names = LOWER(vocareum_catalog_name)
      ) THEN  

        -- Document the catalog already exists
        SET catalog_already_exists = TRUE;

        -- Create Temp View to Store the SQL Variable Value for Global Use
        SET create_view_with_catalog_name = CONCAT(
              "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
              "SELECT '", vocareum_catalog_name, "' AS catalog_name"
            );


      END IF; 

  -- If Vocareum check fails, setup assuming you are in another Workspace like Free Edition. Create a catalog using labuser_username
  ELSE

    -- Checks to see if a catalog is forced to be used. Will use that but the catalog has to exist already. 
    IF catalog_forced != 'NONE' THEN

      -- Use the forced catalog by the user
      SET catalog_name = catalog_forced;

      -- Tests to confirm the catalog exists. Error is returned if it doesn't.
      USE CATALOG IDENTIFIER(catalog_name);

      -- Document the catalog already exists
      SET catalog_already_exists = TRUE;

      -- Create Temp View to Store the SQL Variable Value for Global Use
      SET create_view_with_catalog_name = CONCAT(
        "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
        "SELECT '", catalog_name, "' AS catalog_name"
      );

    -- If catalog is not forced, create default catalog for learner
    ELSE

      -- Limit the user's name to 19 characters. THis is done because there is a limit to the catalog.schema.object name (64 characters). For someone with a long name this could cause issus. Using 19 because that is the general size of the vocareum user name
      SET catalog_name = CONCAT('labuser_', LEFT(safe_user_name,19));

      -- Create Temp View to Store the SQL Variable Value for Global Use
      SET create_view_with_catalog_name = CONCAT(
        "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
        "SELECT '", catalog_name, "' AS catalog_name"
      );

      -- Check existence using the temp view of catalogs
      IF EXISTS (
        SELECT 1
        FROM list_of_catalogs
        WHERE catalog_names = LOWER(catalog_name)
      ) 
        THEN 
        
          -- Document if the catalog already exists
          SET catalog_already_exists = TRUE;
      
      ELSE 
        -- Otherwise, create the catalog for the user
        SET create_catalog_statement = CONCAT(
          'CREATE CATALOG IF NOT EXISTS ',
          catalog_name
        );

        -- Create the catalog
        EXECUTE IMMEDIATE create_catalog_statement;
        
        -- Document if the catalog didn't exist
        SET catalog_already_exists = FALSE;

      END IF;

    END IF;

  END IF;

  -- Create the temp view with the catalog name
  EXECUTE IMMEDIATE create_view_with_catalog_name;


-- ===================================================================
-- 4. Return Final Script Results (Workspace Assumed and Catalog Name
-- ===================================================================
  SELECT
    CASE
      WHEN vocareum_catalog_name <> 'NA' THEN 'Vocareum Workspace'
      ELSE 'Non Vocareum Workspace'
    END AS Workspace,
    CASE
      WHEN vocareum_catalog_name <> 'NA' THEN vocareum_catalog_name
      ELSE catalog_name
    END AS `User Catalog Name`,
    CASE
      WHEN catalog_already_exists = TRUE THEN 'Catalog Already Exists'
      ELSE 'Catalog Created'
    END AS `Catalog Status`;
END;

-- COMMAND ----------

-- Create the global SQL variable with the catalog name using the temp view
DECLARE OR REPLACE my_catalog STRING;
SET VAR my_catalog = (SELECT catalog_name FROM catalog_name_vw);


-- Set default catalog to user's labuser catalog
USE CATALOG IDENTIFIER(my_catalog);


-- COMMAND ----------

DROP VIEW IF EXISTS catalog_name_vw;
DROP VIEW IF EXISTS list_of_catalogs;

-- COMMAND ----------

-- MAGIC %run ./Course-Cleanup

-- COMMAND ----------

-- Create medallion schemas
BEGIN
  CREATE SCHEMA IF NOT EXISTS IDENTIFIER(my_catalog || '.bronze');
  CREATE SCHEMA IF NOT EXISTS IDENTIFIER(my_catalog || '.silver');
  CREATE SCHEMA IF NOT EXISTS IDENTIFIER(my_catalog || '.gold');
  CREATE SCHEMA IF NOT EXISTS IDENTIFIER(my_catalog || '.demo_cdc');
END;

-- COMMAND ----------

-- Verify course data exists
BEGIN
  DECLARE data_check INT DEFAULT 0;
  SET data_check = (
    SELECT COUNT(*)
    FROM system.information_schema.tables
    WHERE table_catalog = my_catalog
      AND table_schema = 'data'
      AND table_name = 'raw_customers'
  );
  IF data_check = 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'COURSE DATA NOT FOUND -- The "data" schema is empty or missing. Please run "1 - REQUIRED - Course Data Setup" before running any demos or labs.';
  END IF;
END;
