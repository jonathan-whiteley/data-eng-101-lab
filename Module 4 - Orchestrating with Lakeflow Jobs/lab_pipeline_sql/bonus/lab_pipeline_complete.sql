-- lab_pipeline_complete.sql
-- Final cleanup/notification task — runs regardless of upstream success or failure.
-- Uses Run-If: ALL_DONE so it always executes.

SELECT
  'Pipeline run complete' AS status,
  current_timestamp() AS completed_at;
