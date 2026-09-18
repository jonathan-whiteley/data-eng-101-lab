-- demo_audit_log.sql
-- Logs run metadata using two task parameters:
--   1. :run_id — receives {{job.run_id}} (job metadata)
--   2. :gold_status — receives {{tasks.refresh_gold.output.first_row.status}} (task output passing)
-- Both are configured as task-level parameters in the Jobs UI.

SELECT
  :run_id AS run_id,
  :gold_status AS gold_task_status,
  current_timestamp() AS logged_at;
