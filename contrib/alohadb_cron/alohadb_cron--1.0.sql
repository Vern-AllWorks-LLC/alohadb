/* contrib/alohadb_cron/alohadb_cron--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_cron" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_cron_jobs
--
-- Each row defines a scheduled job: a SQL command to execute
-- according to a cron schedule expression.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_cron_jobs (
    jobid       serial PRIMARY KEY,
    schedule    text NOT NULL,
    command     text NOT NULL,
    database    text DEFAULT current_database(),
    username    text DEFAULT current_user,
    active      boolean DEFAULT true,
    jobname     text UNIQUE,
    created_at  timestamptz DEFAULT now()
);

COMMENT ON TABLE alohadb_cron_jobs IS
'Scheduled jobs with cron syntax for AlohaDB';

COMMENT ON COLUMN alohadb_cron_jobs.schedule IS
'Cron expression: minute hour day-of-month month day-of-week';

COMMENT ON COLUMN alohadb_cron_jobs.command IS
'SQL command to execute when the schedule matches';

COMMENT ON COLUMN alohadb_cron_jobs.database IS
'Database in which to execute the command';

COMMENT ON COLUMN alohadb_cron_jobs.username IS
'User under which the command runs';

COMMENT ON COLUMN alohadb_cron_jobs.active IS
'Set to false to temporarily disable this job';

COMMENT ON COLUMN alohadb_cron_jobs.jobname IS
'Optional unique name for the job (used by cron_schedule_named / cron_unschedule_named)';

-- ----------------------------------------------------------------
-- alohadb_cron_job_run_details
--
-- Audit log of each job execution.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_cron_job_run_details (
    runid           bigserial PRIMARY KEY,
    jobid           int REFERENCES alohadb_cron_jobs(jobid) ON DELETE CASCADE,
    status          text,
    return_message  text,
    start_time      timestamptz,
    end_time        timestamptz
);

CREATE INDEX ON alohadb_cron_job_run_details (jobid, start_time DESC);

COMMENT ON TABLE alohadb_cron_job_run_details IS
'Execution history for cron jobs';

-- ----------------------------------------------------------------
-- cron_schedule(schedule, command) -> jobid
--
-- Schedule a new cron job.  Returns the assigned job ID.
-- ----------------------------------------------------------------
CREATE FUNCTION cron_schedule(schedule text, command text)
RETURNS int
AS 'MODULE_PATHNAME', 'cron_schedule'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cron_schedule(text, text) IS
'Schedule a new cron job with the given cron expression and SQL command';

-- ----------------------------------------------------------------
-- cron_schedule_named(job_name, schedule, command) -> jobid
--
-- Schedule a named cron job.  If a job with the same name exists,
-- its schedule and command are updated.  Returns the job ID.
-- ----------------------------------------------------------------
CREATE FUNCTION cron_schedule_named(job_name text, schedule text, command text)
RETURNS int
AS 'MODULE_PATHNAME', 'cron_schedule_named'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cron_schedule_named(text, text, text) IS
'Schedule a named cron job (upsert by name)';

-- ----------------------------------------------------------------
-- cron_unschedule(job_id) -> boolean
--
-- Remove a cron job by its ID.  Returns true if deleted.
-- ----------------------------------------------------------------
CREATE FUNCTION cron_unschedule(job_id int)
RETURNS boolean
AS 'MODULE_PATHNAME', 'cron_unschedule'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cron_unschedule(int) IS
'Remove a scheduled cron job by ID';

-- ----------------------------------------------------------------
-- cron_unschedule_named(job_name) -> boolean
--
-- Remove a cron job by its name.  Returns true if deleted.
-- ----------------------------------------------------------------
CREATE FUNCTION cron_unschedule_named(job_name text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'cron_unschedule_named'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cron_unschedule_named(text) IS
'Remove a scheduled cron job by name';

-- ----------------------------------------------------------------
-- cron_job_status() -> TABLE
--
-- Return a summary of all cron jobs including last run details
-- and the computed next run time.
-- ----------------------------------------------------------------
CREATE FUNCTION cron_job_status(
    OUT jobid       int,
    OUT jobname     text,
    OUT schedule    text,
    OUT command     text,
    OUT active      boolean,
    OUT last_run    timestamptz,
    OUT last_status text,
    OUT next_run    timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'cron_job_status'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION cron_job_status() IS
'Show all cron jobs with last execution status and next scheduled run time';
