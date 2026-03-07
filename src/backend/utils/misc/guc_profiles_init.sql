/*
 * guc_profiles_init.sql
 *		Register SQL-callable functions for the auto-tuning profile system.
 *
 * Run this once after initdb, or add to template1, to make the functions
 * available:
 *
 *   psql -f src/backend/utils/misc/guc_profiles_init.sql template1
 *
 * Alternatively, add the entries below to pg_proc.dat before initdb
 * (see comments at the bottom of this file).
 *
 * Copyright (c) 2025, AlohaDB Project
 */

-- Apply a named configuration profile (oltp, analytics, ai_ml, hybrid).
-- Requires superuser privileges.
CREATE OR REPLACE FUNCTION alohadb_apply_profile(text)
RETURNS void
AS 'alohadb_apply_profile'
LANGUAGE internal
VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_apply_profile(text)
IS 'Apply an auto-tuning configuration profile (oltp, analytics, ai_ml, hybrid)';

-- Detect hardware and return component/value pairs.
CREATE OR REPLACE FUNCTION alohadb_detect_hardware(
    OUT component text,
    OUT value text
)
RETURNS SETOF record
AS 'alohadb_detect_hardware'
LANGUAGE internal
VOLATILE;

COMMENT ON FUNCTION alohadb_detect_hardware()
IS 'Detect hardware characteristics for auto-tuning profiles';

/*
 * If you prefer to register these as built-in functions via pg_proc.dat
 * (so they are available immediately after initdb), add the following
 * entries to src/include/catalog/pg_proc.dat before the closing ']':
 *
 * { oid => '9510', descr => 'apply an auto-tuning configuration profile',
 *   proname => 'alohadb_apply_profile', prorettype => 'void',
 *   proargtypes => 'text', provolatile => 'v',
 *   prosrc => 'alohadb_apply_profile' },
 * { oid => '9511', descr => 'detect hardware for auto-tuning profiles',
 *   proname => 'alohadb_detect_hardware', prorows => '10', proretset => 't',
 *   prorettype => 'record', proargtypes => '',
 *   proallargtypes => '{text,text}', proargmodes => '{o,o}',
 *   proargnames => '{component,value}', provolatile => 'v',
 *   prosrc => 'alohadb_detect_hardware' },
 */
