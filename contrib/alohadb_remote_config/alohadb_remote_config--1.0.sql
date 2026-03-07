/* contrib/alohadb_remote_config/alohadb_remote_config--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_remote_config" to load this extension. \quit

-- ============================================================
-- Management Schema Tables
-- ============================================================

-- Management page users (login credentials for the web UI)
CREATE TABLE IF NOT EXISTS mgmt_users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    full_name TEXT,
    email TEXT,
    role TEXT DEFAULT 'viewer',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Registered AlohaDB instances
CREATE TABLE IF NOT EXISTS mgmt_servers (
    id SERIAL PRIMARY KEY,
    hostname TEXT UNIQUE NOT NULL,
    ip_address TEXT NOT NULL,
    port INT DEFAULT 5433,
    instance_token TEXT NOT NULL,
    region TEXT,
    state TEXT,
    city TEXT,
    datacenter TEXT,
    app TEXT,
    pnl TEXT,
    environment TEXT DEFAULT 'dev',
    db_support_team TEXT,
    status TEXT DEFAULT 'pending online',
    last_status_date TIMESTAMPTZ DEFAULT now(),
    online_date TIMESTAMPTZ,
    decom_date TIMESTAMPTZ,
    comments TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Default configuration settings pushed to all instances
CREATE TABLE IF NOT EXISTS mgmt_default_config (
    id SERIAL PRIMARY KEY,
    param_name TEXT UNIQUE NOT NULL,
    param_value TEXT NOT NULL,
    description TEXT,
    can_override BOOLEAN DEFAULT true,
    config_file TEXT DEFAULT 'alohadb.conf',
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Per-server config overrides
CREATE TABLE IF NOT EXISTS mgmt_server_config (
    id SERIAL PRIMARY KEY,
    server_id INT REFERENCES mgmt_servers(id) ON DELETE CASCADE,
    param_name TEXT NOT NULL,
    param_value TEXT NOT NULL,
    is_override BOOLEAN DEFAULT true,
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(server_id, param_name)
);

-- Database users managed per server
CREATE TABLE IF NOT EXISTS mgmt_server_users (
    id SERIAL PRIMARY KEY,
    server_id INT REFERENCES mgmt_servers(id) ON DELETE CASCADE,
    db_username TEXT NOT NULL,
    db_role TEXT DEFAULT 'readonly',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(server_id, db_username)
);

-- Action logs (audit trail for management operations)
CREATE TABLE IF NOT EXISTS mgmt_action_logs (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT,
    detail TEXT,
    remote_ip TEXT NOT NULL,
    service_ticket TEXT,
    auditor_comment TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- Indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_action_logs_created ON mgmt_action_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_action_logs_username ON mgmt_action_logs(username);
CREATE INDEX IF NOT EXISTS idx_action_logs_action ON mgmt_action_logs(action);
CREATE INDEX IF NOT EXISTS idx_servers_status ON mgmt_servers(status);
CREATE INDEX IF NOT EXISTS idx_servers_env ON mgmt_servers(environment);

-- ============================================================
-- Functions
-- ============================================================

-- remote_config_status(): returns current remote config state
CREATE OR REPLACE FUNCTION remote_config_status(
    OUT remote_mode boolean,
    OUT remote_host text,
    OUT remote_port int,
    OUT last_fetch timestamptz,
    OUT last_status text
)
RETURNS record
AS 'MODULE_PATHNAME', 'remote_config_status'
LANGUAGE C STABLE;
