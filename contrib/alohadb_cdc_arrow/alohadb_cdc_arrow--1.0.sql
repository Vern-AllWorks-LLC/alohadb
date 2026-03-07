/* contrib/alohadb_cdc_arrow/alohadb_cdc_arrow--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_cdc_arrow" to load this file. \quit

-- This extension is a logical decoding output plugin implemented as a
-- shared library.  It does not define any SQL-level objects.  To use it,
-- create a logical replication slot:
--
--   SELECT * FROM pg_create_logical_replication_slot('my_slot', 'alohadb_cdc_arrow');
