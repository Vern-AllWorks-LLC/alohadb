# Copyright (c) 2025, AlohaDB

if not libcurl.found()
  subdir_done()
endif

pg_nl2sql_sources = files(
  'pg_nl2sql.c',
  'schema_context.c',
)

if host_system == 'windows'
  pg_nl2sql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_nl2sql',
    '--FILEDESC', 'pg_nl2sql - natural language to SQL translation via LLM',])
endif

pg_nl2sql = shared_module('pg_nl2sql',
  pg_nl2sql_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [libcurl, contrib_mod_args['dependencies']],
  },
)
contrib_targets += pg_nl2sql

install_data(
  'pg_nl2sql.control',
  'pg_nl2sql--1.0.sql',
  kwargs: contrib_data_args,
)
