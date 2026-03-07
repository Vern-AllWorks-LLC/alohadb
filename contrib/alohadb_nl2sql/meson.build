# Copyright (c) 2025, AlohaDB

if not libcurl.found()
  subdir_done()
endif

alohadb_nl2sql_sources = files(
  'alohadb_nl2sql.c',
  'schema_context.c',
)

if host_system == 'windows'
  alohadb_nl2sql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_nl2sql',
    '--FILEDESC', 'alohadb_nl2sql - natural language to SQL translation via LLM',])
endif

alohadb_nl2sql = shared_module('alohadb_nl2sql',
  alohadb_nl2sql_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [libcurl, contrib_mod_args['dependencies']],
  },
)
contrib_targets += alohadb_nl2sql

install_data(
  'alohadb_nl2sql.control',
  'alohadb_nl2sql--1.0.sql',
  kwargs: contrib_data_args,
)
