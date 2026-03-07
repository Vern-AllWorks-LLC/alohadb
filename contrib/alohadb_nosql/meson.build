# Copyright (c) 2025, AlohaDB

alohadb_nosql_sources = files(
  'alohadb_nosql.c',
  'nosql_crud.c',
  'nosql_query.c',
  'nosql_collection.c',
)

if host_system == 'windows'
  alohadb_nosql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_nosql',
    '--FILEDESC', 'alohadb_nosql - document database API with flexible schemas',])
endif

alohadb_nosql = shared_module('alohadb_nosql',
  alohadb_nosql_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_nosql

install_data(
  'alohadb_nosql.control',
  'alohadb_nosql--1.0.sql',
  kwargs: contrib_data_args,
)
