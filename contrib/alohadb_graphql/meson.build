# Copyright (c) 2025, AlohaDB

alohadb_graphql_sources = files(
  'alohadb_graphql.c',
  'graphql_parser.c',
  'graphql_resolver.c',
)

if host_system == 'windows'
  alohadb_graphql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_graphql',
    '--FILEDESC', 'alohadb_graphql - auto-generated GraphQL API',])
endif

alohadb_graphql = shared_module('alohadb_graphql',
  alohadb_graphql_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_graphql

install_data(
  'alohadb_graphql.control',
  'alohadb_graphql--1.0.sql',
  kwargs: contrib_data_args,
)
