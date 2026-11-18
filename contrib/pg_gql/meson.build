# Copyright (c) 2025, AlohaDB

pg_gql_sources = files(
  'pg_gql.c',
  'graphql_parser.c',
  'graphql_resolver.c',
)

if host_system == 'windows'
  pg_gql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_gql',
    '--FILEDESC', 'pg_gql - auto-generated GraphQL API',])
endif

pg_gql = shared_module('pg_gql',
  pg_gql_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_gql

install_data(
  'pg_gql.control',
  'pg_gql--1.0.sql',
  kwargs: contrib_data_args,
)
