# Copyright (c) 2025, AlohaDB

pg_schema_change_sources = files(
  'pg_schema_change.c',
)

if host_system == 'windows'
  pg_schema_change_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_schema_change',
    '--FILEDESC', 'pg_schema_change - online DDL',])
endif

pg_schema_change = shared_module('pg_schema_change',
  pg_schema_change_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_schema_change

install_data(
  'pg_schema_change.control',
  'pg_schema_change--1.0.sql',
  kwargs: contrib_data_args,
)
