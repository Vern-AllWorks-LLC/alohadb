# Copyright (c) 2025, AlohaDB

alohadb_schema_change_sources = files(
  'alohadb_schema_change.c',
)

if host_system == 'windows'
  alohadb_schema_change_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_schema_change',
    '--FILEDESC', 'alohadb_schema_change - online DDL',])
endif

alohadb_schema_change = shared_module('alohadb_schema_change',
  alohadb_schema_change_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_schema_change

install_data(
  'alohadb_schema_change.control',
  'alohadb_schema_change--1.0.sql',
  kwargs: contrib_data_args,
)
