# Copyright (c) 2025, AlohaDB

pg_json_schema_sources = files(
  'pg_json_schema.c',
  'jsonschema_validator.c',
)

if host_system == 'windows'
  pg_json_schema_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_json_schema',
    '--FILEDESC', 'pg_json_schema - JSON Schema validation',])
endif

pg_json_schema = shared_module('pg_json_schema',
  pg_json_schema_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_json_schema

install_data(
  'pg_json_schema.control',
  'pg_json_schema--1.0.sql',
  kwargs: contrib_data_args,
)
