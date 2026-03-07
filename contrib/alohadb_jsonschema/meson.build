# Copyright (c) 2025, AlohaDB

alohadb_jsonschema_sources = files(
  'alohadb_jsonschema.c',
  'jsonschema_validator.c',
)

if host_system == 'windows'
  alohadb_jsonschema_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_jsonschema',
    '--FILEDESC', 'alohadb_jsonschema - JSON Schema validation',])
endif

alohadb_jsonschema = shared_module('alohadb_jsonschema',
  alohadb_jsonschema_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_jsonschema

install_data(
  'alohadb_jsonschema.control',
  'alohadb_jsonschema--1.0.sql',
  kwargs: contrib_data_args,
)
