# Copyright (c) 2025, AlohaDB

pg_model_registry_sources = files(
  'pg_model_registry.c',
)

if host_system == 'windows'
  pg_model_registry_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_model_registry',
    '--FILEDESC', 'pg_model_registry - ML model versioning and deployment',])
endif

pg_model_registry = shared_module('pg_model_registry',
  pg_model_registry_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_model_registry

install_data(
  'pg_model_registry.control',
  'pg_model_registry--1.0.sql',
  kwargs: contrib_data_args,
)
