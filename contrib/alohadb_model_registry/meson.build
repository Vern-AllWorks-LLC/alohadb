# Copyright (c) 2025, AlohaDB

alohadb_model_registry_sources = files(
  'alohadb_model_registry.c',
)

if host_system == 'windows'
  alohadb_model_registry_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_model_registry',
    '--FILEDESC', 'alohadb_model_registry - ML model versioning and deployment',])
endif

alohadb_model_registry = shared_module('alohadb_model_registry',
  alohadb_model_registry_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_model_registry

install_data(
  'alohadb_model_registry.control',
  'alohadb_model_registry--1.0.sql',
  kwargs: contrib_data_args,
)
