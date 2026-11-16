# Copyright (c) 2025, AlohaDB

alohadb_pipeline_sources = files(
  'alohadb_pipeline.c',
)

if host_system == 'windows'
  alohadb_pipeline_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_pipeline',
    '--FILEDESC', 'alohadb_pipeline - DAG-based ETL pipeline orchestration',])
endif

alohadb_pipeline = shared_module('alohadb_pipeline',
  alohadb_pipeline_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_pipeline

install_data(
  'alohadb_pipeline.control',
  'alohadb_pipeline--1.0.sql',
  kwargs: contrib_data_args,
)
