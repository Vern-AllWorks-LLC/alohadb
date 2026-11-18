# Copyright (c) 2025, AlohaDB

pg_pipeline_sources = files(
  'pg_pipeline.c',
)

if host_system == 'windows'
  pg_pipeline_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_pipeline',
    '--FILEDESC', 'pg_pipeline - DAG-based ETL pipeline orchestration',])
endif

pg_pipeline = shared_module('pg_pipeline',
  pg_pipeline_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_pipeline

install_data(
  'pg_pipeline.control',
  'pg_pipeline--1.0.sql',
  kwargs: contrib_data_args,
)
