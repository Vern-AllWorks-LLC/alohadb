# Copyright (c) 2025, AlohaDB

alohadb_parquet_sources = files(
  'alohadb_parquet.c',
  'csv_reader.c',
  'json_reader.c',
  'parquet_reader.c',
)

if host_system == 'windows'
  alohadb_parquet_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_parquet',
    '--FILEDESC', 'alohadb_parquet - Parquet, CSV, and JSON file reader',])
endif

alohadb_parquet = shared_module('alohadb_parquet',
  alohadb_parquet_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_parquet

install_data(
  'alohadb_parquet.control',
  'alohadb_parquet--1.0.sql',
  kwargs: contrib_data_args,
)
