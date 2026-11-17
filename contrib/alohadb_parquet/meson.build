# Copyright (c) 2025, AlohaDB

alohadb_parquet_sources = files(
  'alohadb_parquet.c',
  'csv_reader.c',
  'json_reader.c',
  'parquet_reader.c',
  'parquet_full.c',
  'pq_common.c',
  'export_common.c',
  'parquet_writer.c',
  'arrow_writer.c',
)

if host_system == 'windows'
  alohadb_parquet_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_parquet',
    '--FILEDESC', 'alohadb_parquet - Parquet, CSV, and JSON reader; Parquet and Arrow writer',])
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
  'alohadb_parquet--1.0--1.1.sql',
  'alohadb_parquet--1.1--1.2.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_parquet',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'parquet_v11',
      'parquet_write',
    ],
  },
}
