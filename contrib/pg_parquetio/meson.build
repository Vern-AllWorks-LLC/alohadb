# Copyright (c) 2025, AlohaDB

pg_parquetio_sources = files(
  'pg_parquetio.c',
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
  pg_parquetio_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_parquetio',
    '--FILEDESC', 'pg_parquetio - Parquet, CSV, and JSON reader; Parquet and Arrow writer',])
endif

pg_parquetio = shared_module('pg_parquetio',
  pg_parquetio_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_parquetio

install_data(
  'pg_parquetio.control',
  'pg_parquetio--1.0.sql',
  'pg_parquetio--1.0--1.1.sql',
  'pg_parquetio--1.1--1.2.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_parquetio',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'parquet_v11',
      'parquet_write',
    ],
  },
}
