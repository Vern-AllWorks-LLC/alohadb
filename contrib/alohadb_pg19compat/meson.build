# Copyright (c) 2026, AlohaDB

install_data(
  'alohadb_pg19compat.control',
  'alohadb_pg19compat--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_pg19compat',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg19compat',
    ],
  },
}
