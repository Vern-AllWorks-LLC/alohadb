# Copyright (c) 2025, AlohaDB

install_data(
  'pg_bitemporal.control',
  'pg_bitemporal--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bitemporal',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bitemporal',
    ],
  },
}
