# Copyright (c) 2026, AlohaDB

install_data(
  'pg_kv.control',
  'pg_kv--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_kv',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_kv',
    ],
  },
}
