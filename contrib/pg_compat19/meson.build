# Copyright (c) 2026, AlohaDB

install_data(
  'pg_compat19.control',
  'pg_compat19--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_compat19',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_compat19',
    ],
  },
}
