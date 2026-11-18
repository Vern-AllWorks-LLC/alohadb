# Copyright (c) 2025, AlohaDB

install_data(
  'pg_lineage.control',
  'pg_lineage--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_lineage',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'lineage',
      'lineage_bitemporal',
    ],
  },
}
