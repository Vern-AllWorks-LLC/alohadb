# Copyright (c) 2025, AlohaDB

install_data(
  'alohadb_lineage.control',
  'alohadb_lineage--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_lineage',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'lineage',
      'lineage_bitemporal',
    ],
  },
}
