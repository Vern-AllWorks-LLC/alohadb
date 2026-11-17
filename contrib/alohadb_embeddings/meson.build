# Copyright (c) 2025, AlohaDB

install_data(
  'alohadb_embeddings.control',
  'alohadb_embeddings--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_embeddings',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'alohadb_embeddings',
    ],
  },
}
