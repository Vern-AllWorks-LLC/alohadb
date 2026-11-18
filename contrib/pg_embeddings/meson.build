# Copyright (c) 2025, AlohaDB

install_data(
  'pg_embeddings.control',
  'pg_embeddings--1.1.sql',
  'pg_embeddings--1.0--1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_embeddings',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_embeddings',
    ],
  },
}
