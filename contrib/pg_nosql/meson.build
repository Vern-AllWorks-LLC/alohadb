# Copyright (c) 2025, AlohaDB

pg_nosql_sources = files(
  'pg_nosql.c',
  'nosql_crud.c',
  'nosql_query.c',
  'nosql_collection.c',
)

if host_system == 'windows'
  pg_nosql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_nosql',
    '--FILEDESC', 'pg_nosql - document database API with flexible schemas',])
endif

pg_nosql = shared_module('pg_nosql',
  pg_nosql_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_nosql

install_data(
  'pg_nosql.control',
  'pg_nosql--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_nosql',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'nosql',
    ],
  },
}
