# Copyright (c) 2025, AlohaDB

pg_vector_sources = files(
  'pg_vector.c',
  'vector_ivfflat.c',
)

if host_system == 'windows'
  pg_vector_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_vector',
    '--FILEDESC', 'pg_vector - vector type and IVFFlat ANN index',])
endif

pg_vector = shared_module('pg_vector',
  pg_vector_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_vector

install_data(
  'pg_vector.control',
  'pg_vector--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_vector',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'vector',
    ],
  },
}
