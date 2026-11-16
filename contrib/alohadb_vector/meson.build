# Copyright (c) 2025, AlohaDB

alohadb_vector_sources = files(
  'alohadb_vector.c',
  'vector_ivfflat.c',
)

if host_system == 'windows'
  alohadb_vector_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_vector',
    '--FILEDESC', 'alohadb_vector - vector type and IVFFlat ANN index',])
endif

alohadb_vector = shared_module('alohadb_vector',
  alohadb_vector_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_vector

install_data(
  'alohadb_vector.control',
  'alohadb_vector--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_vector',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'vector',
    ],
  },
}
