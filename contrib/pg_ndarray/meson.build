# Copyright (c) 2026, AlohaDB

if not zstd.found()
  subdir_done()
endif

pg_ndarray_sources = files(
  'pg_ndarray.c',
  'ndarray_tensor.c',
)

if host_system == 'windows'
  pg_ndarray_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_ndarray',
    '--FILEDESC', 'pg_ndarray - chunked, compressed, sliceable N-d arrays',])
endif

pg_ndarray = shared_module('pg_ndarray',
  pg_ndarray_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [zstd, contrib_mod_args.get('dependencies', [])],
  },
)
contrib_targets += pg_ndarray

install_data(
  'pg_ndarray.control',
  'pg_ndarray--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_ndarray',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_ndarray',
    ],
  },
}
