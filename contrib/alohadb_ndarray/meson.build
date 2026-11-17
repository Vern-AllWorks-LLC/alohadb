# Copyright (c) 2026, AlohaDB

if not zstd.found()
  subdir_done()
endif

alohadb_ndarray_sources = files(
  'alohadb_ndarray.c',
  'ndarray_tensor.c',
)

if host_system == 'windows'
  alohadb_ndarray_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_ndarray',
    '--FILEDESC', 'alohadb_ndarray - chunked, compressed, sliceable N-d arrays',])
endif

alohadb_ndarray = shared_module('alohadb_ndarray',
  alohadb_ndarray_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [zstd, contrib_mod_args.get('dependencies', [])],
  },
)
contrib_targets += alohadb_ndarray

install_data(
  'alohadb_ndarray.control',
  'alohadb_ndarray--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_ndarray',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'alohadb_ndarray',
    ],
  },
}
