# Copyright (c) 2025, AlohaDB

if not zstd.found()
  subdir_done()
endif

alohadb_columnar_sources = files(
  'alohadb_columnar.c',
  'columnar_storage.c',
)

if host_system == 'windows'
  alohadb_columnar_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_columnar',
    '--FILEDESC', 'alohadb_columnar - columnar storage with zstd compression',])
endif

alohadb_columnar = shared_module('alohadb_columnar',
  alohadb_columnar_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [zstd, contrib_mod_args.get('dependencies', [])],
  },
)
contrib_targets += alohadb_columnar

install_data(
  'alohadb_columnar.control',
  'alohadb_columnar--1.0.sql',
  kwargs: contrib_data_args,
)
