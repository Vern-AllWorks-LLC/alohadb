# Copyright (c) 2025, AlohaDB

if not zstd.found()
  subdir_done()
endif

pg_columnar_sources = files(
  'pg_columnar.c',
  'columnar_storage.c',
)

if host_system == 'windows'
  pg_columnar_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_columnar',
    '--FILEDESC', 'pg_columnar - columnar storage with zstd compression',])
endif

pg_columnar = shared_module('pg_columnar',
  pg_columnar_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [zstd, contrib_mod_args.get('dependencies', [])],
  },
)
contrib_targets += pg_columnar

install_data(
  'pg_columnar.control',
  'pg_columnar--1.0.sql',
  kwargs: contrib_data_args,
)
