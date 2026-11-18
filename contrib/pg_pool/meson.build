# Copyright (c) 2025, AlohaDB

pg_pool_sources = files(
  'pg_pool.c',
)

if host_system == 'windows'
  pg_pool_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_pool',
    '--FILEDESC', 'pg_pool - built-in connection pooling',])
endif

pg_pool = shared_module('pg_pool',
  pg_pool_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_pool

install_data(
  'pg_pool.control',
  'pg_pool--1.0.sql',
  kwargs: contrib_data_args,
)
