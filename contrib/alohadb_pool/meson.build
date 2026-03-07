# Copyright (c) 2025, AlohaDB

alohadb_pool_sources = files(
  'alohadb_pool.c',
)

if host_system == 'windows'
  alohadb_pool_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_pool',
    '--FILEDESC', 'alohadb_pool - built-in connection pooling',])
endif

alohadb_pool = shared_module('alohadb_pool',
  alohadb_pool_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_pool

install_data(
  'alohadb_pool.control',
  'alohadb_pool--1.0.sql',
  kwargs: contrib_data_args,
)
