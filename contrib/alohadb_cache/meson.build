# Copyright (c) 2025, AlohaDB

alohadb_cache_sources = files(
  'alohadb_cache.c',
  'cache_store.c',
)

if host_system == 'windows'
  alohadb_cache_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_cache',
    '--FILEDESC', 'alohadb_cache - shared memory query result cache',])
endif

alohadb_cache = shared_module('alohadb_cache',
  alohadb_cache_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_cache

install_data(
  'alohadb_cache.control',
  'alohadb_cache--1.0.sql',
  kwargs: contrib_data_args,
)
