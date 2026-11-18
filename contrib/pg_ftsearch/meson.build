# Copyright (c) 2025, AlohaDB

pg_ftsearch_sources = files(
  'pg_ftsearch.c',
  'search_bm25.c',
  'search_fuzzy.c',
  'search_suggest.c',
  'search_geo.c',
)

if host_system == 'windows'
  pg_ftsearch_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_ftsearch',
    '--FILEDESC', 'pg_ftsearch - advanced full-text search',])
endif

pg_ftsearch = shared_module('pg_ftsearch',
  pg_ftsearch_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_ftsearch

install_data(
  'pg_ftsearch.control',
  'pg_ftsearch--1.0.sql',
  kwargs: contrib_data_args,
)
