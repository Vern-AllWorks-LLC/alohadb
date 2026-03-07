# Copyright (c) 2025, AlohaDB

alohadb_search_sources = files(
  'alohadb_search.c',
  'search_bm25.c',
  'search_fuzzy.c',
  'search_suggest.c',
  'search_geo.c',
)

if host_system == 'windows'
  alohadb_search_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_search',
    '--FILEDESC', 'alohadb_search - advanced full-text search',])
endif

alohadb_search = shared_module('alohadb_search',
  alohadb_search_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_search

install_data(
  'alohadb_search.control',
  'alohadb_search--1.0.sql',
  kwargs: contrib_data_args,
)
