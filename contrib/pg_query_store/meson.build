pg_query_store_sources = files(
  'pg_query_store.c',
  'qs_index_advisor.c',
)

if host_system == 'windows'
  pg_query_store_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_query_store',
    '--FILEDESC', 'pg_query_store - query store and index advisor',
  ])
endif

pg_query_store = shared_module('pg_query_store',
  pg_query_store_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_query_store

install_data(
  'pg_query_store.control',
  'pg_query_store--1.0.sql',
  kwargs: contrib_data_args,
)
