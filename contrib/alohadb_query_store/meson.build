alohadb_query_store_sources = files(
  'alohadb_query_store.c',
  'qs_index_advisor.c',
)

if host_system == 'windows'
  alohadb_query_store_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_query_store',
    '--FILEDESC', 'alohadb_query_store - query store and index advisor',
  ])
endif

alohadb_query_store = shared_module('alohadb_query_store',
  alohadb_query_store_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_query_store

install_data(
  'alohadb_query_store.control',
  'alohadb_query_store--1.0.sql',
  kwargs: contrib_data_args,
)
