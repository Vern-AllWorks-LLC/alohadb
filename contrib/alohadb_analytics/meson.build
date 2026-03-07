alohadb_analytics_sources = files(
  'alohadb_analytics.c',
  'hyperfunc.c',
)

if host_system == 'windows'
  alohadb_analytics_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_analytics',
    '--FILEDESC', 'alohadb_analytics - continuous aggregates and hyperfunctions',
  ])
endif

alohadb_analytics = shared_module('alohadb_analytics',
  alohadb_analytics_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_analytics

install_data(
  'alohadb_analytics.control',
  'alohadb_analytics--1.0.sql',
  kwargs: contrib_data_args,
)
