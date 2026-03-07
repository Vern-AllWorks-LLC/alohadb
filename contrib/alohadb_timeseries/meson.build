# Copyright (c) 2025, AlohaDB

alohadb_timeseries_sources = files(
  'alohadb_timeseries.c',
)

if host_system == 'windows'
  alohadb_timeseries_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_timeseries',
    '--FILEDESC', 'alohadb_timeseries - auto-partition management for time-series tables',])
endif

alohadb_timeseries = shared_module('alohadb_timeseries',
  alohadb_timeseries_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_timeseries

install_data(
  'alohadb_timeseries.control',
  'alohadb_timeseries--1.0.sql',
  kwargs: contrib_data_args,
)
