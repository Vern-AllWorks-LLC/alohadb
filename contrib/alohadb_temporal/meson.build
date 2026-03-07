# Copyright (c) 2025, AlohaDB

alohadb_temporal_sources = files(
  'alohadb_temporal.c',
)

if host_system == 'windows'
  alohadb_temporal_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_temporal',
    '--FILEDESC', 'alohadb_temporal - SQL:2011-style system-versioned temporal tables',])
endif

alohadb_temporal = shared_module('alohadb_temporal',
  alohadb_temporal_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_temporal

install_data(
  'alohadb_temporal.control',
  'alohadb_temporal--1.0.sql',
  kwargs: contrib_data_args,
)
