# Copyright (c) 2025, AlohaDB

alohadb_unified_sources = files(
  'alohadb_unified.c',
)

if host_system == 'windows'
  alohadb_unified_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_unified',
    '--FILEDESC', 'alohadb_unified - unified streaming and batch query model',])
endif

alohadb_unified = shared_module('alohadb_unified',
  alohadb_unified_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_unified

install_data(
  'alohadb_unified.control',
  'alohadb_unified--1.0.sql',
  kwargs: contrib_data_args,
)
