# Copyright (c) 2025, AlohaDB

alohadb_tiering_sources = files(
  'alohadb_tiering.c',
)

if host_system == 'windows'
  alohadb_tiering_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_tiering',
    '--FILEDESC', 'alohadb_tiering - automatic storage tiering for partitioned tables',])
endif

alohadb_tiering = shared_module('alohadb_tiering',
  alohadb_tiering_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_tiering

install_data(
  'alohadb_tiering.control',
  'alohadb_tiering--1.0.sql',
  kwargs: contrib_data_args,
)
