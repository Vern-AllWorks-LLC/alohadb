# Copyright (c) 2025, AlohaDB

pg_tiering_sources = files(
  'pg_tiering.c',
)

if host_system == 'windows'
  pg_tiering_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_tiering',
    '--FILEDESC', 'pg_tiering - automatic storage tiering for partitioned tables',])
endif

pg_tiering = shared_module('pg_tiering',
  pg_tiering_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_tiering

install_data(
  'pg_tiering.control',
  'pg_tiering--1.0.sql',
  kwargs: contrib_data_args,
)
