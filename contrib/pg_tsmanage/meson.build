# Copyright (c) 2025, AlohaDB

pg_tsmanage_sources = files(
  'pg_tsmanage.c',
)

if host_system == 'windows'
  pg_tsmanage_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_tsmanage',
    '--FILEDESC', 'pg_tsmanage - auto-partition management for time-series tables',])
endif

pg_tsmanage = shared_module('pg_tsmanage',
  pg_tsmanage_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_tsmanage

install_data(
  'pg_tsmanage.control',
  'pg_tsmanage--1.0.sql',
  kwargs: contrib_data_args,
)
