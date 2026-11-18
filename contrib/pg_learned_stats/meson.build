# Copyright (c) 2025, AlohaDB

pg_learned_stats_sources = files(
  'learned_stats.c',
  'model.c',
)

if host_system == 'windows'
  pg_learned_stats_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_learned_stats',
    '--FILEDESC', 'pg_learned_stats - ML-assisted cardinality estimation',])
endif

pg_learned_stats = shared_module('pg_learned_stats',
  pg_learned_stats_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_learned_stats

install_data(
  'pg_learned_stats.control',
  'pg_learned_stats--1.0.sql',
  kwargs: contrib_data_args,
)
