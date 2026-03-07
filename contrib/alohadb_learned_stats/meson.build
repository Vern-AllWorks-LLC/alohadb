# Copyright (c) 2025, AlohaDB

alohadb_learned_stats_sources = files(
  'learned_stats.c',
  'model.c',
)

if host_system == 'windows'
  alohadb_learned_stats_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_learned_stats',
    '--FILEDESC', 'alohadb_learned_stats - ML-assisted cardinality estimation',])
endif

alohadb_learned_stats = shared_module('alohadb_learned_stats',
  alohadb_learned_stats_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_learned_stats

install_data(
  'learned_stats.control',
  'learned_stats--1.0.sql',
  kwargs: contrib_data_args,
)
