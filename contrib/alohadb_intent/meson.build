# Copyright (c) 2025, AlohaDB

alohadb_intent_sources = files(
  'alohadb_intent.c',
)

if host_system == 'windows'
  alohadb_intent_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_intent',
    '--FILEDESC', 'alohadb_intent - document intent classification and why-indexing',])
endif

alohadb_intent = shared_module('alohadb_intent',
  alohadb_intent_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_intent

install_data(
  'alohadb_intent.control',
  'alohadb_intent--1.0.sql',
  kwargs: contrib_data_args,
)
