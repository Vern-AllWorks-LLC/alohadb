# Copyright (c) 2025, AlohaDB

alohadb_context_sources = files(
  'alohadb_context.c',
)

if host_system == 'windows'
  alohadb_context_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_context',
    '--FILEDESC', 'alohadb_context - entity disambiguation and context-aware search reranking',])
endif

alohadb_context = shared_module('alohadb_context',
  alohadb_context_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_context

install_data(
  'alohadb_context.control',
  'alohadb_context--1.0.sql',
  kwargs: contrib_data_args,
)
