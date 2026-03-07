# Copyright (c) 2025, AlohaDB

alohadb_branch_sources = files(
  'alohadb_branch.c',
)

if host_system == 'windows'
  alohadb_branch_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_branch',
    '--FILEDESC', 'alohadb_branch - lightweight database branching',])
endif

alohadb_branch = shared_module('alohadb_branch',
  alohadb_branch_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_branch

install_data(
  'alohadb_branch.control',
  'alohadb_branch--1.0.sql',
  kwargs: contrib_data_args,
)
