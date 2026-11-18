# Copyright (c) 2025, AlohaDB

pg_branch_sources = files(
  'pg_branch.c',
)

if host_system == 'windows'
  pg_branch_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_branch',
    '--FILEDESC', 'pg_branch - lightweight database branching',])
endif

pg_branch = shared_module('pg_branch',
  pg_branch_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_branch

install_data(
  'pg_branch.control',
  'pg_branch--1.0.sql',
  kwargs: contrib_data_args,
)
