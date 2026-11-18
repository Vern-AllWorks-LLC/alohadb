# Copyright (c) 2025, AlohaDB

pg_vault_sources = files(
  'pg_vault.c',
)

if host_system == 'windows'
  pg_vault_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_vault',
    '--FILEDESC', 'pg_vault - encrypted secrets store using pgcrypto',])
endif

pg_vault = shared_module('pg_vault',
  pg_vault_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_vault

install_data(
  'pg_vault.control',
  'pg_vault--1.0.sql',
  kwargs: contrib_data_args,
)
