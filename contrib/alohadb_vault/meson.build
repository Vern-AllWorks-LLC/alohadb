# Copyright (c) 2025, AlohaDB

alohadb_vault_sources = files(
  'alohadb_vault.c',
)

if host_system == 'windows'
  alohadb_vault_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_vault',
    '--FILEDESC', 'alohadb_vault - encrypted secrets store using pgcrypto',])
endif

alohadb_vault = shared_module('alohadb_vault',
  alohadb_vault_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_vault

install_data(
  'alohadb_vault.control',
  'alohadb_vault--1.0.sql',
  kwargs: contrib_data_args,
)
