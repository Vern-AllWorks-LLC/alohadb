# Copyright (c) 2025, OpenCAN / AlohaDB

pg_remote_config_sources = files(
  'pg_remote_config.c',
)

pg_remote_config = shared_module('pg_remote_config',
  pg_remote_config_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_remote_config

install_data(
  'pg_remote_config.control',
  'pg_remote_config--1.0.sql',
  kwargs: contrib_data_args,
)
