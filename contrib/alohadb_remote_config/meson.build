# Copyright (c) 2025, OpenCAN / AlohaDB

alohadb_remote_config_sources = files(
  'alohadb_remote_config.c',
)

alohadb_remote_config = shared_module('alohadb_remote_config',
  alohadb_remote_config_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_remote_config

install_data(
  'alohadb_remote_config.control',
  'alohadb_remote_config--1.0.sql',
  kwargs: contrib_data_args,
)
