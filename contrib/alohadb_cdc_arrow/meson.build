# Copyright (c) 2025, AlohaDB Project

alohadb_cdc_arrow = shared_module('alohadb_cdc_arrow',
  files('alohadb_cdc_arrow.c', 'arrow_builder.c', 'arrow_ipc.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_cdc_arrow

install_data(
  'alohadb_cdc_arrow.control',
  'alohadb_cdc_arrow--1.0.sql',
  kwargs: contrib_data_args,
)
