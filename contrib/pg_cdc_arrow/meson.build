# Copyright (c) 2025, AlohaDB Project

pg_cdc_arrow = shared_module('pg_cdc_arrow',
  files('pg_cdc_arrow.c', 'arrow_builder.c', 'arrow_ipc.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += pg_cdc_arrow

install_data(
  'pg_cdc_arrow.control',
  'pg_cdc_arrow--1.0.sql',
  kwargs: contrib_data_args,
)
