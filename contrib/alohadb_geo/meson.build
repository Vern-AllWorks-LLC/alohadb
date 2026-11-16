# Copyright (c) 2025, AlohaDB

alohadb_geo_sources = files(
  'alohadb_geo.c',
  'geo_ops.c',
)

if host_system == 'windows'
  alohadb_geo_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_geo',
    '--FILEDESC', 'alohadb_geo - 2D geometry types and spatial operations',])
endif

alohadb_geo = shared_module('alohadb_geo',
  alohadb_geo_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_geo

install_data(
  'alohadb_geo.control',
  'alohadb_geo--1.0.sql',
  kwargs: contrib_data_args,
)
