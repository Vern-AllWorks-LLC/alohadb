# Copyright (c) 2025, AlohaDB

pg_geo_sources = files(
  'pg_geo.c',
  'geo_ops.c',
)

if host_system == 'windows'
  pg_geo_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_geo',
    '--FILEDESC', 'pg_geo - 2D geometry types and spatial operations',])
endif

pg_geo = shared_module('pg_geo',
  pg_geo_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_geo

install_data(
  'pg_geo.control',
  'pg_geo--1.0.sql',
  kwargs: contrib_data_args,
)
