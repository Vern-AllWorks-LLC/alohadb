# Copyright (c) 2025, AlohaDB

pg_restapi_sources = files(
  'pg_restapi.c',
  'restapi_http.c',
  'restapi_handler.c',
)

if host_system == 'windows'
  pg_restapi_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_restapi',
    '--FILEDESC', 'pg_restapi - auto-generated REST API with HTTP background worker',])
endif

pg_restapi = shared_module('pg_restapi',
  pg_restapi_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_restapi

install_data(
  'pg_restapi.control',
  'pg_restapi--1.0.sql',
  kwargs: contrib_data_args,
)
