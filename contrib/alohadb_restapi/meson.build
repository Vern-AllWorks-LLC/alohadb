# Copyright (c) 2025, AlohaDB

alohadb_restapi_sources = files(
  'alohadb_restapi.c',
  'restapi_http.c',
  'restapi_handler.c',
)

if host_system == 'windows'
  alohadb_restapi_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_restapi',
    '--FILEDESC', 'alohadb_restapi - auto-generated REST API with HTTP background worker',])
endif

alohadb_restapi = shared_module('alohadb_restapi',
  alohadb_restapi_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_restapi

install_data(
  'alohadb_restapi.control',
  'alohadb_restapi--1.0.sql',
  kwargs: contrib_data_args,
)
