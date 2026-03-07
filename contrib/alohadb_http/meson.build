# Copyright (c) 2025, AlohaDB

alohadb_http_sources = files(
  'alohadb_http.c',
  'http_client.c',
)

if host_system == 'windows'
  alohadb_http_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_http',
    '--FILEDESC', 'alohadb_http - HTTP client extension',])
endif

alohadb_http = shared_module('alohadb_http',
  alohadb_http_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_http

install_data(
  'alohadb_http.control',
  'alohadb_http--1.0.sql',
  kwargs: contrib_data_args,
)
