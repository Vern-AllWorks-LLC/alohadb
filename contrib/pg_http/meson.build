# Copyright (c) 2025, AlohaDB

pg_http_sources = files(
  'pg_http.c',
  'http_client.c',
)

if host_system == 'windows'
  pg_http_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_http',
    '--FILEDESC', 'pg_http - HTTP client extension',])
endif

pg_http = shared_module('pg_http',
  pg_http_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_http

install_data(
  'pg_http.control',
  'pg_http--1.0.sql',
  kwargs: contrib_data_args,
)
