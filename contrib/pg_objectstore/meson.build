# Copyright (c) 2025, AlohaDB
# Requires libcurl for HTTPS (S3/GCS/Azure all require TLS).

curl_dep = dependency('libcurl', required: true)
openssl_dep = dependency('openssl', required: true)

pg_objectstore_sources = files(
  'pg_objectstore.c',
  'objectstore_s3.c',
  'objectstore_http.c',
)

if host_system == 'windows'
  pg_objectstore_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_objectstore',
    '--FILEDESC', 'pg_objectstore - S3/GCS/Azure cloud storage',])
endif

pg_objectstore = shared_module('pg_objectstore',
  pg_objectstore_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [curl_dep, openssl_dep, contrib_mod_args['dependencies']],
  },
)
contrib_targets += pg_objectstore

install_data(
  'pg_objectstore.control',
  'pg_objectstore--1.0.sql',
  kwargs: contrib_data_args,
)
