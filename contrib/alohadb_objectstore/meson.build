# Copyright (c) 2025, AlohaDB
# Requires libcurl for HTTPS (S3/GCS/Azure all require TLS).

curl_dep = dependency('libcurl', required: true)
openssl_dep = dependency('openssl', required: true)

alohadb_objectstore_sources = files(
  'alohadb_objectstore.c',
  'objectstore_s3.c',
  'objectstore_http.c',
)

if host_system == 'windows'
  alohadb_objectstore_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_objectstore',
    '--FILEDESC', 'alohadb_objectstore - S3/GCS/Azure cloud storage',])
endif

alohadb_objectstore = shared_module('alohadb_objectstore',
  alohadb_objectstore_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [curl_dep, openssl_dep, contrib_mod_args['dependencies']],
  },
)
contrib_targets += alohadb_objectstore

install_data(
  'alohadb_objectstore.control',
  'alohadb_objectstore--1.0.sql',
  kwargs: contrib_data_args,
)
