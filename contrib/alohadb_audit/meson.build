# Copyright (c) 2025-2026, OpenCAN / AlohaDB

openssl_dep = dependency('openssl', required: true)

alohadb_audit_sources = files(
  'alohadb_audit.c',
)

alohadb_audit = shared_module('alohadb_audit',
  alohadb_audit_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [openssl_dep],
  },
)
contrib_targets += alohadb_audit

install_data(
  'alohadb_audit.control',
  'alohadb_audit--1.0.sql',
  'alohadb_audit--1.0--1.1.sql',
  kwargs: contrib_data_args,
)
