# Copyright (c) 2025-2026, OpenCAN / AlohaDB

openssl_dep = dependency('openssl', required: true)

pg_audit_sources = files(
  'pg_audit.c',
)

pg_audit = shared_module('pg_audit',
  pg_audit_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [openssl_dep],
  },
)
contrib_targets += pg_audit

install_data(
  'pg_audit.control',
  'pg_audit--1.0.sql',
  'pg_audit--1.0--1.1.sql',
  kwargs: contrib_data_args,
)
