# Copyright (c) 2026, AlohaDB / OpenCan.ai

openssl_dep = dependency('openssl', required: true)
zlib_dep = dependency('zlib', required: true)

pg_codefs_sources = files(
  'pg_codefs.c',
  'pg_codefs_worker.c',
)

if host_system == 'windows'
  pg_codefs_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_codefs',
    '--FILEDESC', 'pg_codefs - virtual filesystem with git-like versioning',])
endif

pg_codefs = shared_module('pg_codefs',
  pg_codefs_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [openssl_dep, zlib_dep],
  },
)
contrib_targets += pg_codefs

install_data(
  'pg_codefs.control',
  'pg_codefs--1.0.sql',
  kwargs: contrib_data_args,
)
