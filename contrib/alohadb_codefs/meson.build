# Copyright (c) 2026, AlohaDB / OpenCan.ai

openssl_dep = dependency('openssl', required: true)
zlib_dep = dependency('zlib', required: true)

alohadb_codefs_sources = files(
  'alohadb_codefs.c',
  'alohadb_codefs_worker.c',
)

if host_system == 'windows'
  alohadb_codefs_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_codefs',
    '--FILEDESC', 'alohadb_codefs - virtual filesystem with git-like versioning',])
endif

alohadb_codefs = shared_module('alohadb_codefs',
  alohadb_codefs_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [openssl_dep, zlib_dep],
  },
)
contrib_targets += alohadb_codefs

install_data(
  'alohadb_codefs.control',
  'alohadb_codefs--1.0.sql',
  kwargs: contrib_data_args,
)
