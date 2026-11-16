# Copyright (c) 2025, AlohaDB

alohadb_python_sources = files(
  'alohadb_python.c',
)

if host_system == 'windows'
  alohadb_python_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_python',
    '--FILEDESC', 'alohadb_python - Python as a first-class workload',])
endif

alohadb_python = shared_module('alohadb_python',
  alohadb_python_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_python

install_data(
  'alohadb_python.control',
  'alohadb_python--1.0.sql',
  kwargs: contrib_data_args,
)
