# Copyright (c) 2025, AlohaDB

pg_python_sources = files(
  'pg_python.c',
)

if host_system == 'windows'
  pg_python_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_python',
    '--FILEDESC', 'pg_python - Python as a first-class workload',])
endif

pg_python = shared_module('pg_python',
  pg_python_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_python

install_data(
  'pg_python.control',
  'pg_python--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_python',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'python',
    ],
  },
}
