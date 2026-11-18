# Copyright (c) 2026, AlohaDB

pg_measure_sources = files(
  'pg_measure.c',
)

if host_system == 'windows'
  pg_measure_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_measure',
    '--FILEDESC', 'pg_measure - measurements with units, uncertainty and provenance',])
endif

pg_measure = shared_module('pg_measure',
  pg_measure_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_measure

install_data(
  'pg_measure.control',
  'pg_measure--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_measure',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'measure_type',
      'measure_math',
      'measure_prov',
    ],
  },
}
