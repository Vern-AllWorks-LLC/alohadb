# Copyright (c) 2026, AlohaDB

alohadb_measure_sources = files(
  'alohadb_measure.c',
)

if host_system == 'windows'
  alohadb_measure_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_measure',
    '--FILEDESC', 'alohadb_measure - measurements with units, uncertainty and provenance',])
endif

alohadb_measure = shared_module('alohadb_measure',
  alohadb_measure_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_measure

install_data(
  'alohadb_measure.control',
  'alohadb_measure--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_measure',
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
