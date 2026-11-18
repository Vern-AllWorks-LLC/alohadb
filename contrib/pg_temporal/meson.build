# Copyright (c) 2025, AlohaDB

pg_temporal_sources = files(
  'pg_temporal.c',
)

if host_system == 'windows'
  pg_temporal_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_temporal',
    '--FILEDESC', 'pg_temporal - SQL:2011-style system-versioned temporal tables',])
endif

pg_temporal = shared_module('pg_temporal',
  pg_temporal_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_temporal

install_data(
  'pg_temporal.control',
  'pg_temporal--1.0.sql',
  'pg_temporal--1.0--1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_temporal',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'temporal',
    ],
  },
}
