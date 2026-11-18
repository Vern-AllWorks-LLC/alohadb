# Copyright (c) 2025, AlohaDB

pg_unified_sources = files(
  'pg_unified.c',
)

if host_system == 'windows'
  pg_unified_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_unified',
    '--FILEDESC', 'pg_unified - unified streaming and batch query model',])
endif

pg_unified = shared_module('pg_unified',
  pg_unified_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_unified

install_data(
  'pg_unified.control',
  'pg_unified--1.0.sql',
  'pg_unified--1.0--1.1.sql',
  'pg_unified--1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_unified',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'unified',
    ],
  },
}
