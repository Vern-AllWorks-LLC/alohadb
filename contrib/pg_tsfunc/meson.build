# Copyright (c) 2026, AlohaDB

pg_tsfunc_sources = files(
  'pg_tsfunc.c',
  'tscodec.c',
)

if host_system == 'windows'
  pg_tsfunc_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_tsfunc',
    '--FILEDESC', 'pg_tsfunc - scientific time-series functions',])
endif

pg_tsfunc = shared_module('pg_tsfunc',
  pg_tsfunc_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_tsfunc

install_data(
  'pg_tsfunc.control',
  'pg_tsfunc--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_tsfunc',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'tsfunc_lttb',
      'tsfunc_resample',
      'tsfunc_asof',
      'tsfunc_codec',
    ],
  },
}
