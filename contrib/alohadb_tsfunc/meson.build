# Copyright (c) 2026, AlohaDB

alohadb_tsfunc_sources = files(
  'alohadb_tsfunc.c',
  'tscodec.c',
)

if host_system == 'windows'
  alohadb_tsfunc_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_tsfunc',
    '--FILEDESC', 'alohadb_tsfunc - scientific time-series functions',])
endif

alohadb_tsfunc = shared_module('alohadb_tsfunc',
  alohadb_tsfunc_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_tsfunc

install_data(
  'alohadb_tsfunc.control',
  'alohadb_tsfunc--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_tsfunc',
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
