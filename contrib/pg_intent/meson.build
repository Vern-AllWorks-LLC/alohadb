# Copyright (c) 2025, AlohaDB

pg_intent_sources = files(
  'pg_intent.c',
)

if host_system == 'windows'
  pg_intent_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_intent',
    '--FILEDESC', 'pg_intent - document intent classification and why-indexing',])
endif

pg_intent = shared_module('pg_intent',
  pg_intent_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_intent

install_data(
  'pg_intent.control',
  'pg_intent--1.0.sql',
  'pg_intent--1.0--1.1.sql',
  'pg_intent--1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_intent',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'intent',
    ],
  },
}
