# Copyright (c) 2025, AlohaDB

pg_context_sources = files(
  'pg_context.c',
)

if host_system == 'windows'
  pg_context_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_context',
    '--FILEDESC', 'pg_context - entity disambiguation and context-aware search reranking',])
endif

pg_context = shared_module('pg_context',
  pg_context_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_context

install_data(
  'pg_context.control',
  'pg_context--1.0.sql',
  'pg_context--1.0--1.1.sql',
  'pg_context--1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_context',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'context',
    ],
  },
}
