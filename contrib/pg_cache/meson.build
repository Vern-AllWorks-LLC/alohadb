# Copyright (c) 2025-2026, AlohaDB

pg_cache_sources = files(
  'pg_cache.c',
  'cache_store.c',
)

if host_system == 'windows'
  pg_cache_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_cache',
    '--FILEDESC', 'pg_cache - transactional shared-memory key-value cache',])
endif

pg_cache = shared_module('pg_cache',
  pg_cache_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_cache

install_data(
  'pg_cache.control',
  'pg_cache--2.0.sql',
  'pg_cache--1.0--2.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_cache',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_cache',
    ],
    'regress_args': ['--temp-config', files('pg_cache.conf')],
    # needs shared_preload_libraries = 'pg_cache'
    'runningcheck': false,
  },
}
