pg_ratelimit_sources = files(
  'pg_ratelimit.c',
  'session_store.c',
)

if host_system == 'windows'
  pg_ratelimit_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_ratelimit',
    '--FILEDESC', 'pg_ratelimit - rate limiting and session store',
  ])
endif

pg_ratelimit = shared_module('pg_ratelimit',
  pg_ratelimit_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_ratelimit

install_data(
  'pg_ratelimit.control',
  'pg_ratelimit--1.0.sql',
  kwargs: contrib_data_args,
)
