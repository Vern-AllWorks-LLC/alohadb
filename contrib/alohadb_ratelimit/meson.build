alohadb_ratelimit_sources = files(
  'alohadb_ratelimit.c',
  'session_store.c',
)

if host_system == 'windows'
  alohadb_ratelimit_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_ratelimit',
    '--FILEDESC', 'alohadb_ratelimit - rate limiting and session store',
  ])
endif

alohadb_ratelimit = shared_module('alohadb_ratelimit',
  alohadb_ratelimit_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_ratelimit

install_data(
  'alohadb_ratelimit.control',
  'alohadb_ratelimit--1.0.sql',
  kwargs: contrib_data_args,
)
