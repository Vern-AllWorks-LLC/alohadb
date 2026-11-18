pg_contagg_sources = files(
  'pg_contagg.c',
  'hyperfunc.c',
)

if host_system == 'windows'
  pg_contagg_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_contagg',
    '--FILEDESC', 'pg_contagg - continuous aggregates and hyperfunctions',
  ])
endif

pg_contagg = shared_module('pg_contagg',
  pg_contagg_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_contagg

install_data(
  'pg_contagg.control',
  'pg_contagg--1.0.sql',
  kwargs: contrib_data_args,
)
