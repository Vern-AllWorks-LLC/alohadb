pg_vecexec_sources = files(
  'pg_vecexec.c',
)

if host_system == 'windows'
  pg_vecexec_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_vecexec',
    '--FILEDESC', 'AlohaDB Vectorize - analytical query acceleration',
  ])
endif

pg_vecexec = shared_module('pg_vecexec',
  pg_vecexec_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_vecexec

install_data(
  'pg_vecexec.control',
  'pg_vecexec--1.0.sql',
  kwargs: contrib_data_args,
)
