pg_distributed_sources = files(
  'pg_distributed.c',
)

if host_system == 'windows'
  pg_distributed_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_distributed',
    '--FILEDESC', 'AlohaDB Distributed - horizontal sharding',
  ])
endif

pg_distributed = shared_module('pg_distributed',
  pg_distributed_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_distributed

install_data(
  'pg_distributed.control',
  'pg_distributed--1.0.sql',
  kwargs: contrib_data_args,
)
