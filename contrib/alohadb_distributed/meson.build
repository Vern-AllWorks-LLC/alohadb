alohadb_distributed_sources = files(
  'alohadb_distributed.c',
)

if host_system == 'windows'
  alohadb_distributed_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_distributed',
    '--FILEDESC', 'AlohaDB Distributed - horizontal sharding',
  ])
endif

alohadb_distributed = shared_module('alohadb_distributed',
  alohadb_distributed_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_distributed

install_data(
  'alohadb_distributed.control',
  'alohadb_distributed--1.0.sql',
  kwargs: contrib_data_args,
)
