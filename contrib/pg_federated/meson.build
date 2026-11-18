# Copyright (c) 2025, AlohaDB

if not libcurl.found()
  subdir_done()
endif

pg_federated_sources = files(
  'pg_federated.c',
  'flower_client.c',
  'local_train.c',
)

if host_system == 'windows'
  pg_federated_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_federated',
    '--FILEDESC', 'pg_federated - federated learning with Flower framework',])
endif

pg_federated = shared_module('pg_federated',
  pg_federated_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [libcurl, contrib_mod_args['dependencies']],
  },
)
contrib_targets += pg_federated

install_data(
  'pg_federated.control',
  'pg_federated--1.0.sql',
  kwargs: contrib_data_args,
)
