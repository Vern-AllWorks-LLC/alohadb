# Copyright (c) 2025, AlohaDB

if not libcurl.found()
  subdir_done()
endif

alohadb_federated_sources = files(
  'alohadb_federated.c',
  'flower_client.c',
  'local_train.c',
)

if host_system == 'windows'
  alohadb_federated_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_federated',
    '--FILEDESC', 'alohadb_federated - federated learning with Flower framework',])
endif

alohadb_federated = shared_module('alohadb_federated',
  alohadb_federated_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': [libcurl, contrib_mod_args['dependencies']],
  },
)
contrib_targets += alohadb_federated

install_data(
  'alohadb_federated.control',
  'alohadb_federated--1.0.sql',
  kwargs: contrib_data_args,
)
