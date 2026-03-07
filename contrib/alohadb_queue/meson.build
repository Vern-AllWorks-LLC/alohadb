# Copyright (c) 2025, AlohaDB

alohadb_queue_sources = files(
  'alohadb_queue.c',
  'queue_ops.c',
)

if host_system == 'windows'
  alohadb_queue_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_queue',
    '--FILEDESC', 'alohadb_queue - message queue extension',])
endif

alohadb_queue = shared_module('alohadb_queue',
  alohadb_queue_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_queue

install_data(
  'alohadb_queue.control',
  'alohadb_queue--1.0.sql',
  kwargs: contrib_data_args,
)
