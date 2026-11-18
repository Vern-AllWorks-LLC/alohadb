# Copyright (c) 2025, AlohaDB

pg_queue_sources = files(
  'pg_queue.c',
  'queue_ops.c',
)

if host_system == 'windows'
  pg_queue_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_queue',
    '--FILEDESC', 'pg_queue - message queue extension',])
endif

pg_queue = shared_module('pg_queue',
  pg_queue_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_queue

install_data(
  'pg_queue.control',
  'pg_queue--1.0.sql',
  kwargs: contrib_data_args,
)
