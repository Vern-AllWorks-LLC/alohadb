# Copyright (c) 2025, AlohaDB

alohadb_cron_sources = files(
  'alohadb_cron.c',
  'cron_parser.c',
)

if host_system == 'windows'
  alohadb_cron_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_cron',
    '--FILEDESC', 'alohadb_cron - job scheduling with cron syntax',])
endif

alohadb_cron = shared_module('alohadb_cron',
  alohadb_cron_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_cron

install_data(
  'alohadb_cron.control',
  'alohadb_cron--1.0.sql',
  kwargs: contrib_data_args,
)
