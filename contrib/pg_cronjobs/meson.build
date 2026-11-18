# Copyright (c) 2025, AlohaDB

pg_cronjobs_sources = files(
  'pg_cronjobs.c',
  'cron_parser.c',
)

if host_system == 'windows'
  pg_cronjobs_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_cronjobs',
    '--FILEDESC', 'pg_cronjobs - job scheduling with cron syntax',])
endif

pg_cronjobs = shared_module('pg_cronjobs',
  pg_cronjobs_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_cronjobs

install_data(
  'pg_cronjobs.control',
  'pg_cronjobs--1.0.sql',
  kwargs: contrib_data_args,
)
