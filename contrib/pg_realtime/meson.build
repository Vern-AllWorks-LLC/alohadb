# Copyright (c) 2025, AlohaDB
pg_realtime_sources = files('pg_realtime.c')
if host_system == 'windows'
  pg_realtime_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_realtime',
    '--FILEDESC', 'pg_realtime - realtime change notifications',])
endif
pg_realtime = shared_module('pg_realtime',
  pg_realtime_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_realtime
install_data(
  'pg_realtime.control',
  'pg_realtime--1.0.sql',
  kwargs: contrib_data_args,
)
