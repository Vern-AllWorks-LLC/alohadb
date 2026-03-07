# Copyright (c) 2025, AlohaDB
alohadb_realtime_sources = files('alohadb_realtime.c')
if host_system == 'windows'
  alohadb_realtime_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_realtime',
    '--FILEDESC', 'alohadb_realtime - realtime change notifications',])
endif
alohadb_realtime = shared_module('alohadb_realtime',
  alohadb_realtime_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_realtime
install_data(
  'alohadb_realtime.control',
  'alohadb_realtime--1.0.sql',
  kwargs: contrib_data_args,
)
