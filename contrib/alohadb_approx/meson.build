# Copyright (c) 2025, AlohaDB

alohadb_approx_sources = files(
  'alohadb_approx.c',
  'hll_agg.c',
  'cms.c',
  'topk.c',
  'bloom_filter.c',
  'tdigest.c',
)

if host_system == 'windows'
  alohadb_approx_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_approx',
    '--FILEDESC', 'alohadb_approx - approximate query processing: HLL, CMS, Top-K',])
endif

alohadb_approx = shared_module('alohadb_approx',
  alohadb_approx_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_approx

install_data(
  'alohadb_approx.control',
  'alohadb_approx--1.0.sql',
  'alohadb_approx--1.0--1.1.sql',
  kwargs: contrib_data_args,
)
