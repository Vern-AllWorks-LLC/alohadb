# Copyright (c) 2025, AlohaDB

pg_approx_sources = files(
  'pg_approx.c',
  'hll_agg.c',
  'cms.c',
  'topk.c',
  'bloom_filter.c',
  'tdigest.c',
)

if host_system == 'windows'
  pg_approx_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_approx',
    '--FILEDESC', 'pg_approx - approximate query processing: HLL, CMS, Top-K',])
endif

pg_approx = shared_module('pg_approx',
  pg_approx_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_approx

install_data(
  'pg_approx.control',
  'pg_approx--1.0.sql',
  'pg_approx--1.0--1.1.sql',
  kwargs: contrib_data_args,
)
