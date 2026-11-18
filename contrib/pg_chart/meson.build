pg_chart_sources = files(
  'pg_chart.c',
)

if host_system == 'windows'
  pg_chart_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_chart',
    '--FILEDESC', 'pg_chart - SVG chart generation from SQL queries',
  ])
endif

pg_chart = shared_module('pg_chart',
  pg_chart_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_chart

install_data(
  'pg_chart.control',
  'pg_chart--1.0.sql',
  kwargs: contrib_data_args,
)
