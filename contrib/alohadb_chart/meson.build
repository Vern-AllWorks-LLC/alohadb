alohadb_chart_sources = files(
  'alohadb_chart.c',
)

if host_system == 'windows'
  alohadb_chart_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_chart',
    '--FILEDESC', 'alohadb_chart - SVG chart generation from SQL queries',
  ])
endif

alohadb_chart = shared_module('alohadb_chart',
  alohadb_chart_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_chart

install_data(
  'alohadb_chart.control',
  'alohadb_chart--1.0.sql',
  kwargs: contrib_data_args,
)
