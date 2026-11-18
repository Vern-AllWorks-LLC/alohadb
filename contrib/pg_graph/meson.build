# Copyright (c) 2026, AlohaDB

pg_graph_sources = files(
  'pg_graph.c',
  'graph_algo.c',
)

if host_system == 'windows'
  pg_graph_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_graph',
    '--FILEDESC', 'pg_graph - property graphs over ordinary tables',])
endif

pg_graph = shared_module('pg_graph',
  pg_graph_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_graph

install_data(
  'pg_graph.control',
  'pg_graph--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_graph',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'graph_schema',
      'graph_traversal',
      'graph_algorithms',
      'graph_gnn',
    ],
  },
}
