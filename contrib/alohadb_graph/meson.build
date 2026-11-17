# Copyright (c) 2026, AlohaDB

alohadb_graph_sources = files(
  'alohadb_graph.c',
  'graph_algo.c',
)

if host_system == 'windows'
  alohadb_graph_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_graph',
    '--FILEDESC', 'alohadb_graph - property graphs over ordinary tables',])
endif

alohadb_graph = shared_module('alohadb_graph',
  alohadb_graph_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_graph

install_data(
  'alohadb_graph.control',
  'alohadb_graph--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'alohadb_graph',
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
