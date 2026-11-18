# Copyright (c) 2025, AlohaDB

pg_ontology_sources = files(
  'pg_ontology.c',
)

if host_system == 'windows'
  pg_ontology_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_ontology',
    '--FILEDESC', 'pg_ontology - triple-store context graph for entity relationship mapping',])
endif

pg_ontology = shared_module('pg_ontology',
  pg_ontology_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_ontology

install_data(
  'pg_ontology.control',
  'pg_ontology--1.0.sql',
  'pg_ontology--1.0--1.1.sql',
  'pg_ontology--1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_ontology',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'ontology',
    ],
  },
}
