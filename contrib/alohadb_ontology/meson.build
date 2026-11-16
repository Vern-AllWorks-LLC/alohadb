# Copyright (c) 2025, AlohaDB

alohadb_ontology_sources = files(
  'alohadb_ontology.c',
)

if host_system == 'windows'
  alohadb_ontology_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_ontology',
    '--FILEDESC', 'alohadb_ontology - triple-store context graph for entity relationship mapping',])
endif

alohadb_ontology = shared_module('alohadb_ontology',
  alohadb_ontology_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_ontology

install_data(
  'alohadb_ontology.control',
  'alohadb_ontology--1.0.sql',
  kwargs: contrib_data_args,
)
