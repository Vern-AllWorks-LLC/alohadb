# Copyright (c) 2026, AlohaDB
#
# Conversion tools for clusters and scripts from AlohaDB 20.x, whose
# extensions were named alohadb_*.

alohadb_convert_inputs = files('generate.py', 'rename_map.json', 'common.py.in',
  'pg_alohadb_convert_sql.in', 'pg_alohadb_convert_db.in',
  'pg_alohadb_convert_db.sql.in')

custom_target('pg_alohadb_convert_programs',
  input: alohadb_convert_inputs,
  output: ['pg_alohadb_convert_sql', 'pg_alohadb_convert_db'],
  command: [python, '@INPUT0@', meson.current_source_dir(), '@OUTDIR@'],
  build_by_default: true,
  install: true,
  install_dir: dir_bin,
  install_mode: 'rwxr-xr-x',
)

custom_target('pg_alohadb_convert_sql_script',
  input: alohadb_convert_inputs,
  output: ['pg_alohadb_convert_db.sql'],
  command: [python, '@INPUT0@', meson.current_source_dir(), '@OUTDIR@'],
  build_by_default: true,
  install: true,
  install_dir: dir_data,
)
