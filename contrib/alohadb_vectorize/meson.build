alohadb_vectorize_sources = files(
  'alohadb_vectorize.c',
)

if host_system == 'windows'
  alohadb_vectorize_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_vectorize',
    '--FILEDESC', 'AlohaDB Vectorize - analytical query acceleration',
  ])
endif

alohadb_vectorize = shared_module('alohadb_vectorize',
  alohadb_vectorize_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_vectorize

install_data(
  'alohadb_vectorize.control',
  'alohadb_vectorize--1.0.sql',
  kwargs: contrib_data_args,
)
