alohadb_scale_sources = files(
  'alohadb_scale.c',
)

if host_system == 'windows'
  alohadb_scale_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_scale',
    '--FILEDESC', 'AlohaDB Scale - scale-to-zero management',
  ])
endif

alohadb_scale = shared_module('alohadb_scale',
  alohadb_scale_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += alohadb_scale

install_data(
  'alohadb_scale.control',
  'alohadb_scale--1.0.sql',
  kwargs: contrib_data_args,
)
