pg_scale_sources = files(
  'pg_scale.c',
)

if host_system == 'windows'
  pg_scale_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_scale',
    '--FILEDESC', 'AlohaDB Scale - scale-to-zero management',
  ])
endif

pg_scale = shared_module('pg_scale',
  pg_scale_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_scale

install_data(
  'pg_scale.control',
  'pg_scale--1.0.sql',
  kwargs: contrib_data_args,
)
