# Copyright (c) 2025, AlohaDB

# Detect ONNX Runtime availability.
onnxruntime_dep = dependency('libonnxruntime', required: false)

pg_inference_sources = files(
  'pg_inference.c',
  'model_cache.c',
)

pg_inference_c_args = []
pg_inference_deps = []

if onnxruntime_dep.found()
  pg_inference_c_args += '-DHAVE_ONNXRUNTIME'
  pg_inference_deps += onnxruntime_dep
endif

if host_system == 'windows'
  pg_inference_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_inference',
    '--FILEDESC', 'pg_inference - in-database ML inference using ONNX Runtime',])
endif

pg_inference = shared_module('pg_inference',
  pg_inference_sources,
  c_args: pg_inference_c_args,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': pg_inference_deps + contrib_mod_args.get('dependencies', []),
  },
)
contrib_targets += pg_inference

install_data(
  'pg_inference.control',
  'pg_inference--1.0.sql',
  kwargs: contrib_data_args,
)
