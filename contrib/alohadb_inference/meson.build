# Copyright (c) 2025, AlohaDB

# Detect ONNX Runtime availability.
onnxruntime_dep = dependency('libonnxruntime', required: false)

alohadb_inference_sources = files(
  'alohadb_inference.c',
  'model_cache.c',
)

alohadb_inference_c_args = []
alohadb_inference_deps = []

if onnxruntime_dep.found()
  alohadb_inference_c_args += '-DHAVE_ONNXRUNTIME'
  alohadb_inference_deps += onnxruntime_dep
endif

if host_system == 'windows'
  alohadb_inference_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_inference',
    '--FILEDESC', 'alohadb_inference - in-database ML inference using ONNX Runtime',])
endif

alohadb_inference = shared_module('alohadb_inference',
  alohadb_inference_sources,
  c_args: alohadb_inference_c_args,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args + {
    'dependencies': alohadb_inference_deps + contrib_mod_args.get('dependencies', []),
  },
)
contrib_targets += alohadb_inference

install_data(
  'alohadb_inference.control',
  'alohadb_inference--1.0.sql',
  kwargs: contrib_data_args,
)
