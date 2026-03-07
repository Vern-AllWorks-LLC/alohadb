# Copyright (c) 2025, AlohaDB

# ---------------------------------------------------------------
# Detect CUDA via nvcc.  When available, compile the CUDA kernels;
# otherwise link the CPU fallback.
# ---------------------------------------------------------------
nvcc = find_program('nvcc', required: false)

alohadb_gpu_sources = files(
  'alohadb_gpu.c',
)

alohadb_gpu_c_args = []
alohadb_gpu_link_args = []
alohadb_gpu_deps = []

if nvcc.found()
  # Compile gpu_vector_ops.cu with nvcc into a static object
  cuda_dir = run_command(
    nvcc, '--print-libpath',
    check: false,
  )

  gpu_cuda_obj = custom_target('gpu_vector_ops_cuda',
    input: 'gpu_vector_ops.cu',
    output: 'gpu_vector_ops_cuda.o',
    command: [
      nvcc, '-c', '-o', '@OUTPUT@', '@INPUT@',
      '--compiler-options', '-fPIC',
    ],
  )

  alohadb_gpu_sources += gpu_cuda_obj
  alohadb_gpu_c_args += ['-DHAVE_CUDA']

  # Link against CUDA runtime
  cudart_dep = dependency('cudart', required: false)
  if cudart_dep.found()
    alohadb_gpu_deps += cudart_dep
  else
    # Manual fallback: find libcudart via nvcc path
    nvcc_path = nvcc.full_path()
    cuda_base = run_command('dirname',
      run_command('dirname', nvcc_path, check: true).stdout().strip(),
      check: true,
    ).stdout().strip()
    alohadb_gpu_link_args += [
      '-L' + cuda_base / 'lib64',
      '-lcudart',
    ]
  endif
else
  alohadb_gpu_sources += files('gpu_cpu_fallback.c')
endif

if host_system == 'windows'
  alohadb_gpu_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_gpu',
    '--FILEDESC', 'alohadb_gpu - GPU-offloaded vector operations',])
endif

alohadb_gpu_mod_args = contrib_mod_args
if alohadb_gpu_deps.length() > 0
  alohadb_gpu_mod_args += {
    'dependencies': contrib_mod_args.get('dependencies', []) + alohadb_gpu_deps,
  }
endif

alohadb_gpu = shared_module('alohadb_gpu',
  alohadb_gpu_sources,
  c_args: alohadb_gpu_c_args,
  link_args: alohadb_gpu_link_args,
  include_directories: include_directories('.'),
  kwargs: alohadb_gpu_mod_args,
)
contrib_targets += alohadb_gpu

install_data(
  'alohadb_gpu.control',
  'alohadb_gpu--1.0.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'gpu_vector_ops.h',
  install_dir: dir_include_extension / 'alohadb_gpu',
)
