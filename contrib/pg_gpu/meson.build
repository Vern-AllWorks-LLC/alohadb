# Copyright (c) 2025, AlohaDB

# ---------------------------------------------------------------
# Detect CUDA via nvcc.  When available, compile the CUDA kernels;
# otherwise link the CPU fallback.
# ---------------------------------------------------------------
nvcc = find_program('nvcc', required: false)

pg_gpu_sources = files(
  'pg_gpu.c',
)

pg_gpu_c_args = []
pg_gpu_link_args = []
pg_gpu_deps = []

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

  pg_gpu_sources += gpu_cuda_obj
  pg_gpu_c_args += ['-DHAVE_CUDA']

  # Link against CUDA runtime
  cudart_dep = dependency('cudart', required: false)
  if cudart_dep.found()
    pg_gpu_deps += cudart_dep
  else
    # Manual fallback: find libcudart via nvcc path
    nvcc_path = nvcc.full_path()
    cuda_base = run_command('dirname',
      run_command('dirname', nvcc_path, check: true).stdout().strip(),
      check: true,
    ).stdout().strip()
    pg_gpu_link_args += [
      '-L' + cuda_base / 'lib64',
      '-lcudart',
    ]
  endif
else
  pg_gpu_sources += files('gpu_cpu_fallback.c')
endif

if host_system == 'windows'
  pg_gpu_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_gpu',
    '--FILEDESC', 'pg_gpu - GPU-offloaded vector operations',])
endif

pg_gpu_mod_args = contrib_mod_args
if pg_gpu_deps.length() > 0
  pg_gpu_mod_args += {
    'dependencies': contrib_mod_args.get('dependencies', []) + pg_gpu_deps,
  }
endif

pg_gpu = shared_module('pg_gpu',
  pg_gpu_sources,
  c_args: pg_gpu_c_args,
  link_args: pg_gpu_link_args,
  include_directories: include_directories('.'),
  kwargs: pg_gpu_mod_args,
)
contrib_targets += pg_gpu

install_data(
  'pg_gpu.control',
  'pg_gpu--1.0.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'gpu_vector_ops.h',
  install_dir: dir_include_extension / 'pg_gpu',
)
