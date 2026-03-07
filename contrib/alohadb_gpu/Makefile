# contrib/alohadb_gpu/Makefile

MODULE_big = alohadb_gpu

# ---------------------------------------------------------------
# Detect CUDA (nvcc).  When available, compile gpu_vector_ops.cu
# and link with the CUDA runtime.  Otherwise fall back to the
# CPU-only implementation in gpu_cpu_fallback.c.
# ---------------------------------------------------------------
NVCC := $(shell which nvcc 2>/dev/null)

OBJS = \
	$(WIN32RES) \
	alohadb_gpu.o

ifdef NVCC
# CUDA is available — compile the .cu file with nvcc
OBJS += gpu_vector_ops_cuda.o
PG_CPPFLAGS += -DHAVE_CUDA

# Find CUDA include/lib paths from nvcc location
CUDA_DIR := $(shell dirname $(shell dirname $(NVCC)))
PG_CPPFLAGS += -I$(CUDA_DIR)/include
SHLIB_LINK += -L$(CUDA_DIR)/lib64 -lcudart

# Rule to compile .cu files with nvcc.
# We produce a relocatable device code object that the host linker
# can consume.
gpu_vector_ops_cuda.o: gpu_vector_ops.cu gpu_vector_ops.h
	$(NVCC) -c -o $@ $< --compiler-options '-fPIC' \
		-I$(CUDA_DIR)/include
else
# No CUDA — use CPU fallback
OBJS += gpu_cpu_fallback.o
endif

EXTENSION = alohadb_gpu
DATA = alohadb_gpu--1.0.sql
PGFILEDESC = "alohadb_gpu - GPU-offloaded vector operations"

HEADERS = gpu_vector_ops.h

SHLIB_LINK += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_gpu
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
