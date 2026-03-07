# contrib/alohadb_inference/Makefile

MODULE_big = alohadb_inference
OBJS = \
	$(WIN32RES) \
	alohadb_inference.o \
	model_cache.o

EXTENSION = alohadb_inference
DATA = alohadb_inference--1.0.sql
PGFILEDESC = "alohadb_inference - in-database ML inference using ONNX Runtime"

# Detect ONNX Runtime availability.
# Look for the header in standard locations and in ONNXRUNTIME_HOME.
ONNXRUNTIME_HOME ?= /usr/local
ONNXRUNTIME_INCDIR = $(ONNXRUNTIME_HOME)/include
ONNXRUNTIME_LIBDIR = $(ONNXRUNTIME_HOME)/lib

# Check if onnxruntime_c_api.h is available
ifneq (,$(wildcard $(ONNXRUNTIME_INCDIR)/onnxruntime_c_api.h))
HAVE_ONNXRUNTIME = yes
else ifneq (,$(wildcard /usr/include/onnxruntime_c_api.h))
HAVE_ONNXRUNTIME = yes
ONNXRUNTIME_INCDIR = /usr/include
ONNXRUNTIME_LIBDIR = /usr/lib
endif

ifeq ($(HAVE_ONNXRUNTIME),yes)
PG_CPPFLAGS += -DHAVE_ONNXRUNTIME -I$(ONNXRUNTIME_INCDIR)
SHLIB_LINK += -L$(ONNXRUNTIME_LIBDIR) -lonnxruntime
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_inference
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
