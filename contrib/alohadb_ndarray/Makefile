# contrib/alohadb_ndarray/Makefile

MODULE_big = alohadb_ndarray
OBJS = \
	$(WIN32RES) \
	alohadb_ndarray.o \
	ndarray_tensor.o

EXTENSION = alohadb_ndarray
DATA = alohadb_ndarray--1.0.sql
PGFILEDESC = "alohadb_ndarray - chunked, compressed, sliceable N-d arrays"

REGRESS = alohadb_ndarray

SHLIB_LINK += -lzstd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_ndarray
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
