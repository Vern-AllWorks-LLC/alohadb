# contrib/pg_ndarray/Makefile

MODULE_big = pg_ndarray
OBJS = \
	$(WIN32RES) \
	pg_ndarray.o \
	ndarray_tensor.o

EXTENSION = pg_ndarray
DATA = pg_ndarray--1.0.sql
PGFILEDESC = "pg_ndarray - chunked, compressed, sliceable N-d arrays"

REGRESS = pg_ndarray

SHLIB_LINK += -lzstd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_ndarray
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
