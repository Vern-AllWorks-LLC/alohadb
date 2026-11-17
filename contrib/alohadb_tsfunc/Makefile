# contrib/alohadb_tsfunc/Makefile

MODULE_big = alohadb_tsfunc
OBJS = \
	$(WIN32RES) \
	alohadb_tsfunc.o \
	tscodec.o

EXTENSION = alohadb_tsfunc
DATA = alohadb_tsfunc--1.0.sql
PGFILEDESC = "alohadb_tsfunc - scientific time-series functions"

REGRESS = tsfunc_lttb tsfunc_resample tsfunc_asof tsfunc_codec

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_tsfunc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
