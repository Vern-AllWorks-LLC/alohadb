# contrib/pg_tsfunc/Makefile

MODULE_big = pg_tsfunc
OBJS = \
	$(WIN32RES) \
	pg_tsfunc.o \
	tscodec.o

EXTENSION = pg_tsfunc
DATA = pg_tsfunc--1.0.sql
PGFILEDESC = "pg_tsfunc - scientific time-series functions"

REGRESS = tsfunc_lttb tsfunc_resample tsfunc_asof tsfunc_codec

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tsfunc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
