# contrib/alohadb_approx/Makefile

MODULE_big = alohadb_approx
OBJS = \
	$(WIN32RES) \
	alohadb_approx.o \
	hll_agg.o \
	cms.o \
	topk.o

EXTENSION = alohadb_approx
DATA = alohadb_approx--1.0.sql
PGFILEDESC = "alohadb_approx - approximate query processing: HLL, CMS, Top-K"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_approx
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
