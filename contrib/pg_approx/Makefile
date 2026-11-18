# contrib/pg_approx/Makefile

MODULE_big = pg_approx
OBJS = \
	$(WIN32RES) \
	pg_approx.o \
	hll_agg.o \
	cms.o \
	topk.o

EXTENSION = pg_approx
DATA = pg_approx--1.0.sql
PGFILEDESC = "pg_approx - approximate query processing: HLL, CMS, Top-K"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_approx
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
