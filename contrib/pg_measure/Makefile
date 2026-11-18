# contrib/pg_measure/Makefile

MODULE_big = pg_measure
OBJS = \
	$(WIN32RES) \
	pg_measure.o

EXTENSION = pg_measure
DATA = pg_measure--1.0.sql
PGFILEDESC = "pg_measure - measurements with units, uncertainty and provenance"

REGRESS = measure_type measure_math measure_prov

SHLIB_LINK += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_measure
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
