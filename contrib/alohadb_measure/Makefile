# contrib/alohadb_measure/Makefile

MODULE_big = alohadb_measure
OBJS = \
	$(WIN32RES) \
	alohadb_measure.o

EXTENSION = alohadb_measure
DATA = alohadb_measure--1.0.sql
PGFILEDESC = "alohadb_measure - measurements with units, uncertainty and provenance"

REGRESS = measure_type measure_math measure_prov

SHLIB_LINK += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_measure
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
