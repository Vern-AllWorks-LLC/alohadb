# contrib/alohadb_temporal/Makefile

MODULE_big = alohadb_temporal
OBJS = \
	$(WIN32RES) \
	alohadb_temporal.o

EXTENSION = alohadb_temporal
DATA = alohadb_temporal--1.0.sql alohadb_temporal--1.0--1.1.sql
PGFILEDESC = "alohadb_temporal - SQL:2011-style system-versioned temporal tables"

REGRESS = temporal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_temporal
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
