# contrib/pg_temporal/Makefile

MODULE_big = pg_temporal
OBJS = \
	$(WIN32RES) \
	pg_temporal.o

EXTENSION = pg_temporal
DATA = pg_temporal--1.0.sql pg_temporal--1.0--1.1.sql
PGFILEDESC = "pg_temporal - SQL:2011-style system-versioned temporal tables"

REGRESS = temporal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_temporal
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
