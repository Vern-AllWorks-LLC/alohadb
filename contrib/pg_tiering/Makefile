# contrib/pg_tiering/Makefile

MODULE_big = pg_tiering
OBJS = \
	$(WIN32RES) \
	pg_tiering.o

EXTENSION = pg_tiering
DATA = pg_tiering--1.0.sql
PGFILEDESC = "pg_tiering - automatic storage tiering for partitioned tables"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tiering
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
