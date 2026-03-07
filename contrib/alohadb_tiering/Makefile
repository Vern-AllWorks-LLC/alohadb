# contrib/alohadb_tiering/Makefile

MODULE_big = alohadb_tiering
OBJS = \
	$(WIN32RES) \
	alohadb_tiering.o

EXTENSION = alohadb_tiering
DATA = alohadb_tiering--1.0.sql
PGFILEDESC = "alohadb_tiering - automatic storage tiering for partitioned tables"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_tiering
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
