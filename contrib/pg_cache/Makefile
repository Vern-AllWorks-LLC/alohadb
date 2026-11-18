# contrib/pg_cache/Makefile

MODULE_big = pg_cache
OBJS = \
	$(WIN32RES) \
	pg_cache.o \
	cache_store.o

EXTENSION = pg_cache
DATA = pg_cache--2.0.sql pg_cache--1.0--2.0.sql
PGFILEDESC = "pg_cache - transactional shared-memory key-value cache"

REGRESS = pg_cache
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/pg_cache/pg_cache.conf
# Disabled because these tests require "shared_preload_libraries=pg_cache",
# which typical installcheck users do not have.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_cache
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
