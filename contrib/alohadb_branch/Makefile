# contrib/alohadb_branch/Makefile

MODULE_big = alohadb_branch
OBJS = \
	$(WIN32RES) \
	alohadb_branch.o

EXTENSION = alohadb_branch
DATA = alohadb_branch--1.0.sql
PGFILEDESC = "alohadb_branch - lightweight database branching"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_branch
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
