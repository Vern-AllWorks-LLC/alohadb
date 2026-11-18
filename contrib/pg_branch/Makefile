# contrib/pg_branch/Makefile

MODULE_big = pg_branch
OBJS = \
	$(WIN32RES) \
	pg_branch.o

EXTENSION = pg_branch
DATA = pg_branch--1.0.sql
PGFILEDESC = "pg_branch - lightweight database branching"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_branch
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
