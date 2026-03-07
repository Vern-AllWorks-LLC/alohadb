# contrib/alohadb_learned_stats/Makefile

MODULE_big = alohadb_learned_stats
OBJS = \
	$(WIN32RES) \
	learned_stats.o \
	model.o

EXTENSION = alohadb_learned_stats
DATA = learned_stats--1.0.sql
PGFILEDESC = "alohadb_learned_stats - ML-assisted cardinality estimation"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_learned_stats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
