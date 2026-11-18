# contrib/pg_graph/Makefile

MODULE_big = pg_graph
OBJS = \
	$(WIN32RES) \
	pg_graph.o \
	graph_algo.o

EXTENSION = pg_graph
DATA = pg_graph--1.0.sql
PGFILEDESC = "pg_graph - property graphs over ordinary tables"

REGRESS = graph_schema graph_traversal graph_algorithms graph_gnn

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_graph
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
