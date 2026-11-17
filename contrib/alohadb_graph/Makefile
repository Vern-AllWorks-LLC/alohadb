# contrib/alohadb_graph/Makefile

MODULE_big = alohadb_graph
OBJS = \
	$(WIN32RES) \
	alohadb_graph.o \
	graph_algo.o

EXTENSION = alohadb_graph
DATA = alohadb_graph--1.0.sql
PGFILEDESC = "alohadb_graph - property graphs over ordinary tables"

REGRESS = graph_schema graph_traversal graph_algorithms graph_gnn

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_graph
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
