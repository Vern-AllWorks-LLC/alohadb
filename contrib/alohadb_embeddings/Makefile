# contrib/alohadb_embeddings/Makefile

EXTENSION = alohadb_embeddings
DATA = alohadb_embeddings--1.0.sql
PGFILEDESC = "alohadb_embeddings - embedding lifecycle management and hybrid retrieval"

REGRESS = alohadb_embeddings

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_embeddings
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
