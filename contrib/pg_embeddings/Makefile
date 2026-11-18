# contrib/pg_embeddings/Makefile

EXTENSION = pg_embeddings
DATA = pg_embeddings--1.1.sql pg_embeddings--1.0--1.1.sql
PGFILEDESC = "pg_embeddings - embedding lifecycle management and hybrid retrieval"

REGRESS = pg_embeddings

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_embeddings
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
