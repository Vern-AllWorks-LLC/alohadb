# contrib/pg_lineage/Makefile

EXTENSION = pg_lineage
DATA = pg_lineage--1.0.sql
PGFILEDESC = "pg_lineage - dataset snapshots, model lineage and zero-copy branches"

REGRESS = lineage lineage_bitemporal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_lineage
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
