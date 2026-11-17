# contrib/alohadb_bitemporal/Makefile

EXTENSION = alohadb_bitemporal
DATA = alohadb_bitemporal--1.0.sql
PGFILEDESC = "alohadb_bitemporal - SQL:2011-style bitemporal tables"

REGRESS = bitemporal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_bitemporal
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
