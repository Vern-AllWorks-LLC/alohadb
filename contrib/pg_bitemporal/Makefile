# contrib/pg_bitemporal/Makefile

EXTENSION = pg_bitemporal
DATA = pg_bitemporal--1.0.sql
PGFILEDESC = "pg_bitemporal - SQL:2011-style bitemporal tables"

REGRESS = bitemporal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bitemporal
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
