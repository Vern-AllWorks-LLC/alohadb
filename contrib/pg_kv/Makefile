# contrib/pg_kv/Makefile

EXTENSION = pg_kv
DATA = pg_kv--1.0.sql
PGFILEDESC = "pg_kv - key-value API with TTL on an unlogged table"

REGRESS = pg_kv

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_kv
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
