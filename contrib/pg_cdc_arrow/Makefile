# contrib/pg_cdc_arrow/Makefile

MODULE_big = pg_cdc_arrow
OBJS = pg_cdc_arrow.o arrow_builder.o arrow_ipc.o

EXTENSION = pg_cdc_arrow
DATA = pg_cdc_arrow--1.0.sql

PGFILEDESC = "pg_cdc_arrow - CDC output plugin emitting Arrow IPC format"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_cdc_arrow
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
