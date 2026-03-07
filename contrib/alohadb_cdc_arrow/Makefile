# contrib/alohadb_cdc_arrow/Makefile

MODULE_big = alohadb_cdc_arrow
OBJS = alohadb_cdc_arrow.o arrow_builder.o arrow_ipc.o

EXTENSION = alohadb_cdc_arrow
DATA = alohadb_cdc_arrow--1.0.sql

PGFILEDESC = "alohadb_cdc_arrow - CDC output plugin emitting Arrow IPC format"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_cdc_arrow
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
