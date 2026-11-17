# contrib/alohadb_parquet/Makefile

MODULE_big = alohadb_parquet
OBJS = \
	$(WIN32RES) \
	alohadb_parquet.o \
	arrow_writer.o \
	csv_reader.o \
	export_common.o \
	json_reader.o \
	parquet_full.o \
	parquet_reader.o \
	parquet_writer.o \
	pq_common.o

EXTENSION = alohadb_parquet
DATA = alohadb_parquet--1.0.sql \
	alohadb_parquet--1.0--1.1.sql \
	alohadb_parquet--1.1--1.2.sql
PGFILEDESC = "alohadb_parquet - Parquet, CSV, and JSON reader; Parquet and Arrow writer"

REGRESS = parquet_v11 parquet_write

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alohadb_parquet
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
