# contrib/pg_parquetio/Makefile

MODULE_big = pg_parquetio
OBJS = \
	$(WIN32RES) \
	pg_parquetio.o \
	arrow_writer.o \
	csv_reader.o \
	export_common.o \
	json_reader.o \
	parquet_full.o \
	parquet_reader.o \
	parquet_writer.o \
	pq_common.o

EXTENSION = pg_parquetio
DATA = pg_parquetio--1.0.sql \
	pg_parquetio--1.0--1.1.sql \
	pg_parquetio--1.1--1.2.sql
PGFILEDESC = "pg_parquetio - Parquet, CSV, and JSON reader; Parquet and Arrow writer"

REGRESS = parquet_v11 parquet_write

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_parquetio
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
