# contrib/pg_nl2sql/Makefile

MODULE_big = pg_nl2sql
OBJS = \
	$(WIN32RES) \
	pg_nl2sql.o \
	schema_context.o

EXTENSION = pg_nl2sql
DATA = pg_nl2sql--1.0.sql
PGFILEDESC = "pg_nl2sql - natural language to SQL translation via LLM"

PG_CPPFLAGS += $(LIBCURL_CPPFLAGS)
SHLIB_LINK += $(LIBCURL_LDFLAGS) $(LIBCURL_LDLIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_nl2sql
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
