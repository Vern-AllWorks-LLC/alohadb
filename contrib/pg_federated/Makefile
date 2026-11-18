# contrib/pg_federated/Makefile

MODULE_big = pg_federated
OBJS = \
	$(WIN32RES) \
	pg_federated.o \
	flower_client.o \
	local_train.o

EXTENSION = pg_federated
DATA = pg_federated--1.0.sql
PGFILEDESC = "pg_federated - federated learning with Flower framework"

PG_CPPFLAGS += $(LIBCURL_CPPFLAGS)
SHLIB_LINK += $(LIBCURL_LDFLAGS) $(LIBCURL_LDLIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_federated
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
