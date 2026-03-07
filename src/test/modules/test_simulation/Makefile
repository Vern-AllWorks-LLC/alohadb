# src/test/modules/test_simulation/Makefile

MODULE_big = test_simulation
OBJS = \
	test_simulation.o \
	sim_prng.o \
	sim_fault.o

EXTENSION = test_simulation
DATA = test_simulation--1.0.sql
PGFILEDESC = "test_simulation - deterministic simulation testing"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_simulation
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
