#
# Makefile for the ivfflat index access method (ported from pgvector)
#
# src/backend/access/ivfflat/Makefile
#

subdir = src/backend/access/ivfflat
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	ivfbuild.o \
	ivfflat.o \
	ivfinsert.o \
	ivfkmeans.o \
	ivfscan.o \
	ivfutils.o \
	ivfvacuum.o

include $(top_srcdir)/src/backend/common.mk
