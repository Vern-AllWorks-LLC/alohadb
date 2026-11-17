#
# Makefile for the hnsw index access method (ported from pgvector)
#
# src/backend/access/hnsw/Makefile
#

subdir = src/backend/access/hnsw
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	hnsw.o \
	hnswbuild.o \
	hnswinsert.o \
	hnswscan.o \
	hnswutils.o \
	hnswvacuum.o

include $(top_srcdir)/src/backend/common.mk
