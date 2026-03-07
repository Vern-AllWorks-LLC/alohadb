#
# Makefile for the HNSW index access method
#
# src/backend/access/hnsw/Makefile
#

subdir = src/backend/access/hnsw
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	hnswbuild.o \
	hnswcost.o \
	hnswinsert.o \
	hnswoptions.o \
	hnsw_scan.o \
	hnswutils.o \
	hnswvacuum.o \
	hnswvalidate.o

include $(top_srcdir)/src/backend/common.mk
