/*-------------------------------------------------------------------------
 *
 * pq_common.h
 *	  Shared low-level helpers for the alohadb_parquet reader and writers:
 *	  Thrift compact-protocol encoding/decoding, Snappy block
 *	  compression/decompression, and Parquet format constants.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/pq_common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQ_COMMON_H
#define PQ_COMMON_H

#include "lib/stringinfo.h"

/* Parquet physical types (parquet.thrift enum Type) */
#define PQ_TYPE_BOOLEAN			0
#define PQ_TYPE_INT32			1
#define PQ_TYPE_INT64			2
#define PQ_TYPE_INT96			3
#define PQ_TYPE_FLOAT			4
#define PQ_TYPE_DOUBLE			5
#define PQ_TYPE_BYTE_ARRAY		6
#define PQ_TYPE_FIXED_LEN_BYTE_ARRAY 7

/* FieldRepetitionType */
#define PQ_REP_REQUIRED			0
#define PQ_REP_OPTIONAL			1
#define PQ_REP_REPEATED			2

/* ConvertedType (legacy annotations) */
#define PQ_CT_UTF8				0
#define PQ_CT_LIST				3
#define PQ_CT_DATE				6
#define PQ_CT_TIMESTAMP_MILLIS	9
#define PQ_CT_TIMESTAMP_MICROS	10
#define PQ_CT_INT_16			16
#define PQ_CT_INT_32			17
#define PQ_CT_INT_64			18
#define PQ_CT_JSON				19

/* LogicalType union member ids */
#define PQ_LT_STRING			1
#define PQ_LT_LIST				3
#define PQ_LT_DATE				6
#define PQ_LT_TIMESTAMP			8
#define PQ_LT_INTEGER			10
#define PQ_LT_JSON				12
#define PQ_LT_UUID				14

/* Encodings */
#define PQ_ENC_PLAIN			0
#define PQ_ENC_PLAIN_DICTIONARY	2
#define PQ_ENC_RLE				3
#define PQ_ENC_RLE_DICTIONARY	8

/* Compression codecs */
#define PQ_CODEC_UNCOMPRESSED	0
#define PQ_CODEC_SNAPPY			1

/* Page types */
#define PQ_PAGE_DATA			0
#define PQ_PAGE_INDEX			1
#define PQ_PAGE_DICTIONARY		2
#define PQ_PAGE_DATA_V2			3

/* Thrift compact protocol type ids */
#define TC_BOOL_TRUE			1
#define TC_BOOL_FALSE			2
#define TC_BYTE					3
#define TC_I16					4
#define TC_I32					5
#define TC_I64					6
#define TC_DOUBLE				7
#define TC_BINARY				8
#define TC_LIST					9
#define TC_SET					10
#define TC_MAP					11
#define TC_STRUCT				12

/* ----------------------------------------------------------------
 * Thrift compact decoder (bounds-checked; errors are ereport(ERROR))
 * ----------------------------------------------------------------
 */
typedef struct ThriftReader
{
	const uint8 *data;
	size_t		pos;
	size_t		len;
} ThriftReader;

extern uint64 tr_varint(ThriftReader *r);
extern int64 tr_zigzag(ThriftReader *r);
extern bool tr_field(ThriftReader *r, int *last_fid, int *fid, int *ftype);
extern void tr_list_header(ThriftReader *r, int *elemtype, int *count);
extern char *tr_binary(ThriftReader *r, int *len);
extern void tr_skip(ThriftReader *r, int ftype, int depth);
extern bool tr_bool(ThriftReader *r, int ftype);

/* ----------------------------------------------------------------
 * Thrift compact encoder
 * ----------------------------------------------------------------
 */
#define TW_MAX_DEPTH 16

typedef struct ThriftWriter
{
	StringInfo	buf;
	int			depth;
	int16		last_fid[TW_MAX_DEPTH];
} ThriftWriter;

extern void tw_init(ThriftWriter *w, StringInfo buf);
extern void tw_field_i32(ThriftWriter *w, int fid, int32 v);
extern void tw_field_i64(ThriftWriter *w, int fid, int64 v);
extern void tw_field_bool(ThriftWriter *w, int fid, bool v);
extern void tw_field_byte(ThriftWriter *w, int fid, int8 v);
extern void tw_field_binary(ThriftWriter *w, int fid, const char *data, int len);
extern void tw_field_struct_begin(ThriftWriter *w, int fid);
extern void tw_field_list_begin(ThriftWriter *w, int fid, int elemtype, int count);
extern void tw_list_struct_begin(ThriftWriter *w);
extern void tw_list_i32(ThriftWriter *w, int32 v);
extern void tw_list_binary(ThriftWriter *w, const char *data, int len);
extern void tw_struct_end(ThriftWriter *w);

/* ----------------------------------------------------------------
 * Snappy raw block format
 * ----------------------------------------------------------------
 */
extern bool snappy_uncompressed_length(const uint8 *src, size_t src_len,
									   uint32 *result);
extern bool snappy_decompress(const uint8 *src, size_t src_len,
							  uint8 *dst, size_t dst_len);
extern void snappy_compress(const uint8 *src, size_t src_len, StringInfo dst);

#endif							/* PQ_COMMON_H */
