/*-------------------------------------------------------------------------
 *
 * parquet_full.c
 *	  Full Parquet column reader for alohadb_parquet (version 1.1+).
 *
 *	  Implements read_parquet_full() — a set-returning function that
 *	  reads a Parquet file and yields one jsonb row per data row.
 *
 *	  Supported encodings:  PLAIN, PLAIN_DICTIONARY (RLE dictionary)
 *	  Supported compression: UNCOMPRESSED, SNAPPY
 *	  Supported column types: INT32, INT64, FLOAT, DOUBLE,
 *	                          BYTE_ARRAY (text), BOOLEAN
 *	  Limitations: flat (non-nested, non-repeated) schemas only.
 *
 *	  Parquet format spec: Apache 2.0 (no patent issues).
 *	  Thrift compact protocol: Apache 2.0.
 *	  Snappy decompression: open-source implementation below
 *	    (Snappy format by Google; BSD-licensed reference; the
 *	    decompression algorithm itself is prior art — LZ77, 1977).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/parquet_full.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "alohadb_parquet.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>

PG_FUNCTION_INFO_V1(read_parquet_full);

/* ================================================================
 * Minimal Thrift compact-protocol decoder
 *
 * Parquet footer metadata is encoded in Thrift compact binary
 * protocol.  We implement just enough to read FileMetaData:
 *   - varint / zigzag decoding
 *   - field header parsing (field delta + type)
 *   - string / struct / list fields
 *
 * Reference: Apache Thrift spec (Apache 2.0).
 * ================================================================ */

typedef struct ThriftBuf
{
	const uint8_t *data;
	size_t		   pos;
	size_t		   len;
} ThriftBuf;

static uint64_t
thrift_read_varint(ThriftBuf *b)
{
	uint64_t result = 0;
	int		 shift  = 0;
	uint8_t	 byte;

	while (b->pos < b->len)
	{
		byte = b->data[b->pos++];
		result |= (uint64_t)(byte & 0x7f) << shift;
		if ((byte & 0x80) == 0)
			break;
		shift += 7;
	}
	return result;
}

static int64_t
thrift_read_zigzag(ThriftBuf *b)
{
	uint64_t n = thrift_read_varint(b);
	return (int64_t)((n >> 1) ^ -(int64_t)(n & 1));
}

/* Return false at end-of-struct (type 0), true otherwise.
 * *field_id and *type_id are set on true. */
static bool
thrift_next_field(ThriftBuf *b, int *prev_field_id,
				  int *field_id, int *type_id)
{
	uint8_t	header;

	if (b->pos >= b->len)
		return false;

	header = b->data[b->pos++];
	if (header == 0)
		return false;	/* stop field */

	*type_id = header & 0x0f;

	if ((header & 0xf0) != 0)
	{
		/* Short form: delta in high nibble */
		*field_id = *prev_field_id + ((header >> 4) & 0x0f);
	}
	else
	{
		/* Long form: zigzag i16 follows */
		*field_id = (int) thrift_read_zigzag(b);
	}
	*prev_field_id = *field_id;
	return true;
}

/* Skip over any Thrift value (recursive, handles nested structs/lists) */
static void
thrift_skip(ThriftBuf *b, int type_id)
{
	int		fid = 0, ft, n, i;
	uint8_t	elem_type;

	switch (type_id)
	{
		case 1:		/* boolean true  (1 byte already consumed via header) */
		case 2:		/* boolean false */
			break;
		case 3:		/* i8 */
			b->pos++;
			break;
		case 4:		/* i16 (zigzag varint) */
		case 5:		/* i32 */
		case 6:		/* i64 */
			thrift_read_varint(b);
			break;
		case 7:		/* double */
			b->pos += 8;
			break;
		case 8:		/* binary/string */
			{
				int len = (int) thrift_read_varint(b);
				b->pos += len;
			}
			break;
		case 12:	/* struct */
			while (thrift_next_field(b, &fid, &fid, &ft))
				thrift_skip(b, ft);
			break;
		case 15:	/* list */
			if (b->pos >= b->len) break;
			elem_type = b->data[b->pos] & 0x0f;
			n = (b->data[b->pos] >> 4) & 0x0f;
			b->pos++;
			if (n == 15)
				n = (int) thrift_read_varint(b);
			for (i = 0; i < n; i++)
				thrift_skip(b, elem_type);
			break;
		default:
			/* Unknown type — we can't safely continue */
			b->pos = b->len;
			break;
	}
}

/* Read a binary/string field; returns a palloc'd buffer */
static char *
thrift_read_string(ThriftBuf *b, int *out_len)
{
	int		len = (int) thrift_read_varint(b);
	char   *s;

	if (len < 0 || b->pos + (size_t) len > b->len)
		return NULL;

	s = palloc(len + 1);
	memcpy(s, b->data + b->pos, len);
	s[len] = '\0';
	b->pos += len;
	*out_len = len;
	return s;
}

/* ================================================================
 * Parquet schema and column metadata
 * ================================================================ */

#define PARQUET_TYPE_BOOLEAN   0
#define PARQUET_TYPE_INT32     1
#define PARQUET_TYPE_INT64     2
#define PARQUET_TYPE_FLOAT     4
#define PARQUET_TYPE_DOUBLE    5
#define PARQUET_TYPE_BYTEARRAY 6

#define PARQUET_ENCODING_PLAIN            0
#define PARQUET_ENCODING_PLAIN_DICTIONARY 2
#define PARQUET_ENCODING_RLE_DICTIONARY   8

#define PARQUET_COMPRESS_UNCOMPRESSED  0
#define PARQUET_COMPRESS_SNAPPY        1

typedef struct ColumnMeta
{
	char   *name;		/* column name */
	int		type;		/* PARQUET_TYPE_* */
	int		codec;		/* PARQUET_COMPRESS_* */
	int64_t file_offset;
	int64_t total_compressed_size;
	int64_t total_uncompressed_size;
	int64_t num_values;
} ColumnMeta;

typedef struct RowGroup
{
	ColumnMeta *columns;
	int			ncols;
	int64_t		num_rows;
} RowGroup;

typedef struct ParquetFile
{
	RowGroup   *row_groups;
	int			nrow_groups;
	char	  **col_names;
	int		   *col_types;
	int			nschema_cols;	/* number of leaf schema elements */
	int64_t		num_rows_total;
} ParquetFile;

/* ================================================================
 * Minimal Snappy decompressor
 *
 * Snappy is an LZ77 variant (LZ77: Ziv & Lempel 1977, public domain).
 * The format is documented and open; we implement stream decompression
 * only for the block format used by Parquet column chunks.
 * ================================================================ */

static size_t
snappy_read_varint32(const uint8_t *src, size_t pos, size_t len,
					 uint32_t *out)
{
	uint32_t result = 0;
	int		 shift  = 0;

	while (pos < len)
	{
		uint8_t b = src[pos++];
		result |= (uint32_t)(b & 0x7f) << shift;
		if ((b & 0x80) == 0)
			break;
		shift += 7;
	}
	*out = result;
	return pos;
}

/*
 * Decompress a Snappy-compressed block.
 * Returns uncompressed size, or 0 on error.
 * Caller must supply dst buffer of at least uncompressed_size bytes.
 */
static size_t
snappy_decompress(const uint8_t *src, size_t src_len,
				  uint8_t *dst, size_t dst_max)
{
	uint32_t	uncompressed_len;
	size_t		ip = 0;		/* input position */
	size_t		op = 0;		/* output position */

	/* Read uncompressed length */
	ip = snappy_read_varint32(src, ip, src_len, &uncompressed_len);
	if (uncompressed_len > dst_max)
		return 0;

	while (ip < src_len)
	{
		uint8_t tag = src[ip++];
		int		tag_type = tag & 3;

		if (tag_type == 0)
		{
			/* Literal */
			uint32_t	lit_len;
			int			nbytes = (tag >> 2) & 0x3f;

			if (nbytes < 60)
			{
				lit_len = (uint32_t) nbytes + 1;
			}
			else
			{
				int		xtra = nbytes - 59;
				uint32_t v = 0;
				int		i;

				for (i = 0; i < xtra; i++)
					v |= ((uint32_t) src[ip++]) << (i * 8);
				lit_len = v + 1;
			}

			if (ip + lit_len > src_len || op + lit_len > dst_max)
				return 0;
			memcpy(dst + op, src + ip, lit_len);
			ip += lit_len;
			op += lit_len;
		}
		else if (tag_type == 1)
		{
			/* Copy 1 (1-byte offset) */
			uint32_t	copy_len    = 4 + ((tag >> 2) & 0x7);
			uint32_t	copy_offset = ((tag >> 5) & 0x7) |
									((uint32_t) src[ip++] << 3);
			size_t		j;

			if (op < copy_offset || op + copy_len > dst_max)
				return 0;
			for (j = 0; j < copy_len; j++)
				dst[op + j] = dst[op - copy_offset + j];
			op += copy_len;
		}
		else if (tag_type == 2)
		{
			/* Copy 2 (2-byte offset) */
			uint32_t	copy_len    = 1 + ((tag >> 2) & 0x3f);
			uint32_t	copy_offset = (uint32_t) src[ip] |
									((uint32_t) src[ip+1] << 8);
			size_t		j;

			ip += 2;
			if (op < copy_offset || op + copy_len > dst_max)
				return 0;
			for (j = 0; j < copy_len; j++)
				dst[op + j] = dst[op - copy_offset + j];
			op += copy_len;
		}
		else
		{
			/* Copy 4 (4-byte offset) */
			uint32_t	copy_len    = 1 + ((tag >> 2) & 0x3f);
			uint32_t	copy_offset = (uint32_t) src[ip] |
									((uint32_t) src[ip+1] << 8) |
									((uint32_t) src[ip+2] << 16) |
									((uint32_t) src[ip+3] << 24);
			size_t		j;

			ip += 4;
			if (op < copy_offset || op + copy_len > dst_max)
				return 0;
			for (j = 0; j < copy_len; j++)
				dst[op + j] = dst[op - copy_offset + j];
			op += copy_len;
		}
	}

	return op;
}

/* ================================================================
 * Thrift footer parsing
 *
 * We parse just enough of FileMetaData to get:
 *   - schema element names and types
 *   - row group locations (file_offset, compressed/uncompressed size,
 *     codec) and row counts
 * ================================================================ */

static void
parse_schema_element(ThriftBuf *b, char **out_name, int *out_type,
					 bool *out_is_leaf)
{
	int prev_fid = 0, fid, ft;
	int num_children = -1;

	*out_name    = NULL;
	*out_type    = -1;
	*out_is_leaf = false;

	while (thrift_next_field(b, &prev_fid, &fid, &ft))
	{
		switch (fid)
		{
			case 1:	/* type (optional — absent for group nodes) */
				*out_type = (int) thrift_read_zigzag(b);
				break;
			case 2:	/* type_length */
				thrift_read_varint(b);
				break;
			case 3:	/* repetition_type */
				thrift_read_varint(b);
				break;
			case 4:	/* name */
				{
					int len;
					*out_name = thrift_read_string(b, &len);
				}
				break;
			case 5:	/* num_children (present for group nodes) */
				num_children = (int) thrift_read_zigzag(b);
				break;
			default:
				thrift_skip(b, ft);
				break;
		}
	}

	*out_is_leaf = (num_children < 0);
}

static void
parse_column_metadata(ThriftBuf *b, ColumnMeta *col)
{
	int prev_fid = 0, fid, ft;

	col->type                   = PARQUET_TYPE_BYTEARRAY;
	col->codec                  = PARQUET_COMPRESS_UNCOMPRESSED;
	col->file_offset            = 0;
	col->total_compressed_size  = 0;
	col->total_uncompressed_size = 0;
	col->num_values             = 0;

	while (thrift_next_field(b, &prev_fid, &fid, &ft))
	{
		switch (fid)
		{
			case 1:	/* type */
				col->type = (int) thrift_read_zigzag(b);
				break;
			case 3:	/* codec */
				col->codec = (int) thrift_read_zigzag(b);
				break;
			case 5:	/* num_values */
				col->num_values = (int64_t) thrift_read_zigzag(b);
				break;
			case 6:	/* total_uncompressed_size */
				col->total_uncompressed_size = (int64_t) thrift_read_zigzag(b);
				break;
			case 7:	/* total_compressed_size */
				col->total_compressed_size = (int64_t) thrift_read_zigzag(b);
				break;
			case 9:	/* data_page_offset */
				col->file_offset = (int64_t) thrift_read_zigzag(b);
				break;
			default:
				thrift_skip(b, ft);
				break;
		}
	}
}

static void
parse_column_chunk(ThriftBuf *b, ColumnMeta *col)
{
	int prev_fid = 0, fid, ft;

	while (thrift_next_field(b, &prev_fid, &fid, &ft))
	{
		switch (fid)
		{
			case 2:	/* meta_data (ColumnMetaData struct) */
				parse_column_metadata(b, col);
				break;
			case 4:	/* file_offset (for dictionary page) */
				col->file_offset = (int64_t) thrift_read_zigzag(b);
				break;
			default:
				thrift_skip(b, ft);
				break;
		}
	}
}

/*
 * Parse the Thrift-encoded Parquet footer (FileMetaData).
 * Returns a palloc'd ParquetFile structure.
 */
static ParquetFile *
parse_parquet_footer(const uint8_t *footer_data, int footer_len)
{
	ThriftBuf	b;
	ParquetFile *pf;
	int			prev_fid = 0, fid, ft;

	/* Schema element storage (pre-allocated, fixed max) */
	char	   *schema_names[4096];
	int			schema_types[4096];
	int			nschema = 0;

	/* Row group storage */
	RowGroup	rgs[4096];
	int			nrgs = 0;

	b.data = footer_data;
	b.pos  = 0;
	b.len  = (size_t) footer_len;

	pf = palloc0(sizeof(ParquetFile));

	while (thrift_next_field(&b, &prev_fid, &fid, &ft))
	{
		switch (fid)
		{
			case 2:	/* schema list */
			{
				/* List header: elem_type + count */
				int		nelem, elem_type, i;

				if (b.pos >= b.len) break;
				elem_type = b.data[b.pos] & 0x0f;
				nelem     = (b.data[b.pos] >> 4) & 0x0f;
				b.pos++;
				if (nelem == 15)
					nelem = (int) thrift_read_varint(&b);
				(void) elem_type;

				for (i = 0; i < nelem && nschema < 4096; i++)
				{
					char   *name;
					int		type;
					bool	is_leaf;

					parse_schema_element(&b, &name, &type, &is_leaf);

					if (is_leaf && name)
					{
						schema_names[nschema] = name;
						schema_types[nschema] = type;
						nschema++;
					}
				}
				break;
			}

			case 3:	/* num_rows */
				pf->num_rows_total = (int64_t) thrift_read_zigzag(&b);
				break;

			case 4:	/* row_groups list */
			{
				int		nrg_elem, elem_type2, i;

				if (b.pos >= b.len) break;
				elem_type2 = b.data[b.pos] & 0x0f;
				nrg_elem   = (b.data[b.pos] >> 4) & 0x0f;
				b.pos++;
				if (nrg_elem == 15)
					nrg_elem = (int) thrift_read_varint(&b);
				(void) elem_type2;

				for (i = 0; i < nrg_elem && nrgs < 4096; i++)
				{
					/* Each row group is a struct */
					int		rg_prev = 0, rg_fid, rg_ft;

					rgs[nrgs].num_rows = 0;

					/* Columns: list of ColumnChunk structs */
					ColumnMeta *cols = palloc0(sizeof(ColumnMeta) * nschema);
					int			ncol_parsed = 0;

					while (thrift_next_field(&b, &rg_prev, &rg_fid, &rg_ft))
					{
						switch (rg_fid)
						{
							case 1:	/* columns */
							{
								int		ncol_elem, col_etype, j;

								if (b.pos >= b.len) break;
								col_etype = b.data[b.pos] & 0x0f;
								ncol_elem = (b.data[b.pos] >> 4) & 0x0f;
								b.pos++;
								if (ncol_elem == 15)
									ncol_elem = (int) thrift_read_varint(&b);
								(void) col_etype;

								for (j = 0; j < ncol_elem && ncol_parsed < nschema; j++)
								{
									parse_column_chunk(&b, &cols[ncol_parsed]);
									ncol_parsed++;
								}
								break;
							}
							case 3:	/* num_rows */
								rgs[nrgs].num_rows = (int64_t) thrift_read_zigzag(&b);
								break;
							default:
								thrift_skip(&b, rg_ft);
								break;
						}
					}

					rgs[nrgs].columns = cols;
					rgs[nrgs].ncols   = ncol_parsed;
					nrgs++;
				}
				break;
			}

			default:
				thrift_skip(&b, ft);
				break;
		}
	}

	/* Fill pf */
	pf->nschema_cols = nschema;
	pf->col_names    = palloc(sizeof(char *) * nschema);
	pf->col_types    = palloc(sizeof(int)    * nschema);
	memcpy(pf->col_names, schema_names, nschema * sizeof(char *));
	memcpy(pf->col_types, schema_types, nschema * sizeof(int));

	pf->nrow_groups  = nrgs;
	pf->row_groups   = palloc(sizeof(RowGroup) * nrgs);
	memcpy(pf->row_groups, rgs, nrgs * sizeof(RowGroup));

	return pf;
}

/* ================================================================
 * Page decoder
 *
 * After decompression each column chunk contains one or more pages.
 * Each page has a 4-byte (little-endian) length header followed by
 * a Thrift-encoded PageHeader struct, then the page data.
 *
 * We handle:
 *   DICTIONARY_PAGE: build a string/int/float dictionary
 *   DATA_PAGE / DATA_PAGE_V2: read values
 * ================================================================ */

#define PAGE_TYPE_DATA_PAGE        0
#define PAGE_TYPE_INDEX_PAGE       1
#define PAGE_TYPE_DICTIONARY_PAGE  2
#define PAGE_TYPE_DATA_PAGE_V2     3

typedef struct ParquetPageHeader
{
	int		type;
	int32_t uncompressed_page_size;
	int32_t compressed_page_size;
	int32_t num_values;
	int		encoding;
	int		def_level_encoding;
	int		rep_level_encoding;
	int		dict_page_encoding;
} ParquetPageHeader;

static ParquetPageHeader
parse_page_header(ThriftBuf *b)
{
	ParquetPageHeader	h;
	int			prev_fid = 0, fid, ft;

	memset(&h, 0, sizeof(h));

	while (thrift_next_field(b, &prev_fid, &fid, &ft))
	{
		switch (fid)
		{
			case 1:	/* type */
				h.type = (int) thrift_read_zigzag(b);
				break;
			case 2:	/* uncompressed_page_size */
				h.uncompressed_page_size = (int32_t) thrift_read_zigzag(b);
				break;
			case 3:	/* compressed_page_size */
				h.compressed_page_size = (int32_t) thrift_read_zigzag(b);
				break;
			case 5:	/* data_page_header */
				/* Sub-struct: num_values, encoding, def/rep level encodings */
				{
					int sp = 0, sf, sft;
					while (thrift_next_field(b, &sp, &sf, &sft))
					{
						switch (sf)
						{
							case 1: h.num_values = (int32_t) thrift_read_zigzag(b); break;
							case 2: h.def_level_encoding = (int) thrift_read_zigzag(b); break;
							case 3: h.rep_level_encoding = (int) thrift_read_zigzag(b); break;
							case 4: h.encoding = (int) thrift_read_zigzag(b); break;
							default: thrift_skip(b, sft); break;
						}
					}
				}
				break;
			case 7:	/* dictionary_page_header */
				{
					int sp = 0, sf, sft;
					while (thrift_next_field(b, &sp, &sf, &sft))
					{
						switch (sf)
						{
							case 1: h.num_values = (int32_t) thrift_read_zigzag(b); break;
							case 2: h.dict_page_encoding = (int) thrift_read_zigzag(b); break;
							default: thrift_skip(b, sft); break;
						}
					}
				}
				break;
			case 8:	/* data_page_header_v2 */
				{
					int sp = 0, sf, sft;
					while (thrift_next_field(b, &sp, &sf, &sft))
					{
						switch (sf)
						{
							case 1: h.num_values = (int32_t) thrift_read_zigzag(b); break;
							case 5: h.encoding = (int) thrift_read_zigzag(b); break;
							default: thrift_skip(b, sft); break;
						}
					}
					h.type = PAGE_TYPE_DATA_PAGE_V2;
				}
				break;
			default:
				thrift_skip(b, ft);
				break;
		}
	}
	return h;
}

/* ================================================================
 * RLE/bit-packing decoder (for dictionary indices)
 *
 * Parquet uses a hybrid RLE/bit-packing scheme for dictionary page
 * indices.  The format:
 *   - 4-byte LE bit-width header
 *   - then runs: each run prefixed by a varint indicator
 *       if indicator & 1 == 1 -> bit-packed run, length = indicator >> 1
 *       if indicator & 1 == 0 -> RLE run, length = indicator >> 1
 *         followed by the repeated value in ceil(bit_width/8) bytes
 * ================================================================ */

static uint32_t
rle_read_uint(const uint8_t *data, size_t *pos, int bit_width, int idx)
{
	/* Read bit_width bits starting at bit offset idx*bit_width */
	size_t	byte_offset = (*pos) + ((size_t) idx * bit_width) / 8;
	int		bit_offset  = ((size_t) idx * bit_width) % 8;
	uint32_t val = 0;
	int		bits_left = bit_width;
	int		out_shift = 0;

	while (bits_left > 0)
	{
		int		available = 8 - bit_offset;
		int		take = available < bits_left ? available : bits_left;
		uint8_t mask = (take == 8) ? 0xff : ((1 << take) - 1);
		uint32_t chunk = (data[byte_offset] >> bit_offset) & mask;

		val |= chunk << out_shift;
		out_shift += take;
		bits_left -= take;
		bit_offset = 0;
		byte_offset++;
	}
	return val;
}

/* Decode n dictionary indices from data, starting at *pos (byte aligned).
 * Returns a palloc'd array of int32. */
static int32_t *
decode_rle_hybrid(const uint8_t *data, size_t data_len,
				  int bit_width, int n_values, size_t *consumed)
{
	int32_t	   *out = palloc0(sizeof(int32_t) * n_values);
	size_t		pos = 0;
	int			out_idx = 0;

	while (out_idx < n_values && pos < data_len)
	{
		uint64_t	indicator;
		size_t		tmp = pos;

		/* Read varint indicator */
		indicator = 0;
		{
			int		shift = 0;
			uint8_t b;
			do {
				if (tmp >= data_len) goto rle_done;
				b = data[tmp++];
				indicator |= (uint64_t)(b & 0x7f) << shift;
				shift += 7;
			} while (b & 0x80);
		}
		pos = tmp;

		if (indicator & 1)
		{
			/* Bit-packed run: (indicator >> 1) groups of 8 values */
			int		groups = (int)(indicator >> 1);
			int		i;

			for (i = 0; i < groups * 8 && out_idx < n_values; i++)
			{
				out[out_idx++] = (int32_t) rle_read_uint(data, &pos,
														  bit_width, i);
			}
			pos += ((size_t) groups * 8 * bit_width + 7) / 8;
		}
		else
		{
			/* RLE run: (indicator >> 1) repetitions of a fixed value */
			int		run_len = (int)(indicator >> 1);
			int		val_bytes = (bit_width + 7) / 8;
			uint32_t val = 0;
			int		i;

			for (i = 0; i < val_bytes && pos < data_len; i++)
				val |= ((uint32_t) data[pos++]) << (i * 8);

			for (i = 0; i < run_len && out_idx < n_values; i++)
				out[out_idx++] = (int32_t) val;
		}
	}

rle_done:
	*consumed = pos;
	return out;
}

/* ================================================================
 * Column chunk reader
 *
 * Read all pages of one column chunk and return an array of
 * Datum values (one per row in the row group).
 * NULLs are indicated by a parallel bool array.
 * ================================================================ */

typedef struct ColumnData
{
	Datum  *values;
	bool   *nulls;
	int64_t n;
} ColumnData;

static ColumnData
read_column_chunk(FILE *fp, ColumnMeta *col,
				  int parquet_type, int64_t nrows)
{
	ColumnData	cd;
	uint8_t	   *chunk_buf;
	uint8_t	   *uncomp_buf = NULL;
	size_t		chunk_pos;
	int64_t		rows_read = 0;

	/* Dictionary (for PLAIN_DICTIONARY encoding) */
	Datum	   *dict         = NULL;
	int			dict_size    = 0;
	bool		have_dict    = false;

	cd.values = palloc0(sizeof(Datum) * nrows);
	cd.nulls  = palloc0(sizeof(bool)  * nrows);
	cd.n      = nrows;

	/* Read compressed chunk into memory */
	chunk_buf = palloc(col->total_compressed_size);
	fseek(fp, col->file_offset, SEEK_SET);
	if (fread(chunk_buf, 1, col->total_compressed_size, fp) !=
		(size_t) col->total_compressed_size)
	{
		/* Read failure — return all NULLs */
		for (int i = 0; i < nrows; i++) cd.nulls[i] = true;
		pfree(chunk_buf);
		return cd;
	}

	/* Decompress if needed */
	if (col->codec == PARQUET_COMPRESS_SNAPPY)
	{
		uncomp_buf = palloc(col->total_uncompressed_size);
		size_t actual = snappy_decompress(
			chunk_buf, col->total_compressed_size,
			uncomp_buf, col->total_uncompressed_size);

		if (actual == 0 || actual > (size_t) col->total_uncompressed_size)
		{
			/* Decompression failed — return NULLs */
			for (int i = 0; i < nrows; i++) cd.nulls[i] = true;
			pfree(chunk_buf);
			pfree(uncomp_buf);
			return cd;
		}
		pfree(chunk_buf);
		chunk_buf = uncomp_buf;
		chunk_pos = 0;
	}
	else
		chunk_pos = 0;

	/* Process pages */
	while (rows_read < nrows && chunk_pos < (size_t) col->total_uncompressed_size)
	{
		ThriftBuf	pb;
		ParquetPageHeader	ph;
		const uint8_t *page_data;

		pb.data = chunk_buf + chunk_pos;
		pb.pos  = 0;
		pb.len  = (size_t)(col->total_uncompressed_size) - chunk_pos;

		ph = parse_page_header(&pb);
		chunk_pos += pb.pos;

		page_data = chunk_buf + chunk_pos;

		if (ph.type == PAGE_TYPE_DICTIONARY_PAGE)
		{
			/* Build dictionary: nvalues entries of the column type */
			dict      = palloc0(sizeof(Datum) * ph.num_values);
			dict_size = ph.num_values;
			have_dict = true;

			for (int di = 0; di < ph.num_values; di++)
			{
				switch (parquet_type)
				{
					case PARQUET_TYPE_INT32:
					{
						int32_t v;
						memcpy(&v, page_data + di * 4, 4);
						dict[di] = Int32GetDatum(v);
						break;
					}
					case PARQUET_TYPE_INT64:
					{
						int64_t v;
						memcpy(&v, page_data + di * 8, 8);
						dict[di] = Int64GetDatum(v);
						break;
					}
					case PARQUET_TYPE_FLOAT:
					{
						float v;
						memcpy(&v, page_data + di * 4, 4);
						dict[di] = Float4GetDatum(v);
						break;
					}
					case PARQUET_TYPE_DOUBLE:
					{
						double v;
						memcpy(&v, page_data + di * 8, 8);
						dict[di] = Float8GetDatum(v);
						break;
					}
					case PARQUET_TYPE_BYTEARRAY:
					{
						/* prefix 4-byte LE length then bytes */
						uint32_t slen;
						const uint8_t *sp = page_data;

						for (int j = 0; j < di; j++)
						{
							memcpy(&slen, sp, 4);
							sp += 4 + slen;
						}
						memcpy(&slen, sp, 4);
						sp += 4;
						dict[di] = PointerGetDatum(
							cstring_to_text_with_len((const char *) sp, slen));
						break;
					}
					default:
						dict[di] = CStringGetTextDatum("");
						break;
				}
			}
		}
		else if (ph.type == PAGE_TYPE_DATA_PAGE ||
				 ph.type == PAGE_TYPE_DATA_PAGE_V2)
		{
			int		nvals = ph.num_values;
			int		enc   = ph.encoding;

			if (enc == PARQUET_ENCODING_PLAIN_DICTIONARY ||
				enc == PARQUET_ENCODING_RLE_DICTIONARY)
			{
				/* Dictionary indices encoded as RLE/bit-packed */
				int		bit_width = (have_dict && dict_size > 0)
										? (int) ceil(log2((double)(dict_size + 1)))
										: 8;
				size_t	consumed;
				int32_t *indices;

				if (bit_width == 0) bit_width = 1;

				/* First byte of page data is the bit width */
				int page_bit_width = (int) page_data[0];
				(void) bit_width;

				indices = decode_rle_hybrid(page_data + 1,
											ph.compressed_page_size - 1,
											page_bit_width, nvals, &consumed);

				for (int vi = 0; vi < nvals && rows_read < nrows; vi++, rows_read++)
				{
					int idx = indices[vi];

					if (have_dict && idx >= 0 && idx < dict_size)
					{
						cd.values[rows_read] = dict[idx];
						cd.nulls[rows_read]  = false;
					}
					else
						cd.nulls[rows_read] = true;
				}
			}
			else
			{
				/* PLAIN encoding */
				const uint8_t *p = page_data;

				for (int vi = 0; vi < nvals && rows_read < nrows; vi++, rows_read++)
				{
					cd.nulls[rows_read] = false;

					switch (parquet_type)
					{
						case PARQUET_TYPE_BOOLEAN:
						{
							int byte_pos = vi / 8;
							int bit_pos  = vi % 8;
							bool v = (page_data[byte_pos] >> bit_pos) & 1;
							cd.values[rows_read] = BoolGetDatum(v);
							break;
						}
						case PARQUET_TYPE_INT32:
						{
							int32_t v;
							memcpy(&v, p, 4); p += 4;
							cd.values[rows_read] = Int32GetDatum(v);
							break;
						}
						case PARQUET_TYPE_INT64:
						{
							int64_t v;
							memcpy(&v, p, 8); p += 8;
							cd.values[rows_read] = Int64GetDatum(v);
							break;
						}
						case PARQUET_TYPE_FLOAT:
						{
							float v;
							memcpy(&v, p, 4); p += 4;
							cd.values[rows_read] = Float4GetDatum(v);
							break;
						}
						case PARQUET_TYPE_DOUBLE:
						{
							double v;
							memcpy(&v, p, 8); p += 8;
							cd.values[rows_read] = Float8GetDatum(v);
							break;
						}
						case PARQUET_TYPE_BYTEARRAY:
						{
							uint32_t slen;
							memcpy(&slen, p, 4); p += 4;
							cd.values[rows_read] = PointerGetDatum(
								cstring_to_text_with_len((const char *) p, slen));
							p += slen;
							break;
						}
						default:
							cd.nulls[rows_read] = true;
							break;
					}
				}
			}
		}

		chunk_pos += ph.compressed_page_size;
	}

	/* Mark any remaining rows as NULL */
	for (; rows_read < nrows; rows_read++)
		cd.nulls[rows_read] = true;

	pfree(chunk_buf);
	return cd;
}

/* ================================================================
 * read_parquet_full — SRF entry point
 *
 * Uses the multi-call SRF API.  On the first call we:
 *   1. Validate and open the file
 *   2. Parse the footer
 *   3. Read all column chunks for the first row group
 *   4. Emit rows one by one
 *
 * On subsequent calls we advance row_idx and, when a row group is
 * exhausted, read the next one.
 * ================================================================ */

typedef struct ParquetReaderState
{
	ParquetFile	   *pf;
	FILE		   *fp;

	/* Current row group */
	int				rg_idx;
	int64_t			row_idx;	/* row within current row group */
	int64_t			rg_nrows;

	/* Column data for current row group */
	ColumnData	   *col_data;

	/* jsonb output options (from options jsonb) */
	bool			columns_as_text; /* cast everything to text */
} ParquetReaderState;

Datum
read_parquet_full(PG_FUNCTION_ARGS)
{
	FuncCallContext		   *funcctx;
	ParquetReaderState	   *state;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcxt;
		char		   *filepath;
		long			file_size;
		uint8_t			footer_len_buf[4];
		int32_t			footer_len;
		uint8_t		   *footer_buf;
		char			tail_magic[4];
		ParquetFile	   *pf;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt  = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		filepath = text_to_cstring(PG_GETARG_TEXT_PP(0));
		/* options jsonb (arg 1) is reserved for future use */

		if (!parquet_validate_path(filepath))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("file path \"%s\" is not in an allowed directory",
							filepath)));

		state = palloc0(sizeof(ParquetReaderState));
		state->fp = fopen(filepath, "rb");

		if (!state->fp)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", filepath)));

		/* Validate magic */
		{
			char magic[4];
			if (fread(magic, 1, 4, state->fp) != 4 ||
				memcmp(magic, "PAR1", 4) != 0)
			{
				fclose(state->fp);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("\"%s\" is not a valid Parquet file", filepath)));
			}
		}

		fseek(state->fp, 0, SEEK_END);
		file_size = ftell(state->fp);

		/* Read footer length (4 bytes before trailing PAR1) */
		fseek(state->fp, file_size - 8, SEEK_SET);
		if (fread(footer_len_buf, 1, 4, state->fp) != 4)
		{
			fclose(state->fp);
			ereport(ERROR, (errmsg("could not read Parquet footer length")));
		}

		footer_len = (int32_t)((uint32_t) footer_len_buf[0] |
							   ((uint32_t) footer_len_buf[1] << 8) |
							   ((uint32_t) footer_len_buf[2] << 16) |
							   ((uint32_t) footer_len_buf[3] << 24));

		/* Validate trailing magic */
		fseek(state->fp, file_size - 4, SEEK_SET);
		if (fread(tail_magic, 1, 4, state->fp) != 4 ||
			memcmp(tail_magic, "PAR1", 4) != 0)
		{
			fclose(state->fp);
			ereport(ERROR, (errmsg("corrupted Parquet footer in \"%s\"", filepath)));
		}

		if (footer_len <= 0 || footer_len > file_size - 12)
		{
			fclose(state->fp);
			ereport(ERROR, (errmsg("invalid Parquet footer length %d", footer_len)));
		}

		/* Read and parse footer */
		footer_buf = palloc(footer_len);
		fseek(state->fp, file_size - 8 - footer_len, SEEK_SET);
		if (fread(footer_buf, 1, footer_len, state->fp) != (size_t) footer_len)
		{
			fclose(state->fp);
			ereport(ERROR, (errmsg("could not read Parquet footer")));
		}

		pf = parse_parquet_footer(footer_buf, footer_len);
		pfree(footer_buf);

		if (pf->num_rows_total == 0 || pf->nrow_groups == 0)
		{
			fclose(state->fp);
			MemoryContextSwitchTo(oldcxt);
			funcctx->max_calls = 0;
			SRF_RETURN_DONE(funcctx);
		}

		state->pf      = pf;
		state->rg_idx  = 0;
		state->row_idx = 0;

		/* Read first row group columns */
		{
			RowGroup *rg = &pf->row_groups[0];
			int		  c;

			state->rg_nrows  = rg->num_rows;
			state->col_data  = palloc(sizeof(ColumnData) * pf->nschema_cols);

			for (c = 0; c < pf->nschema_cols && c < rg->ncols; c++)
			{
				int ptype = (c < pf->nschema_cols) ? pf->col_types[c]
												   : PARQUET_TYPE_BYTEARRAY;
				state->col_data[c] = read_column_chunk(
					state->fp, &rg->columns[c], ptype, rg->num_rows);
			}
		}

		funcctx->user_fctx = state;
		funcctx->max_calls = (uint64) pf->num_rows_total;

		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	state   = (ParquetReaderState *) funcctx->user_fctx;

	/* Check if we have more rows */
	if (funcctx->call_cntr >= funcctx->max_calls)
	{
		if (state->fp)
			fclose(state->fp);
		SRF_RETURN_DONE(funcctx);
	}

	/* Advance to next row group if needed */
	if (state->row_idx >= state->rg_nrows)
	{
		state->rg_idx++;

		if (state->rg_idx >= state->pf->nrow_groups)
		{
			if (state->fp)
				fclose(state->fp);
			SRF_RETURN_DONE(funcctx);
		}

		RowGroup *rg = &state->pf->row_groups[state->rg_idx];
		int		  c;

		state->rg_nrows  = rg->num_rows;
		state->row_idx   = 0;
		state->col_data  = palloc(sizeof(ColumnData) * state->pf->nschema_cols);

		for (c = 0; c < state->pf->nschema_cols && c < rg->ncols; c++)
		{
			int ptype = state->pf->col_types[c];
			state->col_data[c] = read_column_chunk(
				state->fp, &rg->columns[c], ptype, rg->num_rows);
		}
	}

	/* Build jsonb object for current row */
	{
		StringInfoData	jbuf;
		Datum			result;
		int				c;
		int64_t			ri = state->row_idx;
		ParquetFile	   *pf = state->pf;

		initStringInfo(&jbuf);
		appendStringInfoChar(&jbuf, '{');

		for (c = 0; c < pf->nschema_cols; c++)
		{
			char		   *colname = pf->col_names[c];
			ColumnData	   *cd     = &state->col_data[c];
			bool			is_null = (ri >= cd->n) || cd->nulls[ri];

			if (c > 0)
				appendStringInfoChar(&jbuf, ',');

			appendStringInfoChar(&jbuf, '"');
			appendStringInfoString(&jbuf, colname);
			appendStringInfoString(&jbuf, "\":");

			if (is_null)
			{
				appendStringInfoString(&jbuf, "null");
			}
			else
			{
				switch (pf->col_types[c])
				{
					case PARQUET_TYPE_BOOLEAN:
						appendStringInfoString(&jbuf,
							DatumGetBool(cd->values[ri]) ? "true" : "false");
						break;
					case PARQUET_TYPE_INT32:
						appendStringInfo(&jbuf, "%d",
										 DatumGetInt32(cd->values[ri]));
						break;
					case PARQUET_TYPE_INT64:
						appendStringInfo(&jbuf, INT64_FORMAT,
										 DatumGetInt64(cd->values[ri]));
						break;
					case PARQUET_TYPE_FLOAT:
						appendStringInfo(&jbuf, "%g",
										 (double) DatumGetFloat4(cd->values[ri]));
						break;
					case PARQUET_TYPE_DOUBLE:
						appendStringInfo(&jbuf, "%g",
										 DatumGetFloat8(cd->values[ri]));
						break;
					case PARQUET_TYPE_BYTEARRAY:
					default:
					{
						text   *t   = DatumGetTextP(cd->values[ri]);
						int		len = VARSIZE_ANY_EXHDR(t);
						char   *sp  = VARDATA_ANY(t);
						int		i;

						appendStringInfoChar(&jbuf, '"');
						for (i = 0; i < len; i++)
						{
							unsigned char ch = (unsigned char) sp[i];
							if (ch == '"')
								appendStringInfoString(&jbuf, "\\\"");
							else if (ch == '\\')
								appendStringInfoString(&jbuf, "\\\\");
							else if (ch < 0x20)
								appendStringInfo(&jbuf, "\\u%04x", ch);
							else
								appendStringInfoCharMacro(&jbuf, (char) ch);
						}
						appendStringInfoChar(&jbuf, '"');
						break;
					}
				}
			}
		}

		appendStringInfoChar(&jbuf, '}');

		result = DirectFunctionCall1(jsonb_in, CStringGetDatum(jbuf.data));
		pfree(jbuf.data);

		state->row_idx++;

		SRF_RETURN_NEXT(funcctx, result);
	}
}
