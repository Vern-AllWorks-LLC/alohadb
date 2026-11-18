/*-------------------------------------------------------------------------
 *
 * parquet_full.c
 *	  Parquet reader for pg_parquetio (version 1.1+).
 *
 *	  Implements read_parquet_full(), the C function behind
 *	  read_parquet(file_path text, options jsonb) RETURNS SETOF jsonb.
 *	  Every row of the file becomes one jsonb object keyed by column name.
 *
 *	  Supported:
 *	    - data pages v1 and v2, dictionary pages
 *	    - encodings PLAIN, PLAIN_DICTIONARY / RLE_DICTIONARY, RLE (booleans)
 *	    - compression UNCOMPRESSED and SNAPPY
 *	    - physical types BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE,
 *	      BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
 *	    - optional/required columns (definition levels), nested groups
 *	      (reported as "a.b"), and single-level lists (one repetition
 *	      level, 2- or 3-level encoding), returned as JSON arrays
 *	    - logical types: STRING/UTF8, JSON (embedded as JSON), DATE and
 *	      TIMESTAMP (ISO 8601 strings, "+00:00" when adjusted to UTC),
 *	      UUID, signed/unsigned INTEGER; un-annotated BYTE_ARRAY and other
 *	      FIXED_LEN_BYTE_ARRAY values are returned as "\x..." hex strings
 *	      (bytea text format); NaN/Infinity as JSON strings
 *	  Not supported (clear error): other codecs (GZIP, ZSTD, LZ4, ...),
 *	  DELTA_* and BYTE_STREAM_SPLIT encodings, multiply-nested lists, maps.
 *
 *	  The result is materialized into a tuplestore one row group at a time,
 *	  so memory use is bounded by a single row group.
 *
 *	  Prior art / patent notes: Apache Parquet and Apache Thrift are open
 *	  specifications under the Apache License 2.0 (express patent grant
 *	  from contributors); Snappy is an openly documented LZ77-family
 *	  format (see pq_common.c).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_parquetio/parquet_full.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>
#include <stdio.h>

#include "catalog/pg_type.h"
#include "common/int.h"
#include "common/shortest_dec.h"
#include "datatype/timestamp.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "pg_parquetio.h"
#include "pq_common.h"

PG_FUNCTION_INFO_V1(read_parquet_full);

#define PG_TO_UNIX_DAYS		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define PG_TO_UNIX_USECS	((int64) PG_TO_UNIX_DAYS * USECS_PER_DAY)

/* ================================================================
 * Metadata structures
 * ================================================================ */

typedef struct PqSchemaNode
{
	char	   *name;
	int			type;			/* physical type, -1 for groups */
	int			type_length;
	int			repetition;
	int			num_children;
	int			converted;		/* ConvertedType or -1 */
	int			logical;		/* LogicalType member id or 0 */
	bool		ts_utc;
	int			ts_unit;		/* 1 millis, 2 micros, 3 nanos */
	int			int_bits;
	bool		int_signed;
} PqSchemaNode;

typedef struct PqLeaf
{
	char	   *name;			/* output key */
	PqSchemaNode *node;
	int			max_def;
	int			max_rep;
	int			list_def;		/* definition level of the repeated node */
} PqLeaf;

typedef struct PqChunk
{
	int			type;
	int			codec;
	int64		start;
	int64		size;
} PqChunk;

typedef struct PqRowGroup
{
	int64		num_rows;
	PqChunk    *cols;
	int			ncols;
} PqRowGroup;

typedef struct PqFileMeta
{
	PqSchemaNode *nodes;
	int			nnodes;
	PqLeaf	   *leaves;
	int			nleaves;
	PqRowGroup *rgs;
	int			nrgs;
	int64		num_rows;
} PqFileMeta;

static void
pq_corrupt(const char *what)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("corrupt Parquet file: %s", what)));
}

/* ================================================================
 * Footer parsing
 * ================================================================ */

static void
parse_logical_type(ThriftReader *r, PqSchemaNode *n)
{
	int			last = 0,
				fid,
				ft;

	while (tr_field(r, &last, &fid, &ft))
	{
		if (ft != TC_STRUCT)
		{
			tr_skip(r, ft, 0);
			continue;
		}
		n->logical = fid;
		if (fid == PQ_LT_TIMESTAMP)
		{
			int			l2 = 0,
						f2,
						t2;

			while (tr_field(r, &l2, &f2, &t2))
			{
				if (f2 == 1)
					n->ts_utc = tr_bool(r, t2);
				else if (f2 == 2 && t2 == TC_STRUCT)
				{
					int			l3 = 0,
								f3,
								t3;

					while (tr_field(r, &l3, &f3, &t3))
					{
						n->ts_unit = f3;
						tr_skip(r, t3, 0);
					}
				}
				else
					tr_skip(r, t2, 0);
			}
		}
		else if (fid == PQ_LT_INTEGER)
		{
			int			l2 = 0,
						f2,
						t2;

			while (tr_field(r, &l2, &f2, &t2))
			{
				if (f2 == 1 && t2 == TC_BYTE)
				{
					if (r->pos >= r->len)
						pq_corrupt("truncated integer type");
					n->int_bits = (int8) r->data[r->pos++];
				}
				else if (f2 == 2)
					n->int_signed = tr_bool(r, t2);
				else
					tr_skip(r, t2, 0);
			}
		}
		else
			tr_skip(r, ft, 0);
	}
}

static void
parse_schema_element(ThriftReader *r, PqSchemaNode *n)
{
	int			last = 0,
				fid,
				ft;

	memset(n, 0, sizeof(*n));
	n->type = -1;
	n->converted = -1;
	n->int_signed = true;

	while (tr_field(r, &last, &fid, &ft))
	{
		switch (fid)
		{
			case 1:
				n->type = (int) tr_zigzag(r);
				break;
			case 2:
				n->type_length = (int) tr_zigzag(r);
				break;
			case 3:
				n->repetition = (int) tr_zigzag(r);
				break;
			case 4:
				n->name = tr_binary(r, NULL);
				break;
			case 5:
				n->num_children = (int) tr_zigzag(r);
				break;
			case 6:
				n->converted = (int) tr_zigzag(r);
				break;
			case 10:
				if (ft == TC_STRUCT)
					parse_logical_type(r, n);
				else
					tr_skip(r, ft, 0);
				break;
			default:
				tr_skip(r, ft, 0);
				break;
		}
	}
	if (n->name == NULL)
		n->name = pstrdup("");
}

static void
parse_column_chunk(ThriftReader *r, PqChunk *c)
{
	int			last = 0,
				fid,
				ft;
	int64		data_off = -1;
	int64		dict_off = -1;

	c->codec = PQ_CODEC_UNCOMPRESSED;
	c->size = 0;
	c->type = -1;

	while (tr_field(r, &last, &fid, &ft))
	{
		if (fid == 3 && ft == TC_STRUCT)
		{
			int			l2 = 0,
						f2,
						t2;

			while (tr_field(r, &l2, &f2, &t2))
			{
				switch (f2)
				{
					case 1:
						c->type = (int) tr_zigzag(r);
						break;
					case 4:
						c->codec = (int) tr_zigzag(r);
						break;
					case 7:
						c->size = tr_zigzag(r);
						break;
					case 9:
						data_off = tr_zigzag(r);
						break;
					case 11:
						dict_off = tr_zigzag(r);
						break;
					default:
						tr_skip(r, t2, 0);
						break;
				}
			}
		}
		else if (fid == 1)
		{
			(void) tr_binary(r, NULL);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Parquet files with column chunks in external files are not supported")));
		}
		else
			tr_skip(r, ft, 0);
	}
	if (data_off < 0)
		pq_corrupt("column chunk without metadata");
	c->start = (dict_off > 0 && dict_off < data_off) ? dict_off : data_off;
}

/* Walk the flattened schema tree to compute leaf columns and levels */
static void
walk_schema(PqFileMeta *fm, int *idx, int def, int rep, int list_def,
			const char *path, const char *top, PqLeaf *out, int *nout)
{
	PqSchemaNode *n;
	char	   *mypath;
	int			i;

	if (*idx >= fm->nnodes)
		pq_corrupt("schema tree is truncated");
	n = &fm->nodes[(*idx)++];

	if (n->repetition == PQ_REP_OPTIONAL)
		def++;
	else if (n->repetition == PQ_REP_REPEATED)
	{
		def++;
		rep++;
		list_def = def;
	}
	mypath = path ? psprintf("%s.%s", path, n->name) : pstrdup(n->name);
	if (top == NULL)
		top = n->name;

	if (n->num_children <= 0)
	{
		PqLeaf	   *l;

		if (n->type < 0)
			pq_corrupt("schema leaf without a type");
		if (*nout >= fm->nnodes)
			pq_corrupt("too many leaves");
		if (rep > 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("nested Parquet column \"%s\" with more than one repetition level is not supported",
							mypath)));
		l = &out[(*nout)++];
		l->name = (rep > 0) ? pstrdup(top) : mypath;
		l->node = n;
		l->max_def = def;
		l->max_rep = rep;
		l->list_def = list_def;
		return;
	}
	for (i = 0; i < n->num_children; i++)
		walk_schema(fm, idx, def, rep, list_def, mypath, top, out, nout);
}

static PqFileMeta *
parse_footer(const uint8 *data, int len)
{
	ThriftReader r;
	PqFileMeta *fm = palloc0(sizeof(PqFileMeta));
	int			last = 0,
				fid,
				ft;
	bool		have_schema = false;

	r.data = data;
	r.pos = 0;
	r.len = len;

	while (tr_field(&r, &last, &fid, &ft))
	{
		if (fid == 2 && ft == TC_LIST)
		{
			int			et,
						n,
						i;

			tr_list_header(&r, &et, &n);
			if (et != TC_STRUCT || n < 1)
				pq_corrupt("bad schema list");
			fm->nodes = palloc0(sizeof(PqSchemaNode) * n);
			fm->nnodes = n;
			for (i = 0; i < n; i++)
				parse_schema_element(&r, &fm->nodes[i]);
			have_schema = true;
		}
		else if (fid == 3)
			fm->num_rows = tr_zigzag(&r);
		else if (fid == 4 && ft == TC_LIST)
		{
			int			et,
						n,
						i;

			tr_list_header(&r, &et, &n);
			if (et != TC_STRUCT)
				pq_corrupt("bad row group list");
			fm->rgs = palloc0(sizeof(PqRowGroup) * Max(n, 1));
			fm->nrgs = n;
			for (i = 0; i < n; i++)
			{
				PqRowGroup *rg = &fm->rgs[i];
				int			l2 = 0,
							f2,
							t2;

				while (tr_field(&r, &l2, &f2, &t2))
				{
					if (f2 == 1 && t2 == TC_LIST)
					{
						int			e3,
									n3,
									j;

						tr_list_header(&r, &e3, &n3);
						if (e3 != TC_STRUCT)
							pq_corrupt("bad column chunk list");
						rg->cols = palloc0(sizeof(PqChunk) * Max(n3, 1));
						rg->ncols = n3;
						for (j = 0; j < n3; j++)
							parse_column_chunk(&r, &rg->cols[j]);
					}
					else if (f2 == 3)
						rg->num_rows = tr_zigzag(&r);
					else
						tr_skip(&r, t2, 0);
				}
			}
		}
		else
			tr_skip(&r, ft, 0);
	}

	if (!have_schema)
		pq_corrupt("missing schema");

	/* leaves */
	{
		int			idx = 1;
		int			i;
		PqSchemaNode *root = &fm->nodes[0];

		fm->leaves = palloc0(sizeof(PqLeaf) * fm->nnodes);
		for (i = 0; i < root->num_children; i++)
			walk_schema(fm, &idx, 0, 0, 0, NULL, NULL, fm->leaves, &fm->nleaves);
	}

	{
		int			i;

		for (i = 0; i < fm->nrgs; i++)
		{
			int			j;

			if (fm->rgs[i].ncols != fm->nleaves)
				pq_corrupt("row group column count does not match schema");
			for (j = 0; j < fm->rgs[i].ncols; j++)
			{
				int			codec = fm->rgs[i].cols[j].codec;

				if (codec != PQ_CODEC_UNCOMPRESSED && codec != PQ_CODEC_SNAPPY)
				{
					static const char *const names[] = {
						"UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI",
						"LZ4", "ZSTD", "LZ4_RAW"
					};

					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("Parquet compression codec %s is not supported",
									(codec >= 0 && codec < lengthof(names)) ? names[codec] : "unknown"),
							 errhint("Only UNCOMPRESSED and SNAPPY column chunks can be read.")));
				}
			}
		}
	}
	return fm;
}

/* ================================================================
 * Page parsing and decoding
 * ================================================================ */

typedef struct PqPageHeader
{
	int			type;
	int32		usize;
	int32		csize;
	int32		num_values;
	int			encoding;
	/* v2 */
	int32		def_len;
	int32		rep_len;
	bool		is_compressed;
} PqPageHeader;

static void
parse_page_header(ThriftReader *r, PqPageHeader *h)
{
	int			last = 0,
				fid,
				ft;

	memset(h, 0, sizeof(*h));
	h->type = -1;
	h->is_compressed = true;

	while (tr_field(r, &last, &fid, &ft))
	{
		if (fid == 1)
			h->type = (int) tr_zigzag(r);
		else if (fid == 2)
			h->usize = (int32) tr_zigzag(r);
		else if (fid == 3)
			h->csize = (int32) tr_zigzag(r);
		else if ((fid == 5 || fid == 7 || fid == 8) && ft == TC_STRUCT)
		{
			int			l2 = 0,
						f2,
						t2;

			while (tr_field(r, &l2, &f2, &t2))
			{
				if (f2 == 1)
					h->num_values = (int32) tr_zigzag(r);
				else if (f2 == 2 && fid != 8)
					h->encoding = (int) tr_zigzag(r);
				else if (f2 == 4 && fid == 8)
					h->encoding = (int) tr_zigzag(r);
				else if (f2 == 5 && fid == 8)
					h->def_len = (int32) tr_zigzag(r);
				else if (f2 == 6 && fid == 8)
					h->rep_len = (int32) tr_zigzag(r);
				else if (f2 == 7 && fid == 8)
					h->is_compressed = tr_bool(r, t2);
				else
					tr_skip(r, t2, 0);
			}
		}
		else
			tr_skip(r, ft, 0);
	}
	if (h->usize < 0 || h->csize < 0 || h->num_values < 0 ||
		h->def_len < 0 || h->rep_len < 0)
		pq_corrupt("negative size in page header");
}

static int
bit_width_of(int maxval)
{
	int			w = 0;

	while (maxval > 0)
	{
		w++;
		maxval >>= 1;
	}
	return w;
}

/*
 * Decode n values of the RLE/bit-packed hybrid encoding.
 * Returns the number of bytes consumed.
 */
static size_t
decode_hybrid(const uint8 *p, size_t len, int bw, int n, int32 *out)
{
	size_t		pos = 0;
	int			got = 0;
	int			vbytes = (bw + 7) / 8;

	if (bw > 32)
		pq_corrupt("bad bit width");
	while (got < n)
	{
		uint64		ind = 0;
		int			shift = 0;
		uint8		b;

		do
		{
			if (pos >= len || shift > 63)
				pq_corrupt("truncated RLE run");
			b = p[pos++];
			ind |= (uint64) (b & 0x7f) << shift;
			shift += 7;
		} while (b & 0x80);

		if (ind & 1)
		{
			uint64		groups = ind >> 1;
			uint64		nvals = groups * 8;
			uint64		nbytes = groups * bw;
			uint64		k;

			if (nbytes > len - pos)
				pq_corrupt("truncated bit-packed run");
			for (k = 0; k < nvals && got < n; k++)
			{
				uint64		bit = k * bw;
				uint32		v = 0;
				int			i;

				for (i = 0; i < bw; i++, bit++)
					if (p[pos + (bit >> 3)] & (1 << (bit & 7)))
						v |= (1U << i);
				out[got++] = (int32) v;
			}
			pos += nbytes;
		}
		else
		{
			uint64		run = ind >> 1;
			uint32		v = 0;
			int			i;

			if ((size_t) vbytes > len - pos)
				pq_corrupt("truncated RLE value");
			for (i = 0; i < vbytes; i++)
				v |= (uint32) p[pos + i] << (8 * i);
			pos += vbytes;
			if (run == 0)
				pq_corrupt("empty RLE run");
			while (run-- > 0 && got < n)
				out[got++] = (int32) v;
		}
	}
	return pos;
}

static uint32
read_le32(const uint8 *p)
{
	return (uint32) p[0] | ((uint32) p[1] << 8) |
		((uint32) p[2] << 16) | ((uint32) p[3] << 24);
}

static uint64
read_le64(const uint8 *p)
{
	return (uint64) read_le32(p) | ((uint64) read_le32(p + 4) << 32);
}

/* ================================================================
 * Value rendering to JSON
 * ================================================================ */

static void
append_hex_string(StringInfo out, const uint8 *p, int len)
{
	static const char hex[] = "0123456789abcdef";
	int			i;

	appendStringInfoString(out, "\"\\\\x");
	for (i = 0; i < len; i++)
	{
		appendStringInfoChar(out, hex[p[i] >> 4]);
		appendStringInfoChar(out, hex[p[i] & 15]);
	}
	appendStringInfoChar(out, '"');
}

static void
append_double(StringInfo out, double v, bool is_float)
{
	char		buf[64];

	if (isnan(v))
		appendStringInfoString(out, "\"NaN\"");
	else if (isinf(v))
		appendStringInfoString(out, v > 0 ? "\"Infinity\"" : "\"-Infinity\"");
	else
	{
		if (is_float)
			float_to_shortest_decimal_buf((float) v, buf);
		else
			double_to_shortest_decimal_buf(v, buf);
		appendStringInfoString(out, buf);
	}
}

static void
append_timestamp(StringInfo out, int64 unix_usecs, bool utc)
{
	Timestamp	ts;
	char		buf[MAXDATELEN + 1];

	if (pg_sub_s64_overflow(unix_usecs, PG_TO_UNIX_USECS, &ts) ||
		!IS_VALID_TIMESTAMP(ts))
	{
		appendStringInfo(out, INT64_FORMAT, unix_usecs);
		return;
	}
	if (utc)
	{
		int			tz = 0;

		JsonEncodeDateTime(buf, TimestampTzGetDatum(ts), TIMESTAMPTZOID, &tz);
	}
	else
		JsonEncodeDateTime(buf, TimestampGetDatum(ts), TIMESTAMPOID, NULL);
	appendStringInfoChar(out, '"');
	appendStringInfoString(out, buf);
	appendStringInfoChar(out, '"');
}

static bool
is_timestamp(PqSchemaNode *n, int *unit, bool *utc)
{
	if (n->logical == PQ_LT_TIMESTAMP)
	{
		*unit = n->ts_unit;
		*utc = n->ts_utc;
		return true;
	}
	if (n->converted == PQ_CT_TIMESTAMP_MICROS || n->converted == PQ_CT_TIMESTAMP_MILLIS)
	{
		*unit = (n->converted == PQ_CT_TIMESTAMP_MILLIS) ? 1 : 2;
		*utc = true;
		return true;
	}
	return false;
}

/*
 * Render one PLAIN-encoded value at *p (advancing *p); "end" bounds the
 * buffer; "bitidx" is the index among boolean values in the page.
 */
static void
render_plain(StringInfo out, PqSchemaNode *n, const uint8 **pp,
			 const uint8 *end, int64 bitidx, const uint8 *bool_base)
{
	const uint8 *p = *pp;

	switch (n->type)
	{
		case PQ_TYPE_BOOLEAN:
			if (bool_base + bitidx / 8 >= end)
				pq_corrupt("truncated boolean data");
			appendStringInfoString(out,
								   (bool_base[bitidx / 8] >> (bitidx % 8)) & 1 ? "true" : "false");
			return;
		case PQ_TYPE_INT32:
			{
				int32		v;

				if (end - p < 4)
					pq_corrupt("truncated INT32 data");
				v = (int32) read_le32(p);
				*pp = p + 4;
				if (n->logical == PQ_LT_DATE || n->converted == PQ_CT_DATE)
				{
					DateADT		d = (DateADT) v - PG_TO_UNIX_DAYS;
					char		buf[MAXDATELEN + 1];

					if (IS_VALID_DATE(d))
					{
						JsonEncodeDateTime(buf, DateADTGetDatum(d), DATEOID, NULL);
						appendStringInfo(out, "\"%s\"", buf);
					}
					else
						appendStringInfo(out, "%d", v);
				}
				else if (n->logical == PQ_LT_INTEGER && !n->int_signed)
					appendStringInfo(out, "%u", (uint32) v);
				else
					appendStringInfo(out, "%d", v);
				return;
			}
		case PQ_TYPE_INT64:
			{
				int64		v;
				int			unit;
				bool		utc;

				if (end - p < 8)
					pq_corrupt("truncated INT64 data");
				v = (int64) read_le64(p);
				*pp = p + 8;
				if (is_timestamp(n, &unit, &utc))
				{
					int64		us = v;

					if (unit == 1)
					{
						if (pg_mul_s64_overflow(v, 1000, &us))
							us = PG_INT64_MAX;
					}
					else if (unit == 3)
						us = (v >= 0) ? v / 1000 : -((-v + 999) / 1000);
					append_timestamp(out, us, utc);
				}
				else if (n->logical == PQ_LT_INTEGER && !n->int_signed)
					appendStringInfo(out, UINT64_FORMAT, (uint64) v);
				else
					appendStringInfo(out, INT64_FORMAT, v);
				return;
			}
		case PQ_TYPE_INT96:
			{
				int64		nanos;
				int32		jday;

				if (end - p < 12)
					pq_corrupt("truncated INT96 data");
				nanos = (int64) read_le64(p);
				jday = (int32) read_le32(p + 8);
				*pp = p + 12;
				append_timestamp(out,
								 ((int64) jday - UNIX_EPOCH_JDATE) * USECS_PER_DAY + nanos / 1000,
								 false);
				return;
			}
		case PQ_TYPE_FLOAT:
			{
				uint32		u;
				float		f;

				if (end - p < 4)
					pq_corrupt("truncated FLOAT data");
				u = read_le32(p);
				memcpy(&f, &u, 4);
				*pp = p + 4;
				append_double(out, f, true);
				return;
			}
		case PQ_TYPE_DOUBLE:
			{
				uint64		u;
				double		d;

				if (end - p < 8)
					pq_corrupt("truncated DOUBLE data");
				u = read_le64(p);
				memcpy(&d, &u, 8);
				*pp = p + 8;
				append_double(out, d, false);
				return;
			}
		case PQ_TYPE_BYTE_ARRAY:
		case PQ_TYPE_FIXED_LEN_BYTE_ARRAY:
			{
				uint32		len;
				const uint8 *s;

				if (n->type == PQ_TYPE_BYTE_ARRAY)
				{
					if (end - p < 4)
						pq_corrupt("truncated BYTE_ARRAY length");
					len = read_le32(p);
					p += 4;
				}
				else
				{
					if (n->type_length <= 0)
						pq_corrupt("FIXED_LEN_BYTE_ARRAY without a length");
					len = n->type_length;
				}
				if ((uint64) (end - p) < len)
					pq_corrupt("truncated byte array data");
				s = p;
				*pp = p + len;

				if (n->logical == PQ_LT_UUID && len == 16)
				{
					static const char hex[] = "0123456789abcdef";
					int			i;

					appendStringInfoChar(out, '"');
					for (i = 0; i < 16; i++)
					{
						if (i == 4 || i == 6 || i == 8 || i == 10)
							appendStringInfoChar(out, '-');
						appendStringInfoChar(out, hex[s[i] >> 4]);
						appendStringInfoChar(out, hex[s[i] & 15]);
					}
					appendStringInfoChar(out, '"');
				}
				else if (n->type == PQ_TYPE_BYTE_ARRAY &&
						 (n->logical == PQ_LT_STRING || n->logical == PQ_LT_JSON ||
						  n->logical == 4 /* ENUM */ ||
						  n->converted == PQ_CT_UTF8 || n->converted == PQ_CT_JSON ||
						  n->converted == 4 /* ENUM */ ))
				{
					char	   *conv = pg_any_to_server((const char *) s, len, PG_UTF8);
					int			clen = (conv == (const char *) s) ? (int) len : (int) strlen(conv);

					if (n->logical == PQ_LT_JSON || n->converted == PQ_CT_JSON)
						appendBinaryStringInfo(out, conv, clen);
					else
						escape_json_with_len(out, conv, clen);
				}
				else
					append_hex_string(out, s, len);
				return;
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported Parquet physical type %d", n->type)));
	}
}

/* ================================================================
 * Column chunk decoding: one JSON fragment per row
 * ================================================================ */

typedef struct ColFrags
{
	StringInfoData buf;
	int64	   *offs;			/* nrows + 1 */
	int64		nrows;			/* rows started so far */
	bool		list_open;
} ColFrags;

static void
frag_start_row(ColFrags *cf, int64 max_rows)
{
	if (cf->list_open)
	{
		appendStringInfoChar(&cf->buf, ']');
		cf->list_open = false;
	}
	if (cf->nrows >= max_rows)
		pq_corrupt("more rows in column chunk than in row group");
	cf->offs[cf->nrows++] = cf->buf.len;
}

static void
decode_chunk(FILE *fp, const char *filepath, PqChunk *ch, PqLeaf *leaf,
			 int64 rg_rows, ColFrags *cf)
{
	uint8	   *chunk;
	size_t		pos = 0;
	StringInfoData *dict = NULL;
	int			ndict = 0;
	int32	   *rep = NULL;
	int32	   *def = NULL;
	int32	   *idx = NULL;
	int			cap = 0;
	uint8	   *ubuf = NULL;
	size_t		ubuf_cap = 0;

	initStringInfo(&cf->buf);
	cf->offs = palloc(sizeof(int64) * (rg_rows + 1));
	cf->nrows = 0;
	cf->list_open = false;

	if (ch->size < 0 || (Size) ch->size > MaxAllocSize - 1 || ch->start < 4)
		pq_corrupt("bad column chunk size or offset");
	chunk = palloc(ch->size + 1);
	if (fseeko(fp, (off_t) ch->start, SEEK_SET) != 0 ||
		fread(chunk, 1, ch->size, fp) != (size_t) ch->size)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read column chunk from \"%s\"", filepath)));

	while (pos < (size_t) ch->size)
	{
		ThriftReader tr;
		PqPageHeader h;
		const uint8 *page;
		const uint8 *vals;
		const uint8 *vend;
		int			n;
		int			i;
		int64		nnonnull;

		tr.data = chunk + pos;
		tr.pos = 0;
		tr.len = ch->size - pos;
		parse_page_header(&tr, &h);
		pos += tr.pos;
		if ((size_t) h.csize > ch->size - pos)
			pq_corrupt("page extends past column chunk");
		page = chunk + pos;
		pos += h.csize;

		if (h.type != PQ_PAGE_DATA && h.type != PQ_PAGE_DATA_V2 &&
			h.type != PQ_PAGE_DICTIONARY)
			continue;			/* index pages etc. */

		/* decompress the part of the page that is compressed */
		{
			size_t		skip = (h.type == PQ_PAGE_DATA_V2) ? (size_t) h.def_len + h.rep_len : 0;
			bool		compressed = (ch->codec == PQ_CODEC_SNAPPY) &&
				(h.type != PQ_PAGE_DATA_V2 || h.is_compressed);

			if (skip > (size_t) h.csize || skip > (size_t) h.usize)
				pq_corrupt("level lengths exceed page size");
			if (compressed)
			{
				size_t		need = h.usize;

				if (need + 1 > ubuf_cap)
				{
					ubuf_cap = need + 1;
					ubuf = ubuf ? repalloc(ubuf, ubuf_cap) : palloc(ubuf_cap);
				}
				memcpy(ubuf, page, skip);
				if (!snappy_decompress(page + skip, h.csize - skip,
									   ubuf + skip, h.usize - skip))
					pq_corrupt("invalid Snappy data");
				page = ubuf;
				vend = ubuf + h.usize;
			}
			else
				vend = page + h.csize;
		}

		if (h.type == PQ_PAGE_DICTIONARY)
		{
			const uint8 *p = page;

			if (h.encoding != PQ_ENC_PLAIN && h.encoding != PQ_ENC_PLAIN_DICTIONARY)
				pq_corrupt("unsupported dictionary page encoding");
			ndict = h.num_values;
			dict = palloc(sizeof(StringInfoData) * Max(ndict, 1));
			for (i = 0; i < ndict; i++)
			{
				initStringInfo(&dict[i]);
				render_plain(&dict[i], leaf->node, &p, vend, i, page);
			}
			continue;
		}

		n = h.num_values;
		if (n > cap)
		{
			cap = n;
			rep = rep ? repalloc(rep, sizeof(int32) * cap) : palloc(sizeof(int32) * cap);
			def = def ? repalloc(def, sizeof(int32) * cap) : palloc(sizeof(int32) * cap);
			idx = idx ? repalloc(idx, sizeof(int32) * cap) : palloc(sizeof(int32) * cap);
		}

		/* levels */
		{
			const uint8 *p = page;

			if (h.type == PQ_PAGE_DATA)
			{
				if (leaf->max_rep > 0)
				{
					uint32		l;

					if (vend - p < 4)
						pq_corrupt("truncated repetition levels");
					l = read_le32(p);
					p += 4;
					if ((uint64) (vend - p) < l)
						pq_corrupt("truncated repetition levels");
					decode_hybrid(p, l, bit_width_of(leaf->max_rep), n, rep);
					p += l;
				}
				if (leaf->max_def > 0)
				{
					uint32		l;

					if (vend - p < 4)
						pq_corrupt("truncated definition levels");
					l = read_le32(p);
					p += 4;
					if ((uint64) (vend - p) < l)
						pq_corrupt("truncated definition levels");
					decode_hybrid(p, l, bit_width_of(leaf->max_def), n, def);
					p += l;
				}
			}
			else
			{
				if (leaf->max_rep > 0)
					decode_hybrid(p, h.rep_len, bit_width_of(leaf->max_rep), n, rep);
				p += h.rep_len;
				if (leaf->max_def > 0)
					decode_hybrid(p, h.def_len, bit_width_of(leaf->max_def), n, def);
				p += h.def_len;
			}
			if (leaf->max_rep == 0)
				memset(rep, 0, sizeof(int32) * n);
			if (leaf->max_def == 0)
				for (i = 0; i < n; i++)
					def[i] = 0;
			vals = p;
		}

		nnonnull = 0;
		for (i = 0; i < n; i++)
		{
			if (def[i] > leaf->max_def || rep[i] > leaf->max_rep)
				pq_corrupt("level out of range");
			if (def[i] == leaf->max_def)
				nnonnull++;
		}

		/* value decoding setup */
		if (h.encoding == PQ_ENC_PLAIN_DICTIONARY || h.encoding == PQ_ENC_RLE_DICTIONARY)
		{
			int			bw;

			if (dict == NULL)
				pq_corrupt("dictionary-encoded page without dictionary");
			if (nnonnull > 0)
			{
				if (vals >= vend)
					pq_corrupt("truncated dictionary indices");
				bw = vals[0];
				decode_hybrid(vals + 1, vend - vals - 1, bw, (int) nnonnull, idx);
				for (i = 0; i < nnonnull; i++)
					if (idx[i] < 0 || idx[i] >= ndict)
						pq_corrupt("dictionary index out of range");
			}
		}
		else if (h.encoding == PQ_ENC_RLE && leaf->node->type == PQ_TYPE_BOOLEAN)
		{
			const uint8 *p = vals;
			uint32		l;

			/* RLE-encoded values carry a 4-byte length in v1 and v2 pages */
			if (vend - p < 4)
				pq_corrupt("truncated boolean RLE data");
			l = read_le32(p);
			p += 4;
			if ((uint64) (vend - p) < l)
				pq_corrupt("truncated boolean RLE data");
			decode_hybrid(p, l, 1, (int) nnonnull, idx);
		}
		else if (h.encoding != PQ_ENC_PLAIN)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Parquet encoding %d in column \"%s\" is not supported",
							h.encoding, leaf->name),
					 errhint("Supported encodings are PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY and RLE (booleans).")));

		/* assemble rows */
		{
			const uint8 *p = vals;
			int64		vi = 0;

			for (i = 0; i < n; i++)
			{
				bool		is_value = (def[i] == leaf->max_def);

				if (leaf->max_rep == 0)
				{
					frag_start_row(cf, rg_rows);
				}
				else if (rep[i] == 0)
				{
					frag_start_row(cf, rg_rows);
					if (def[i] < leaf->list_def - 1)
					{
						appendStringInfoString(&cf->buf, "null");
						continue;
					}
					if (def[i] == leaf->list_def - 1)
					{
						appendStringInfoString(&cf->buf, "[]");
						continue;
					}
					appendStringInfoChar(&cf->buf, '[');
					cf->list_open = true;
				}
				else
				{
					if (!cf->list_open || def[i] < leaf->list_def)
						pq_corrupt("bad repetition level sequence");
					appendStringInfoChar(&cf->buf, ',');
				}

				if (!is_value)
				{
					appendStringInfoString(&cf->buf, "null");
					continue;
				}

				if (h.encoding == PQ_ENC_PLAIN_DICTIONARY || h.encoding == PQ_ENC_RLE_DICTIONARY)
					appendBinaryStringInfo(&cf->buf, dict[idx[vi]].data, dict[idx[vi]].len);
				else if (h.encoding == PQ_ENC_RLE)
					appendStringInfoString(&cf->buf, idx[vi] ? "true" : "false");
				else
					render_plain(&cf->buf, leaf->node, &p, vend, vi, vals);
				vi++;
			}
		}
	}

	if (cf->list_open)
	{
		appendStringInfoChar(&cf->buf, ']');
		cf->list_open = false;
	}
	/* rows missing from the chunk read as null */
	while (cf->nrows < rg_rows)
	{
		cf->offs[cf->nrows++] = cf->buf.len;
		appendStringInfoString(&cf->buf, "null");
	}
	cf->offs[rg_rows] = cf->buf.len;
	pfree(chunk);
}

/* ================================================================
 * read_parquet_full -- materialized SRF
 * ================================================================ */
Datum
read_parquet_full(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char	   *filepath = text_to_cstring(PG_GETARG_TEXT_PP(0));
	FILE	   *fp;
	uint8		tail[8];
	uint8		magic[4];
	off_t		file_size;
	uint32		footer_len;
	uint8	   *footer;
	PqFileMeta *fm;
	MemoryContext rgcxt;
	MemoryContext rowcxt;
	MemoryContext oldcxt;
	StringInfoData row;
	int			r;
	char	  **keys;

	/* options jsonb (arg 1) is reserved for future use */

	if (!parquet_validate_path(filepath))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("file path \"%s\" is not in an allowed directory",
						filepath)));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	fp = AllocateFile(filepath, PG_BINARY_R);
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filepath)));

	if (fread(magic, 1, 4, fp) != 4 || memcmp(magic, "PAR1", 4) != 0 ||
		fseeko(fp, 0, SEEK_END) != 0 || (file_size = ftello(fp)) < 12)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("\"%s\" is not a valid Parquet file", filepath)));

	if (fseeko(fp, file_size - 8, SEEK_SET) != 0 ||
		fread(tail, 1, 8, fp) != 8 || memcmp(tail + 4, "PAR1", 4) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("corrupted Parquet footer in \"%s\"", filepath)));

	footer_len = read_le32(tail);
	if (footer_len == 0 || (off_t) footer_len > file_size - 12 ||
		footer_len > MaxAllocSize - 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid Parquet footer length %u", footer_len)));

	footer = palloc(footer_len);
	if (fseeko(fp, file_size - 8 - footer_len, SEEK_SET) != 0 ||
		fread(footer, 1, footer_len, fp) != footer_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read Parquet footer of \"%s\"", filepath)));

	fm = parse_footer(footer, (int) footer_len);
	pfree(footer);

	/* pre-render the JSON object keys */
	keys = palloc(sizeof(char *) * Max(fm->nleaves, 1));
	for (r = 0; r < fm->nleaves; r++)
	{
		StringInfoData k;
		char	   *nm = fm->leaves[r].name;
		char	   *conv = pg_any_to_server(nm, strlen(nm), PG_UTF8);

		initStringInfo(&k);
		escape_json(&k, conv);
		appendStringInfoChar(&k, ':');
		keys[r] = k.data;
	}

	rgcxt = AllocSetContextCreate(CurrentMemoryContext, "read_parquet row group",
								  ALLOCSET_DEFAULT_SIZES);
	rowcxt = AllocSetContextCreate(CurrentMemoryContext, "read_parquet row",
								   ALLOCSET_DEFAULT_SIZES);
	initStringInfo(&row);

	for (r = 0; r < fm->nrgs; r++)
	{
		PqRowGroup *rg = &fm->rgs[r];
		ColFrags   *frags;
		int64		i;
		int			c;

		if (rg->num_rows <= 0)
			continue;
		if ((Size) rg->num_rows > MaxAllocSize / sizeof(int64) - 1)
			pq_corrupt("row group too large");

		oldcxt = MemoryContextSwitchTo(rgcxt);
		frags = palloc0(sizeof(ColFrags) * Max(fm->nleaves, 1));
		for (c = 0; c < fm->nleaves; c++)
			decode_chunk(fp, filepath, &rg->cols[c], &fm->leaves[c],
						 rg->num_rows, &frags[c]);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < rg->num_rows; i++)
		{
			Datum		val;
			bool		isnull = false;

			resetStringInfo(&row);
			appendStringInfoChar(&row, '{');
			for (c = 0; c < fm->nleaves; c++)
			{
				ColFrags   *cf = &frags[c];

				if (c > 0)
					appendStringInfoChar(&row, ',');
				appendStringInfoString(&row, keys[c]);
				appendBinaryStringInfo(&row, cf->buf.data + cf->offs[i],
									   (int) (cf->offs[i + 1] - cf->offs[i]));
			}
			appendStringInfoChar(&row, '}');

			/* jsonb_in leaves parser garbage behind; contain it per row */
			oldcxt = MemoryContextSwitchTo(rowcxt);
			val = DirectFunctionCall1(jsonb_in, CStringGetDatum(row.data));
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, &val, &isnull);
			MemoryContextSwitchTo(oldcxt);
			MemoryContextReset(rowcxt);
		}
		MemoryContextReset(rgcxt);
		CHECK_FOR_INTERRUPTS();
	}

	FreeFile(fp);
	MemoryContextDelete(rowcxt);
	MemoryContextDelete(rgcxt);
	return (Datum) 0;
}
