/*-------------------------------------------------------------------------
 *
 * parquet_writer.c
 *	  write_parquet(): export the result of a query to an Apache Parquet
 *	  file on the server.
 *
 *	  The query is executed through an SPI cursor and fetched in small
 *	  batches; rows are accumulated column-wise into one row group at a
 *	  time (at most row_group_rows rows, or ~128MB of encoded data), which
 *	  is then written out, so memory use is bounded by one row group.
 *
 *	  File layout produced (Parquet format 2.x, data page v1):
 *	    "PAR1"
 *	    row group*: one column chunk per column; each chunk is a sequence
 *	      of DATA_PAGEs (~1MB / 20000 rows each), PLAIN-encoded values,
 *	      repetition/definition levels in the RLE/bit-packed hybrid
 *	      encoding, page body optionally Snappy-compressed
 *	    FileMetaData (Thrift compact protocol), with schema, logical type
 *	      annotations, per-chunk statistics and column orders
 *	    footer length (int32 LE), "PAR1"
 *
 *	  Type mapping (all columns are OPTIONAL):
 *	    bool                   BOOLEAN
 *	    int2 / int4            INT32  + INTEGER(16|32, signed)
 *	    int8                   INT64  + INTEGER(64, signed)
 *	    float4 / float8        FLOAT / DOUBLE
 *	    text, varchar, bpchar  BYTE_ARRAY + STRING
 *	    bytea                  BYTE_ARRAY
 *	    date                   INT32 + DATE (days since 1970-01-01)
 *	    timestamp              INT64 + TIMESTAMP(MICROS, isAdjustedToUTC=false)
 *	    timestamptz            INT64 + TIMESTAMP(MICROS, isAdjustedToUTC=true)
 *	    uuid                   FIXED_LEN_BYTE_ARRAY(16) + UUID
 *	    json / jsonb           BYTE_ARRAY + JSON
 *	    vector, float4[]       LIST<FLOAT>  (3-level list)
 *	    float8[]               LIST<DOUBLE> (3-level list)
 *	    numeric, anything else BYTE_ARRAY + STRING (type output function;
 *	                           numeric stays exact -- cast to float8 in the
 *	                           query for a DOUBLE column)
 *	  PostgreSQL date/timestamp values are shifted from the 2000-01-01
 *	  epoch to the Unix epoch.
 *
 *	  Prior art / patent notes: Apache Parquet (format specification and
 *	  parquet.thrift) and Apache Thrift are published under the Apache
 *	  License 2.0, which carries an express patent grant from contributors.
 *	  Only the published, openly documented encodings are used here: PLAIN,
 *	  the RLE/bit-packed hybrid for levels, and Snappy (open format; the
 *	  compressor is a plain LZ77-style matcher, see pq_common.c).  No
 *	  dictionary, delta, or proprietary encodings are implemented.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_parquetio/parquet_writer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/uuid.h"

#include "pg_parquetio.h"
#include "export_common.h"
#include "pq_common.h"

PG_FUNCTION_INFO_V1(write_parquet);

#define PQ_PAGE_TARGET_BYTES	(1024 * 1024)
#define PQ_PAGE_MAX_ROWS		20000
#define PQ_STATS_MAX_LEN		64

typedef struct PqStats
{
	int64		null_count;
	bool		has_minmax;
	bool		minmax_disabled;
	int64		imin,
				imax;
	double		dmin,
				dmax;
	StringInfoData smin,
				smax;
} PqStats;

typedef struct PqColumn
{
	ExportColumn *ec;
	int			ptype;			/* physical type of the leaf */
	bool		is_list;
	int			max_def;
	int			max_rep;

	/* current page */
	uint8	   *def;
	uint8	   *rep;
	int			nlevels;
	int			levels_cap;
	StringInfoData values;
	int			nbools;
	int			page_rows;

	/* current column chunk (encoded pages) */
	StringInfoData chunk;
	int64		chunk_values;
	int64		chunk_ubytes;
	PqStats		st;
} PqColumn;

typedef struct PqChunkMeta
{
	int64		offset;
	int64		num_values;
	int64		ubytes;
	int64		cbytes;
	int64		null_count;
	bool		has_stats;
	bool		has_minmax;
	char	   *min;
	char	   *max;
	int			minlen;
	int			maxlen;
} PqChunkMeta;

typedef struct PqRowGroupMeta
{
	int64		num_rows;
	int64		offset;
	int64		ubytes;
	int64		cbytes;
	PqChunkMeta *cols;
} PqRowGroupMeta;

typedef struct PqWriter
{
	int			ncols;
	PqColumn   *cols;
	int			codec;
	ExportFile *file;
	int64		rg_rows;
	int64		total_rows;
	PqRowGroupMeta *rgs;
	int			nrgs;
	int			rgs_cap;
	StringInfoData pagebuf;
	StringInfoData compbuf;
	StringInfoData hdrbuf;
} PqWriter;

/* ----------------------------------------------------------------
 * Levels: RLE / bit-packed hybrid encoding
 * ----------------------------------------------------------------
 */
static void
append_uvarint(StringInfo s, uint64 v)
{
	while (v >= 0x80)
	{
		appendStringInfoChar(s, (char) ((v & 0x7f) | 0x80));
		v >>= 7;
	}
	appendStringInfoChar(s, (char) v);
}

static int
run_length_at(const uint8 *v, int n, int i)
{
	int			r = 1;

	while (i + r < n && v[i + r] == v[i])
		r++;
	return r;
}

static void
rle_hybrid_encode(StringInfo out, const uint8 *v, int n, int bw)
{
	int			i = 0;

	while (i < n)
	{
		int			run = run_length_at(v, n, i);

		if (run >= 8)
		{
			append_uvarint(out, (uint64) run << 1);
			appendStringInfoChar(out, (char) v[i]);	/* bw <= 8: one byte */
			i += run;
		}
		else
		{
			int			start = i;
			int			groups = 0;
			int			nbytes;
			int			k;
			char	   *dst;

			do
			{
				groups++;
				i += 8;
			} while (i < n && run_length_at(v, n, i) < 8);

			append_uvarint(out, ((uint64) groups << 1) | 1);
			nbytes = groups * bw;
			enlargeStringInfo(out, nbytes);
			dst = out->data + out->len;
			memset(dst, 0, nbytes);
			for (k = 0; k < groups * 8; k++)
			{
				uint32		val = (start + k < n) ? v[start + k] : 0;
				int			bit = k * bw;
				int			b;

				for (b = 0; b < bw; b++, bit++)
					if (val & (1U << b))
						dst[bit >> 3] |= (char) (1 << (bit & 7));
			}
			out->len += nbytes;
			out->data[out->len] = '\0';
			if (i > n)
				i = n;
		}
	}
}

static int
bit_width(int maxval)
{
	int			w = 0;

	while (maxval > 0)
	{
		w++;
		maxval >>= 1;
	}
	return w;
}

/* v1 data page levels: 4-byte LE length followed by the hybrid stream */
static void
encode_levels(StringInfo out, const uint8 *v, int n, int maxlevel)
{
	int			lenpos = out->len;
	uint32		len;

	append_le32(out, 0);
	rle_hybrid_encode(out, v, n, bit_width(maxlevel));
	len = out->len - lenpos - 4;
	out->data[lenpos] = (char) (len & 0xff);
	out->data[lenpos + 1] = (char) ((len >> 8) & 0xff);
	out->data[lenpos + 2] = (char) ((len >> 16) & 0xff);
	out->data[lenpos + 3] = (char) ((len >> 24) & 0xff);
}

/* ----------------------------------------------------------------
 * Column setup
 * ----------------------------------------------------------------
 */
static int
leaf_ptype(ExportKind k)
{
	switch (k)
	{
		case EK_BOOL:
			return PQ_TYPE_BOOLEAN;
		case EK_INT16:
		case EK_INT32:
		case EK_DATE:
			return PQ_TYPE_INT32;
		case EK_INT64:
		case EK_TIMESTAMP:
		case EK_TIMESTAMPTZ:
			return PQ_TYPE_INT64;
		case EK_FLOAT4:
		case EK_VECTOR:
		case EK_FLOAT4_ARRAY:
			return PQ_TYPE_FLOAT;
		case EK_FLOAT8:
		case EK_FLOAT8_ARRAY:
			return PQ_TYPE_DOUBLE;
		case EK_UUID:
			return PQ_TYPE_FIXED_LEN_BYTE_ARRAY;
		case EK_TEXT:
		case EK_BYTEA:
		case EK_JSON:
		case EK_OUTPUT_TEXT:
			return PQ_TYPE_BYTE_ARRAY;
	}
	return PQ_TYPE_BYTE_ARRAY;
}

static void
pq_reset_stats(PqColumn *c)
{
	c->st.null_count = 0;
	c->st.has_minmax = false;
	/* min/max not meaningful for JSON or lists; skip */
	c->st.minmax_disabled = (c->is_list || c->ec->kind == EK_JSON);
	resetStringInfo(&c->st.smin);
	resetStringInfo(&c->st.smax);
}

static void
pq_init_columns(PqWriter *w, ExportColumn *ecols, int ncols)
{
	int			i;

	w->ncols = ncols;
	w->cols = palloc0(sizeof(PqColumn) * ncols);
	for (i = 0; i < ncols; i++)
	{
		PqColumn   *c = &w->cols[i];

		c->ec = &ecols[i];
		c->ptype = leaf_ptype(ecols[i].kind);
		c->is_list = (ecols[i].kind == EK_VECTOR ||
					  ecols[i].kind == EK_FLOAT4_ARRAY ||
					  ecols[i].kind == EK_FLOAT8_ARRAY);
		c->max_def = c->is_list ? 3 : 1;
		c->max_rep = c->is_list ? 1 : 0;
		c->levels_cap = 1024;
		c->def = palloc(c->levels_cap);
		c->rep = palloc(c->levels_cap);
		initStringInfo(&c->values);
		initStringInfo(&c->chunk);
		initStringInfo(&c->st.smin);
		initStringInfo(&c->st.smax);
		pq_reset_stats(c);
	}
}

/* ----------------------------------------------------------------
 * Appending values
 * ----------------------------------------------------------------
 */
static inline void
push_level(PqColumn *c, int rep, int def)
{
	if (c->nlevels >= c->levels_cap)
	{
		if ((Size) c->levels_cap * 2 >= MaxAllocSize)
			elog(ERROR, "too many values in one Parquet page");
		c->levels_cap *= 2;
		c->def = repalloc(c->def, c->levels_cap);
		c->rep = repalloc(c->rep, c->levels_cap);
	}
	c->def[c->nlevels] = (uint8) def;
	c->rep[c->nlevels] = (uint8) rep;
	c->nlevels++;
}

static void
stats_int(PqColumn *c, int64 v)
{
	if (c->st.minmax_disabled)
		return;
	if (!c->st.has_minmax)
	{
		c->st.imin = c->st.imax = v;
		c->st.has_minmax = true;
	}
	else if (v < c->st.imin)
		c->st.imin = v;
	else if (v > c->st.imax)
		c->st.imax = v;
}

static void
stats_float(PqColumn *c, double v)
{
	if (c->st.minmax_disabled || isnan(v))
		return;
	if (!c->st.has_minmax)
	{
		c->st.dmin = c->st.dmax = v;
		c->st.has_minmax = true;
	}
	else if (v < c->st.dmin)
		c->st.dmin = v;
	else if (v > c->st.dmax)
		c->st.dmax = v;
}

static int
bytes_cmp(const char *a, int alen, const char *b, int blen)
{
	int			r = memcmp(a, b, Min(alen, blen));

	if (r != 0)
		return r;
	return (alen > blen) - (alen < blen);
}

static void
stats_bytes(PqColumn *c, const char *s, int len)
{
	if (c->st.minmax_disabled)
		return;
	if (len > PQ_STATS_MAX_LEN)
	{
		/* keep the footer small; drop min/max for this chunk */
		c->st.minmax_disabled = true;
		c->st.has_minmax = false;
		return;
	}
	if (!c->st.has_minmax)
	{
		appendBinaryStringInfo(&c->st.smin, s, len);
		appendBinaryStringInfo(&c->st.smax, s, len);
		c->st.has_minmax = true;
		return;
	}
	if (bytes_cmp(s, len, c->st.smin.data, c->st.smin.len) < 0)
	{
		resetStringInfo(&c->st.smin);
		appendBinaryStringInfo(&c->st.smin, s, len);
	}
	if (bytes_cmp(s, len, c->st.smax.data, c->st.smax.len) > 0)
	{
		resetStringInfo(&c->st.smax);
		appendBinaryStringInfo(&c->st.smax, s, len);
	}
}

static void
append_byte_array(PqColumn *c, const char *s, int len)
{
	append_le32(&c->values, (uint32) len);
	appendBinaryStringInfo(&c->values, s, len);
	stats_bytes(c, s, len);
}

static void
pq_add_scalar(PqColumn *c, Datum d)
{
	ExportColumn *ec = c->ec;

	switch (ec->kind)
	{
		case EK_BOOL:
			{
				bool		v = DatumGetBool(d);

				if (c->nbools % 8 == 0)
					appendStringInfoChar(&c->values, 0);
				if (v)
					c->values.data[c->values.len - 1] |= (char) (1 << (c->nbools % 8));
				c->nbools++;
				stats_int(c, v ? 1 : 0);
				break;
			}
		case EK_INT16:
			append_le32(&c->values, (uint32) (int32) DatumGetInt16(d));
			stats_int(c, DatumGetInt16(d));
			break;
		case EK_INT32:
			append_le32(&c->values, (uint32) DatumGetInt32(d));
			stats_int(c, DatumGetInt32(d));
			break;
		case EK_INT64:
			append_le64(&c->values, (uint64) DatumGetInt64(d));
			stats_int(c, DatumGetInt64(d));
			break;
		case EK_FLOAT4:
			append_f32(&c->values, DatumGetFloat4(d));
			stats_float(c, DatumGetFloat4(d));
			break;
		case EK_FLOAT8:
			append_f64(&c->values, DatumGetFloat8(d));
			stats_float(c, DatumGetFloat8(d));
			break;
		case EK_DATE:
			{
				int32		v = export_date_value(d);

				append_le32(&c->values, (uint32) v);
				stats_int(c, v);
				break;
			}
		case EK_TIMESTAMP:
		case EK_TIMESTAMPTZ:
			{
				int64		v = export_timestamp_value(d);

				append_le64(&c->values, (uint64) v);
				stats_int(c, v);
				break;
			}
		case EK_UUID:
			{
				pg_uuid_t  *u = DatumGetUUIDP(d);

				appendBinaryStringInfo(&c->values, (const char *) u->data, UUID_LEN);
				stats_bytes(c, (const char *) u->data, UUID_LEN);
				break;
			}
		case EK_TEXT:
		case EK_BYTEA:
		case EK_JSON:
		case EK_OUTPUT_TEXT:
			{
				int			len;
				const char *s = export_text_value(ec, d, &len);

				append_byte_array(c, s, len);
				break;
			}
		default:
			elog(ERROR, "unexpected export kind %d", (int) ec->kind);
	}
}

static void
pq_add_value(PqColumn *c, Datum d, bool isnull)
{
	if (!c->is_list)
	{
		if (isnull)
		{
			push_level(c, 0, 0);
			c->st.null_count++;
		}
		else
		{
			push_level(c, 0, 1);
			pq_add_scalar(c, d);
		}
	}
	else if (isnull)
	{
		push_level(c, 0, 0);
		c->st.null_count++;
	}
	else if (c->ec->kind == EK_VECTOR)
	{
		int			dim;
		const float *x = export_vector_value(c->ec, d, &dim);
		int			i;

		if (dim == 0)
			push_level(c, 0, 1);
		for (i = 0; i < dim; i++)
			push_level(c, i == 0 ? 0 : 1, 3);
		append_f32_array(&c->values, x, dim);
	}
	else
	{
		Datum	   *elems;
		bool	   *nulls;
		int			n = export_float_array_value(c->ec, d, &elems, &nulls);
		int			i;

		if (n == 0)
			push_level(c, 0, 1);
		for (i = 0; i < n; i++)
		{
			int			rep = (i == 0) ? 0 : 1;

			if (nulls[i])
				push_level(c, rep, 2);
			else
			{
				push_level(c, rep, 3);
				if (c->ec->kind == EK_FLOAT4_ARRAY)
					append_f32(&c->values, DatumGetFloat4(elems[i]));
				else
					append_f64(&c->values, DatumGetFloat8(elems[i]));
			}
		}
	}
	c->page_rows++;
}

/* ----------------------------------------------------------------
 * Pages, row groups
 * ----------------------------------------------------------------
 */
static void
pq_flush_page(PqWriter *w, PqColumn *c)
{
	StringInfo	body = &w->pagebuf;
	StringInfo	payload;
	ThriftWriter tw;
	int64		ulen;

	if (c->nlevels == 0)
		return;

	resetStringInfo(body);
	if (c->max_rep > 0)
		encode_levels(body, c->rep, c->nlevels, c->max_rep);
	if (c->max_def > 0)
		encode_levels(body, c->def, c->nlevels, c->max_def);
	appendBinaryStringInfo(body, c->values.data, c->values.len);
	ulen = body->len;

	if (w->codec == PQ_CODEC_SNAPPY)
	{
		resetStringInfo(&w->compbuf);
		snappy_compress((const uint8 *) body->data, body->len, &w->compbuf);
		payload = &w->compbuf;
	}
	else
		payload = body;

	resetStringInfo(&w->hdrbuf);
	tw_init(&tw, &w->hdrbuf);
	tw_field_i32(&tw, 1, PQ_PAGE_DATA);
	tw_field_i32(&tw, 2, (int32) ulen);
	tw_field_i32(&tw, 3, (int32) payload->len);
	tw_field_struct_begin(&tw, 5);	/* DataPageHeader */
	tw_field_i32(&tw, 1, c->nlevels);
	tw_field_i32(&tw, 2, PQ_ENC_PLAIN);
	tw_field_i32(&tw, 3, PQ_ENC_RLE);
	tw_field_i32(&tw, 4, PQ_ENC_RLE);
	tw_struct_end(&tw);
	appendStringInfoChar(&w->hdrbuf, 0);	/* end PageHeader */

	appendBinaryStringInfo(&c->chunk, w->hdrbuf.data, w->hdrbuf.len);
	appendBinaryStringInfo(&c->chunk, payload->data, payload->len);
	c->chunk_values += c->nlevels;
	c->chunk_ubytes += w->hdrbuf.len + ulen;

	c->nlevels = 0;
	c->nbools = 0;
	c->page_rows = 0;
	resetStringInfo(&c->values);
}

static Size
pq_buffered_bytes(PqWriter *w)
{
	Size		total = 0;
	int			i;

	for (i = 0; i < w->ncols; i++)
		total += w->cols[i].chunk.len + w->cols[i].values.len;
	return total;
}

static void
pq_stat_bytes(PqColumn *c, bool is_min, char **out, int *outlen)
{
	StringInfoData s;

	initStringInfo(&s);
	switch (c->ptype)
	{
		case PQ_TYPE_BOOLEAN:
			appendStringInfoChar(&s, (char) (is_min ? c->st.imin : c->st.imax));
			break;
		case PQ_TYPE_INT32:
			append_le32(&s, (uint32) (int32) (is_min ? c->st.imin : c->st.imax));
			break;
		case PQ_TYPE_INT64:
			append_le64(&s, (uint64) (is_min ? c->st.imin : c->st.imax));
			break;
		case PQ_TYPE_FLOAT:
		case PQ_TYPE_DOUBLE:
			{
				double		v = is_min ? c->st.dmin : c->st.dmax;

				/* spec: write -0.0 as min and +0.0 as max for zeros */
				if (v == 0.0)
					v = is_min ? -0.0 : 0.0;
				if (c->ptype == PQ_TYPE_FLOAT)
					append_f32(&s, (float) v);
				else
					append_f64(&s, v);
				break;
			}
		default:
			{
				StringInfo	src = is_min ? &c->st.smin : &c->st.smax;

				appendBinaryStringInfo(&s, src->data, src->len);
				break;
			}
	}
	*out = s.data;
	*outlen = s.len;
}

static void
pq_flush_row_group(PqWriter *w)
{
	PqRowGroupMeta *rg;
	int			i;

	if (w->rg_rows == 0)
		return;

	if (w->nrgs >= w->rgs_cap)
	{
		w->rgs_cap = w->rgs_cap ? w->rgs_cap * 2 : 16;
		w->rgs = w->rgs ? repalloc(w->rgs, sizeof(PqRowGroupMeta) * w->rgs_cap)
			: palloc(sizeof(PqRowGroupMeta) * w->rgs_cap);
	}
	rg = &w->rgs[w->nrgs++];
	rg->num_rows = w->rg_rows;
	rg->offset = (int64) w->file->offset;
	rg->ubytes = 0;
	rg->cbytes = 0;
	rg->cols = palloc0(sizeof(PqChunkMeta) * w->ncols);

	for (i = 0; i < w->ncols; i++)
	{
		PqColumn   *c = &w->cols[i];
		PqChunkMeta *m = &rg->cols[i];

		pq_flush_page(w, c);

		m->offset = (int64) w->file->offset;
		m->num_values = c->chunk_values;
		m->ubytes = c->chunk_ubytes;
		m->cbytes = c->chunk.len;
		m->has_stats = !c->is_list;
		m->null_count = c->st.null_count;
		m->has_minmax = c->st.has_minmax && !c->st.minmax_disabled;
		if (m->has_minmax)
		{
			pq_stat_bytes(c, true, &m->min, &m->minlen);
			pq_stat_bytes(c, false, &m->max, &m->maxlen);
		}

		export_file_write(w->file, c->chunk.data, c->chunk.len);
		rg->ubytes += m->ubytes;
		rg->cbytes += m->cbytes;

		/* release big buffers rather than keeping their peak size */
		if (c->chunk.maxlen > 8 * 1024 * 1024)
		{
			pfree(c->chunk.data);
			initStringInfo(&c->chunk);
		}
		else
			resetStringInfo(&c->chunk);
		c->chunk_values = 0;
		c->chunk_ubytes = 0;
		pq_reset_stats(c);
	}
	w->rg_rows = 0;
}

/* ----------------------------------------------------------------
 * Footer (FileMetaData)
 * ----------------------------------------------------------------
 */
static void
tw_empty_struct(ThriftWriter *tw, int fid)
{
	tw_field_struct_begin(tw, fid);
	tw_struct_end(tw);
}

static void
pq_write_leaf_annotations(ThriftWriter *tw, ExportKind k)
{
	int			ct = -1;

	switch (k)
	{
		case EK_INT16:
			ct = PQ_CT_INT_16;
			break;
		case EK_INT32:
			ct = PQ_CT_INT_32;
			break;
		case EK_INT64:
			ct = PQ_CT_INT_64;
			break;
		case EK_TEXT:
		case EK_OUTPUT_TEXT:
			ct = PQ_CT_UTF8;
			break;
		case EK_DATE:
			ct = PQ_CT_DATE;
			break;
		case EK_TIMESTAMPTZ:
			ct = PQ_CT_TIMESTAMP_MICROS;
			break;
		case EK_JSON:
			ct = PQ_CT_JSON;
			break;
		default:
			break;
	}
	if (ct >= 0)
		tw_field_i32(tw, 6, ct);

	switch (k)
	{
		case EK_INT16:
		case EK_INT32:
		case EK_INT64:
			tw_field_struct_begin(tw, 10);
			tw_field_struct_begin(tw, PQ_LT_INTEGER);
			tw_field_byte(tw, 1, k == EK_INT16 ? 16 : (k == EK_INT32 ? 32 : 64));
			tw_field_bool(tw, 2, true);
			tw_struct_end(tw);
			tw_struct_end(tw);
			break;
		case EK_TEXT:
		case EK_OUTPUT_TEXT:
			tw_field_struct_begin(tw, 10);
			tw_empty_struct(tw, PQ_LT_STRING);
			tw_struct_end(tw);
			break;
		case EK_DATE:
			tw_field_struct_begin(tw, 10);
			tw_empty_struct(tw, PQ_LT_DATE);
			tw_struct_end(tw);
			break;
		case EK_TIMESTAMP:
		case EK_TIMESTAMPTZ:
			tw_field_struct_begin(tw, 10);
			tw_field_struct_begin(tw, PQ_LT_TIMESTAMP);
			tw_field_bool(tw, 1, k == EK_TIMESTAMPTZ);
			tw_field_struct_begin(tw, 2);	/* TimeUnit */
			tw_empty_struct(tw, 2); /* MICROS */
			tw_struct_end(tw);
			tw_struct_end(tw);
			tw_struct_end(tw);
			break;
		case EK_JSON:
			tw_field_struct_begin(tw, 10);
			tw_empty_struct(tw, PQ_LT_JSON);
			tw_struct_end(tw);
			break;
		case EK_UUID:
			tw_field_struct_begin(tw, 10);
			tw_empty_struct(tw, PQ_LT_UUID);
			tw_struct_end(tw);
			break;
		default:
			break;
	}
}

static int
pq_schema_count(PqWriter *w)
{
	int			n = 1;
	int			i;

	for (i = 0; i < w->ncols; i++)
		n += w->cols[i].is_list ? 3 : 1;
	return n;
}

static void
pq_write_schema(PqWriter *w, ThriftWriter *tw)
{
	int			i;

	tw_field_list_begin(tw, 2, TC_STRUCT, pq_schema_count(w));

	/* root */
	tw_list_struct_begin(tw);
	tw_field_binary(tw, 4, "schema", 6);
	tw_field_i32(tw, 5, w->ncols);
	tw_struct_end(tw);

	for (i = 0; i < w->ncols; i++)
	{
		PqColumn   *c = &w->cols[i];

		if (c->is_list)
		{
			/* optional group <name> (LIST) */
			tw_list_struct_begin(tw);
			tw_field_i32(tw, 3, PQ_REP_OPTIONAL);
			tw_field_binary(tw, 4, c->ec->name, c->ec->namelen);
			tw_field_i32(tw, 5, 1);
			tw_field_i32(tw, 6, PQ_CT_LIST);
			tw_field_struct_begin(tw, 10);
			tw_empty_struct(tw, PQ_LT_LIST);
			tw_struct_end(tw);
			tw_struct_end(tw);
			/* repeated group list */
			tw_list_struct_begin(tw);
			tw_field_i32(tw, 3, PQ_REP_REPEATED);
			tw_field_binary(tw, 4, "list", 4);
			tw_field_i32(tw, 5, 1);
			tw_struct_end(tw);
			/* optional <float|double> element */
			tw_list_struct_begin(tw);
			tw_field_i32(tw, 1, c->ptype);
			tw_field_i32(tw, 3, PQ_REP_OPTIONAL);
			tw_field_binary(tw, 4, "element", 7);
			tw_struct_end(tw);
		}
		else
		{
			tw_list_struct_begin(tw);
			tw_field_i32(tw, 1, c->ptype);
			if (c->ptype == PQ_TYPE_FIXED_LEN_BYTE_ARRAY)
				tw_field_i32(tw, 2, UUID_LEN);
			tw_field_i32(tw, 3, PQ_REP_OPTIONAL);
			tw_field_binary(tw, 4, c->ec->name, c->ec->namelen);
			pq_write_leaf_annotations(tw, c->ec->kind);
			tw_struct_end(tw);
		}
	}
}

static void
pq_write_footer(PqWriter *w)
{
	StringInfoData buf;
	ThriftWriter tw;
	int			r,
				i;
	const char *created_by = "pg_parquetio version 1.2";

	initStringInfo(&buf);
	tw_init(&tw, &buf);

	tw_field_i32(&tw, 1, 2);	/* version */
	pq_write_schema(w, &tw);
	tw_field_i64(&tw, 3, w->total_rows);

	tw_field_list_begin(&tw, 4, TC_STRUCT, w->nrgs);
	for (r = 0; r < w->nrgs; r++)
	{
		PqRowGroupMeta *rg = &w->rgs[r];

		tw_list_struct_begin(&tw);
		tw_field_list_begin(&tw, 1, TC_STRUCT, w->ncols);
		for (i = 0; i < w->ncols; i++)
		{
			PqColumn   *c = &w->cols[i];
			PqChunkMeta *m = &rg->cols[i];

			tw_list_struct_begin(&tw);	/* ColumnChunk */
			tw_field_i64(&tw, 2, m->offset);
			tw_field_struct_begin(&tw, 3);	/* ColumnMetaData */
			tw_field_i32(&tw, 1, c->ptype);
			tw_field_list_begin(&tw, 2, TC_I32, 2);
			tw_list_i32(&tw, PQ_ENC_PLAIN);
			tw_list_i32(&tw, PQ_ENC_RLE);
			if (c->is_list)
			{
				tw_field_list_begin(&tw, 3, TC_BINARY, 3);
				tw_list_binary(&tw, c->ec->name, c->ec->namelen);
				tw_list_binary(&tw, "list", 4);
				tw_list_binary(&tw, "element", 7);
			}
			else
			{
				tw_field_list_begin(&tw, 3, TC_BINARY, 1);
				tw_list_binary(&tw, c->ec->name, c->ec->namelen);
			}
			tw_field_i32(&tw, 4, w->codec);
			tw_field_i64(&tw, 5, m->num_values);
			tw_field_i64(&tw, 6, m->ubytes);
			tw_field_i64(&tw, 7, m->cbytes);
			tw_field_i64(&tw, 9, m->offset);
			if (m->has_stats)
			{
				tw_field_struct_begin(&tw, 12);
				tw_field_i64(&tw, 3, m->null_count);
				if (m->has_minmax)
				{
					tw_field_binary(&tw, 5, m->max, m->maxlen);
					tw_field_binary(&tw, 6, m->min, m->minlen);
				}
				tw_struct_end(&tw);
			}
			tw_struct_end(&tw); /* ColumnMetaData */
			tw_struct_end(&tw); /* ColumnChunk */
		}
		tw_field_i64(&tw, 2, rg->ubytes);
		tw_field_i64(&tw, 3, rg->num_rows);
		tw_field_i64(&tw, 5, rg->offset);
		tw_field_i64(&tw, 6, rg->cbytes);
		tw_struct_end(&tw);
	}

	tw_field_binary(&tw, 6, created_by, strlen(created_by));

	/* column_orders: TYPE_ORDER for every leaf */
	tw_field_list_begin(&tw, 7, TC_STRUCT, w->ncols);
	for (i = 0; i < w->ncols; i++)
	{
		tw_list_struct_begin(&tw);
		tw_empty_struct(&tw, 1);
		tw_struct_end(&tw);
	}
	appendStringInfoChar(&buf, 0);	/* end FileMetaData */

	export_file_write(w->file, buf.data, buf.len);
	{
		StringInfoData tail;

		initStringInfo(&tail);
		append_le32(&tail, (uint32) buf.len);
		appendBinaryStringInfo(&tail, "PAR1", 4);
		export_file_write(w->file, tail.data, tail.len);
		pfree(tail.data);
	}
	pfree(buf.data);
}

/* ----------------------------------------------------------------
 * write_parquet(query text, path text, compression text, row_group_rows int)
 * ----------------------------------------------------------------
 */
Datum
write_parquet(PG_FUNCTION_ARGS)
{
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *compression = text_to_cstring(PG_GETARG_TEXT_PP(2));
	int32		row_group_rows = PG_GETARG_INT32(3);
	char	   *fullpath;
	int			codec;
	MemoryContext cxt;
	MemoryContext rowcxt;
	MemoryContext oldcxt;
	ExportFile *volatile file = NULL;
	int64		total = 0;

	fullpath = export_check_output_path(path);

	if (pg_strcasecmp(compression, "snappy") == 0)
		codec = PQ_CODEC_SNAPPY;
	else if (pg_strcasecmp(compression, "none") == 0 ||
			 pg_strcasecmp(compression, "uncompressed") == 0)
		codec = PQ_CODEC_UNCOMPRESSED;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported Parquet compression \"%s\"", compression),
				 errhint("Supported values are \"snappy\" and \"none\".")));

	if (row_group_rows < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("row_group_rows must be at least 1")));

	cxt = AllocSetContextCreate(CurrentMemoryContext, "write_parquet",
								ALLOCSET_DEFAULT_SIZES);
	rowcxt = AllocSetContextCreate(cxt, "write_parquet rows",
								   ALLOCSET_DEFAULT_SIZES);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	oldcxt = MemoryContextSwitchTo(cxt);

	PG_TRY();
	{
		Portal		portal;
		ExportColumn *ecols;
		int			ncols;
		PqWriter   *w;
		Datum	   *values;
		bool	   *nulls;

		portal = export_open_cursor(query);
		ecols = export_describe(portal->tupDesc, &ncols);

		w = palloc0(sizeof(PqWriter));
		w->codec = codec;
		initStringInfo(&w->pagebuf);
		initStringInfo(&w->compbuf);
		initStringInfo(&w->hdrbuf);
		pq_init_columns(w, ecols, ncols);
		values = palloc(sizeof(Datum) * ncols);
		nulls = palloc(sizeof(bool) * ncols);

		file = export_file_open(fullpath);
		w->file = file;
		export_file_write(file, "PAR1", 4);

		for (;;)
		{
			SPITupleTable *tt;
			uint64		i;

			SPI_cursor_fetch(portal, true, EXPORT_FETCH_ROWS);
			if (SPI_processed == 0)
				break;
			tt = SPI_tuptable;

			for (i = 0; i < SPI_processed; i++)
			{
				int			c;

				MemoryContextSwitchTo(rowcxt);
				heap_deform_tuple(tt->vals[i], tt->tupdesc, values, nulls);
				for (c = 0; c < ncols; c++)
				{
					PqColumn   *col = &w->cols[c];

					pq_add_value(col, values[c], nulls[c]);
					if (col->values.len >= PQ_PAGE_TARGET_BYTES ||
						col->page_rows >= PQ_PAGE_MAX_ROWS)
						pq_flush_page(w, col);
				}
				MemoryContextSwitchTo(cxt);

				w->rg_rows++;
				w->total_rows++;
				if (w->rg_rows >= row_group_rows ||
					((w->rg_rows % 1024) == 0 &&
					 pq_buffered_bytes(w) >= EXPORT_MAX_BUFFERED))
					pq_flush_row_group(w);
			}
			SPI_freetuptable(tt);
			MemoryContextReset(rowcxt);
			CHECK_FOR_INTERRUPTS();
		}

		pq_flush_row_group(w);
		pq_write_footer(w);
		export_file_commit(file);
		total = w->total_rows;

		SPI_cursor_close(portal);
	}
	PG_CATCH();
	{
		export_file_abort(file);
		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	SPI_finish();
	MemoryContextDelete(cxt);

	PG_RETURN_INT64(total);
}
