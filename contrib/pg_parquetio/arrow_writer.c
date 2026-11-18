/*-------------------------------------------------------------------------
 *
 * arrow_writer.c
 *	  write_arrow(): export a query result as an Apache Arrow IPC *file*
 *	  ("Feather v2"), and arrow_ipc_stream(): return the same data as Arrow
 *	  IPC *stream* messages, one bytea per message.
 *
 *	  The IPC file format is what PyArrow (pyarrow.ipc.open_file /
 *	  pyarrow.memory_map), Polars, DuckDB, etc. can memory-map with zero
 *	  copy, so ML dataloaders can consume exports without an ETL step:
 *
 *	    "ARROW1\0\0"
 *	    Schema message
 *	    RecordBatch message*      (at most batch_rows rows each)
 *	    end-of-stream marker
 *	    Footer flatbuffer (schema + block index of record batches)
 *	    int32 footer length, "ARROW1"
 *
 *	  Every message is encapsulated as 0xFFFFFFFF, int32 metadata length,
 *	  a Message flatbuffer padded to 8 bytes, then the body; all body
 *	  buffers are 8-byte aligned.  Metadata version V5, little-endian,
 *	  no compression, no dictionaries.
 *
 *	  Type mapping:
 *	    bool -> Bool; int2/int4/int8 -> Int16/Int32/Int64;
 *	    float4/float8 -> Float32/Float64; text, varchar, bpchar -> Utf8;
 *	    bytea -> Binary; date -> Date32 (days since 1970-01-01);
 *	    timestamp -> Timestamp(MICROSECOND); timestamptz ->
 *	    Timestamp(MICROSECOND, "UTC"); uuid -> FixedSizeBinary(16) tagged
 *	    with the canonical "arrow.uuid" extension; json/jsonb -> Utf8 tagged
 *	    "arrow.json"; vector(n) -> FixedSizeList<float32>[n]; vector
 *	    without a declared dimension and float4[] -> List<float32>;
 *	    float8[] -> List<float64>; numeric and everything else -> Utf8 via
 *	    the type's output function.
 *
 *	  The original pg_cdc_arrow encoder uses a simplified,
 *	  non-flatbuffer metadata layout that standard Arrow readers cannot
 *	  parse, so this file contains a small, spec-conformant FlatBuffers
 *	  builder instead (only its buffer-management ideas were borrowed).
 *
 *	  Prior art / patent notes: Apache Arrow's columnar and IPC formats and
 *	  the FlatBuffers serialization format are published open
 *	  specifications under the Apache License 2.0 (express patent grant
 *	  from contributors).  Only the published layouts are produced here.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_parquetio/arrow_writer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "utils/uuid.h"

#include "pg_parquetio.h"
#include "export_common.h"

PG_FUNCTION_INFO_V1(write_arrow);
PG_FUNCTION_INFO_V1(arrow_ipc_stream);

/* ================================================================
 * Minimal FlatBuffers builder (builds back to front, like the reference
 * implementations).  Offsets returned are "distance from the end of the
 * buffer", which stay valid as the buffer grows at the front.
 * ================================================================ */

#define FB_MAX_FIELDS 16

typedef struct FBBuilder
{
	uint8	   *buf;
	size_t		cap;
	size_t		head;			/* data lives in buf[head .. cap) */
	size_t		minalign;
	uint32		vtable[FB_MAX_FIELDS];
	int			nfields;
	uint32		obj_end;
} FBBuilder;

static void
fb_init(FBBuilder *b)
{
	b->cap = 1024;
	b->buf = palloc0(b->cap);
	b->head = b->cap;
	b->minalign = 1;
	b->nfields = 0;
}

static inline uint32
fb_off(FBBuilder *b)
{
	return (uint32) (b->cap - b->head);
}

static void
fb_reserve(FBBuilder *b, size_t n)
{
	while (b->head < n)
	{
		size_t		used = b->cap - b->head;
		size_t		ncap = b->cap * 2;
		uint8	   *nb = palloc0(ncap);

		memcpy(nb + ncap - used, b->buf + b->head, used);
		pfree(b->buf);
		b->buf = nb;
		b->head = ncap - used;
		b->cap = ncap;
	}
}

static void
fb_pad(FBBuilder *b, size_t n)
{
	fb_reserve(b, n);
	while (n-- > 0)
		b->buf[--b->head] = 0;
}

/* Align so that after writing "additional" bytes we are "size"-aligned */
static void
fb_prep(FBBuilder *b, size_t size, size_t additional)
{
	size_t		pad;

	if (size > b->minalign)
		b->minalign = size;
	pad = (~((size_t) fb_off(b) + additional) + 1) & (size - 1);
	fb_reserve(b, pad + size + additional);
	fb_pad(b, pad);
}

static void
fb_place(FBBuilder *b, const void *data, size_t n)
{
	fb_reserve(b, n);
	b->head -= n;
	memcpy(b->buf + b->head, data, n);
}

static void
fb_put_le(FBBuilder *b, uint64 v, int size)
{
	uint8		tmp[8];
	int			i;

	for (i = 0; i < size; i++)
		tmp[i] = (uint8) ((v >> (8 * i)) & 0xff);
	fb_prep(b, size, 0);
	fb_place(b, tmp, size);
}

static void
fb_uoffset(FBBuilder *b, uint32 off)
{
	fb_prep(b, 4, 0);
	fb_put_le(b, fb_off(b) - off + 4, 4);
}

static uint32
fb_string(FBBuilder *b, const char *s, int len)
{
	uint8		zero = 0;

	fb_prep(b, 4, len + 1);
	fb_place(b, &zero, 1);
	fb_place(b, s, len);
	fb_put_le(b, (uint32) len, 4);
	return fb_off(b);
}

static uint32
fb_vec_offsets(FBBuilder *b, const uint32 *offs, int n)
{
	int			i;

	fb_prep(b, 4, 4 * (size_t) n);
	for (i = n - 1; i >= 0; i--)
		fb_uoffset(b, offs[i]);
	fb_put_le(b, (uint32) n, 4);
	return fb_off(b);
}

/* vector of structs; "data" holds n structs laid out little-endian */
static uint32
fb_vec_structs(FBBuilder *b, const uint8 *data, int n, size_t elemsize,
			   size_t align)
{
	fb_prep(b, 4, elemsize * n);
	fb_prep(b, align, elemsize * n);
	fb_place(b, data, elemsize * n);
	fb_put_le(b, (uint32) n, 4);
	return fb_off(b);
}

static void
fb_start(FBBuilder *b, int nfields)
{
	Assert(nfields <= FB_MAX_FIELDS);
	memset(b->vtable, 0, sizeof(b->vtable));
	b->nfields = nfields;
	b->obj_end = fb_off(b);
}

static void
fb_add_scalar(FBBuilder *b, int id, uint64 v, int size)
{
	fb_put_le(b, v, size);
	b->vtable[id] = fb_off(b);
}

static void
fb_add_offset(FBBuilder *b, int id, uint32 off)
{
	fb_uoffset(b, off);
	b->vtable[id] = fb_off(b);
}

static uint32
fb_end(FBBuilder *b)
{
	uint32		obj;
	uint32		vt;
	int32		so;
	int			i;

	fb_put_le(b, 0, 4);			/* placeholder for soffset to vtable */
	obj = fb_off(b);
	for (i = b->nfields - 1; i >= 0; i--)
		fb_put_le(b, b->vtable[i] ? obj - b->vtable[i] : 0, 2);
	fb_put_le(b, obj - b->obj_end, 2);
	fb_put_le(b, (uint32) (b->nfields + 2) * 2, 2);
	vt = fb_off(b);
	so = (int32) (vt - obj);
	{
		uint8	   *p = b->buf + b->cap - obj;
		uint32		u = (uint32) so;

		p[0] = u & 0xff;
		p[1] = (u >> 8) & 0xff;
		p[2] = (u >> 16) & 0xff;
		p[3] = (u >> 24) & 0xff;
	}
	return obj;
}

static void
fb_finish(FBBuilder *b, uint32 root, const uint8 **data, size_t *len)
{
	fb_prep(b, b->minalign, 4);
	fb_uoffset(b, root);
	*data = b->buf + b->head;
	*len = fb_off(b);
}

/* ================================================================
 * Arrow schema constants (Schema.fbs / Message.fbs / File.fbs)
 * ================================================================ */

#define AT_INT				2
#define AT_FLOATING_POINT	3
#define AT_BINARY			4
#define AT_UTF8				5
#define AT_BOOL				6
#define AT_DATE				8
#define AT_TIMESTAMP		10
#define AT_LIST				12
#define AT_FIXED_SIZE_BINARY 15
#define AT_FIXED_SIZE_LIST	16

#define MH_SCHEMA			1
#define MH_RECORD_BATCH		3

#define METADATA_V5			4

/* ================================================================
 * Column builders
 * ================================================================ */

typedef struct ArrowCol
{
	ExportColumn *ec;
	int			atype;			/* AT_* of the column */
	int			width;			/* value width for fixed-width data */
	bool		is_list;		/* List or FixedSizeList */
	int			child_width;	/* 4 (float32) or 8 (float64) */

	StringInfoData validity;
	StringInfoData offsets;		/* Utf8/Binary/List */
	StringInfoData data;		/* values, or child values for lists */
	StringInfoData cvalidity;	/* child validity for lists */
	int64		null_count;
	int64		cnull_count;
	int64		clength;		/* number of child values */
} ArrowCol;

typedef struct ArrowWriter
{
	int			ncols;
	ArrowCol   *cols;
	int64		nrows;			/* rows in the current batch */
} ArrowWriter;

static void
set_bit(StringInfo bm, int64 idx, bool valid)
{
	if (idx % 8 == 0)
		appendStringInfoChar(bm, 0);
	if (valid)
		bm->data[idx / 8] |= (char) (1 << (idx % 8));
}

static void
aw_reset(ArrowWriter *w)
{
	int			i;

	for (i = 0; i < w->ncols; i++)
	{
		ArrowCol   *c = &w->cols[i];

		resetStringInfo(&c->validity);
		resetStringInfo(&c->offsets);
		resetStringInfo(&c->data);
		resetStringInfo(&c->cvalidity);
		c->null_count = 0;
		c->cnull_count = 0;
		c->clength = 0;
		if (c->atype == AT_UTF8 || c->atype == AT_BINARY || c->atype == AT_LIST)
			append_le32(&c->offsets, 0);
	}
	w->nrows = 0;
}

static ArrowWriter *
aw_create(ExportColumn *ecols, int ncols)
{
	ArrowWriter *w = palloc0(sizeof(ArrowWriter));
	int			i;

	w->ncols = ncols;
	w->cols = palloc0(sizeof(ArrowCol) * ncols);
	for (i = 0; i < ncols; i++)
	{
		ArrowCol   *c = &w->cols[i];

		c->ec = &ecols[i];
		switch (ecols[i].kind)
		{
			case EK_BOOL:
				c->atype = AT_BOOL;
				break;
			case EK_INT16:
				c->atype = AT_INT;
				c->width = 2;
				break;
			case EK_INT32:
				c->atype = AT_INT;
				c->width = 4;
				break;
			case EK_INT64:
				c->atype = AT_INT;
				c->width = 8;
				break;
			case EK_FLOAT4:
				c->atype = AT_FLOATING_POINT;
				c->width = 4;
				break;
			case EK_FLOAT8:
				c->atype = AT_FLOATING_POINT;
				c->width = 8;
				break;
			case EK_DATE:
				c->atype = AT_DATE;
				c->width = 4;
				break;
			case EK_TIMESTAMP:
			case EK_TIMESTAMPTZ:
				c->atype = AT_TIMESTAMP;
				c->width = 8;
				break;
			case EK_UUID:
				c->atype = AT_FIXED_SIZE_BINARY;
				c->width = UUID_LEN;
				break;
			case EK_BYTEA:
				c->atype = AT_BINARY;
				break;
			case EK_TEXT:
			case EK_JSON:
			case EK_OUTPUT_TEXT:
				c->atype = AT_UTF8;
				break;
			case EK_VECTOR:
				c->is_list = true;
				c->child_width = 4;
				c->atype = ecols[i].fixed_dim > 0 ? AT_FIXED_SIZE_LIST : AT_LIST;
				break;
			case EK_FLOAT4_ARRAY:
				c->is_list = true;
				c->child_width = 4;
				c->atype = AT_LIST;
				break;
			case EK_FLOAT8_ARRAY:
				c->is_list = true;
				c->child_width = 8;
				c->atype = AT_LIST;
				break;
		}
		initStringInfo(&c->validity);
		initStringInfo(&c->offsets);
		initStringInfo(&c->data);
		initStringInfo(&c->cvalidity);
	}
	aw_reset(w);
	return w;
}

static void
aw_append_zeros(StringInfo s, int n)
{
	enlargeStringInfo(s, n);
	memset(s->data + s->len, 0, n);
	s->len += n;
	s->data[s->len] = '\0';
}

static void
aw_add_value(ArrowCol *c, int64 row, Datum d, bool isnull)
{
	ExportColumn *ec = c->ec;

	set_bit(&c->validity, row, !isnull);
	if (isnull)
		c->null_count++;

	switch (c->atype)
	{
		case AT_BOOL:
			set_bit(&c->data, row, !isnull && DatumGetBool(d));
			break;
		case AT_INT:
		case AT_FLOATING_POINT:
		case AT_DATE:
		case AT_TIMESTAMP:
			if (isnull)
				aw_append_zeros(&c->data, c->width);
			else
				switch (ec->kind)
				{
					case EK_INT16:
						append_le16(&c->data, (uint16) DatumGetInt16(d));
						break;
					case EK_INT32:
						append_le32(&c->data, (uint32) DatumGetInt32(d));
						break;
					case EK_INT64:
						append_le64(&c->data, (uint64) DatumGetInt64(d));
						break;
					case EK_FLOAT4:
						append_f32(&c->data, DatumGetFloat4(d));
						break;
					case EK_FLOAT8:
						append_f64(&c->data, DatumGetFloat8(d));
						break;
					case EK_DATE:
						append_le32(&c->data, (uint32) export_date_value(d));
						break;
					default:
						append_le64(&c->data, (uint64) export_timestamp_value(d));
						break;
				}
			break;
		case AT_FIXED_SIZE_BINARY:
			if (isnull)
				aw_append_zeros(&c->data, UUID_LEN);
			else
				appendBinaryStringInfo(&c->data,
									   (const char *) DatumGetUUIDP(d)->data,
									   UUID_LEN);
			break;
		case AT_UTF8:
		case AT_BINARY:
			if (!isnull)
			{
				int			len;
				const char *s = export_text_value(ec, d, &len);

				appendBinaryStringInfo(&c->data, s, len);
			}
			if (c->data.len > PG_INT32_MAX)
				elog(ERROR, "Arrow string buffer overflow");
			append_le32(&c->offsets, (uint32) c->data.len);
			break;
		case AT_FIXED_SIZE_LIST:
			{
				int			dim = ec->fixed_dim;
				int			i;

				if (isnull)
				{
					aw_append_zeros(&c->data, 4 * dim);
					for (i = 0; i < dim; i++)
						set_bit(&c->cvalidity, c->clength + i, true);
				}
				else
				{
					int			vdim;
					const float *x = export_vector_value(ec, d, &vdim);

					append_f32_array(&c->data, x, vdim);
					for (i = 0; i < vdim; i++)
						set_bit(&c->cvalidity, c->clength + i, true);
				}
				c->clength += dim;
				break;
			}
		case AT_LIST:
			if (!isnull && ec->kind == EK_VECTOR)
			{
				int			vdim;
				const float *x = export_vector_value(ec, d, &vdim);
				int			i;

				append_f32_array(&c->data, x, vdim);
				for (i = 0; i < vdim; i++)
					set_bit(&c->cvalidity, c->clength + i, true);
				c->clength += vdim;
			}
			else if (!isnull)
			{
				Datum	   *elems;
				bool	   *nulls;
				int			n = export_float_array_value(ec, d, &elems, &nulls);
				int			i;

				for (i = 0; i < n; i++)
				{
					set_bit(&c->cvalidity, c->clength + i, !nulls[i]);
					if (nulls[i])
					{
						c->cnull_count++;
						aw_append_zeros(&c->data, c->child_width);
					}
					else if (c->child_width == 4)
						append_f32(&c->data, DatumGetFloat4(elems[i]));
					else
						append_f64(&c->data, DatumGetFloat8(elems[i]));
				}
				c->clength += n;
			}
			if (c->clength > PG_INT32_MAX)
				elog(ERROR, "Arrow list offset overflow");
			append_le32(&c->offsets, (uint32) c->clength);
			break;
	}
}

static Size
aw_buffered_bytes(ArrowWriter *w)
{
	Size		t = 0;
	int			i;

	for (i = 0; i < w->ncols; i++)
		t += w->cols[i].data.len + w->cols[i].offsets.len;
	return t;
}

/* ================================================================
 * Flatbuffer construction of schema / record batch / footer
 * ================================================================ */

static uint32
build_type(FBBuilder *b, ArrowCol *c, int atype, int child_width)
{
	switch (atype)
	{
		case AT_INT:
			fb_start(b, 2);
			fb_add_scalar(b, 0, (uint32) (c->width * 8), 4);
			fb_add_scalar(b, 1, 1, 1);
			return fb_end(b);
		case AT_FLOATING_POINT:
			{
				int			w = child_width ? child_width : c->width;

				fb_start(b, 1);
				fb_add_scalar(b, 0, w == 4 ? 1 : 2, 2);	/* SINGLE / DOUBLE */
				return fb_end(b);
			}
		case AT_DATE:
			fb_start(b, 1);
			fb_add_scalar(b, 0, 0, 2);	/* DAY */
			return fb_end(b);
		case AT_TIMESTAMP:
			{
				uint32		tz = 0;

				if (c->ec->kind == EK_TIMESTAMPTZ)
					tz = fb_string(b, "UTC", 3);
				fb_start(b, 2);
				fb_add_scalar(b, 0, 2, 2);	/* MICROSECOND */
				if (tz)
					fb_add_offset(b, 1, tz);
				return fb_end(b);
			}
		case AT_FIXED_SIZE_BINARY:
			fb_start(b, 1);
			fb_add_scalar(b, 0, UUID_LEN, 4);
			return fb_end(b);
		case AT_FIXED_SIZE_LIST:
			fb_start(b, 1);
			fb_add_scalar(b, 0, (uint32) c->ec->fixed_dim, 4);
			return fb_end(b);
		default:
			/* Bool, Utf8, Binary, List: empty tables */
			fb_start(b, 0);
			return fb_end(b);
	}
}

static uint32
build_kv(FBBuilder *b, const char *k, const char *v)
{
	uint32		ko = fb_string(b, k, strlen(k));
	uint32		vo = fb_string(b, v, strlen(v));

	fb_start(b, 2);
	fb_add_offset(b, 0, ko);
	fb_add_offset(b, 1, vo);
	return fb_end(b);
}

static uint32
build_field(FBBuilder *b, ArrowCol *c)
{
	uint32		name;
	uint32		type;
	uint32		children;
	uint32		meta = 0;

	if (c->is_list)
	{
		/* child field "item" */
		uint32		cname = fb_string(b, "item", 4);
		uint32		ctype = build_type(b, c, AT_FLOATING_POINT, c->child_width);
		uint32		cchildren = fb_vec_offsets(b, NULL, 0);
		uint32		child;

		fb_start(b, 7);
		fb_add_offset(b, 0, cname);
		fb_add_scalar(b, 1, 1, 1);
		fb_add_scalar(b, 2, AT_FLOATING_POINT, 1);
		fb_add_offset(b, 3, ctype);
		fb_add_offset(b, 5, cchildren);
		child = fb_end(b);
		children = fb_vec_offsets(b, &child, 1);
	}
	else
		children = fb_vec_offsets(b, NULL, 0);

	if (c->ec->kind == EK_UUID || c->ec->kind == EK_JSON)
	{
		uint32		kv[2];

		kv[0] = build_kv(b, "ARROW:extension:name",
						 c->ec->kind == EK_UUID ? "arrow.uuid" : "arrow.json");
		kv[1] = build_kv(b, "ARROW:extension:metadata", "");
		meta = fb_vec_offsets(b, kv, 2);
	}

	name = fb_string(b, c->ec->name, c->ec->namelen);
	type = build_type(b, c, c->atype, 0);

	fb_start(b, 7);
	fb_add_offset(b, 0, name);
	fb_add_scalar(b, 1, 1, 1);	/* nullable */
	fb_add_scalar(b, 2, (uint64) c->atype, 1);
	fb_add_offset(b, 3, type);
	fb_add_offset(b, 5, children);
	if (meta)
		fb_add_offset(b, 6, meta);
	return fb_end(b);
}

static uint32
build_schema(FBBuilder *b, ArrowWriter *w)
{
	uint32	   *fields = palloc(sizeof(uint32) * w->ncols);
	uint32		fvec;
	int			i;

	for (i = 0; i < w->ncols; i++)
		fields[i] = build_field(b, &w->cols[i]);
	fvec = fb_vec_offsets(b, fields, w->ncols);
	pfree(fields);

	fb_start(b, 2);
	fb_add_scalar(b, 0, 0, 2);	/* Little endian */
	fb_add_offset(b, 1, fvec);
	return fb_end(b);
}

static uint32
build_message(FBBuilder *b, int header_type, uint32 header, int64 body_len)
{
	fb_start(b, 4);
	fb_add_scalar(b, 3, (uint64) body_len, 8);
	fb_add_offset(b, 2, header);
	fb_add_scalar(b, 0, METADATA_V5, 2);
	fb_add_scalar(b, 1, (uint64) header_type, 1);
	return fb_end(b);
}

/* Wrap a finished flatbuffer as an encapsulated IPC message prefix */
static void
encapsulate(StringInfo out, const uint8 *fb, size_t fblen)
{
	size_t		padded = TYPEALIGN(8, fblen);

	append_le32(out, 0xFFFFFFFF);
	append_le32(out, (uint32) padded);
	appendBinaryStringInfo(out, (const char *) fb, fblen);
	aw_append_zeros(out, (int) (padded - fblen));
}

static void
emit_schema_message(ArrowWriter *w, StringInfo out)
{
	FBBuilder	b;
	const uint8 *data;
	size_t		len;
	uint32		schema;

	fb_init(&b);
	schema = build_schema(&b, w);
	fb_finish(&b, build_message(&b, MH_SCHEMA, schema, 0), &data, &len);
	encapsulate(out, data, len);
	pfree(b.buf);
}

/* body buffer bookkeeping */
typedef struct BufDesc
{
	int64		offset;
	int64		length;
} BufDesc;

typedef struct BatchLayout
{
	BufDesc    *bufs;
	int			nbufs;
	int64	   *nodes;			/* pairs (length, null_count) */
	int			nnodes;
	int64		body_len;
} BatchLayout;

static void
layout_add_buf(BatchLayout *l, StringInfo body, const char *data, int64 len)
{
	l->bufs[l->nbufs].offset = body->len;
	l->bufs[l->nbufs].length = len;
	l->nbufs++;
	if (len > 0)
	{
		appendBinaryStringInfo(body, data, (int) len);
		aw_append_zeros(body, (int) (TYPEALIGN(8, len) - len));
	}
}

static void
layout_add_node(BatchLayout *l, int64 length, int64 nulls)
{
	l->nodes[2 * l->nnodes] = length;
	l->nodes[2 * l->nnodes + 1] = nulls;
	l->nnodes++;
}

/*
 * Append one RecordBatch message (metadata + body) to "out".  Returns the
 * metadata block length (prefix + flatbuffer + padding) and body length.
 */
static void
emit_batch_message(ArrowWriter *w, StringInfo out, int32 *meta_len,
				   int64 *body_len)
{
	BatchLayout l;
	StringInfoData body;
	FBBuilder	b;
	uint8	   *raw;
	uint32		nodes_vec,
				bufs_vec,
				rb;
	const uint8 *data;
	size_t		len;
	int			i;
	int			start = out->len;

	l.bufs = palloc(sizeof(BufDesc) * w->ncols * 4);
	l.nodes = palloc(sizeof(int64) * w->ncols * 4);
	l.nbufs = 0;
	l.nnodes = 0;
	initStringInfo(&body);

	for (i = 0; i < w->ncols; i++)
	{
		ArrowCol   *c = &w->cols[i];

		layout_add_node(&l, w->nrows, c->null_count);
		/* validity may be omitted when there are no nulls */
		if (c->null_count > 0)
			layout_add_buf(&l, &body, c->validity.data, c->validity.len);
		else
			layout_add_buf(&l, &body, NULL, 0);

		switch (c->atype)
		{
			case AT_UTF8:
			case AT_BINARY:
				layout_add_buf(&l, &body, c->offsets.data, c->offsets.len);
				layout_add_buf(&l, &body, c->data.data, c->data.len);
				break;
			case AT_LIST:
			case AT_FIXED_SIZE_LIST:
				if (c->atype == AT_LIST)
					layout_add_buf(&l, &body, c->offsets.data, c->offsets.len);
				layout_add_node(&l, c->clength, c->cnull_count);
				if (c->cnull_count > 0)
					layout_add_buf(&l, &body, c->cvalidity.data, c->cvalidity.len);
				else
					layout_add_buf(&l, &body, NULL, 0);
				layout_add_buf(&l, &body, c->data.data, c->data.len);
				break;
			default:
				layout_add_buf(&l, &body, c->data.data, c->data.len);
				break;
		}
	}
	l.body_len = body.len;

	fb_init(&b);

	/* FieldNode structs: {length: long, null_count: long} */
	raw = palloc(16 * Max(l.nnodes, 1));
	for (i = 0; i < l.nnodes; i++)
	{
		int			k;

		for (k = 0; k < 8; k++)
		{
			raw[16 * i + k] = (uint8) ((uint64) l.nodes[2 * i] >> (8 * k));
			raw[16 * i + 8 + k] = (uint8) ((uint64) l.nodes[2 * i + 1] >> (8 * k));
		}
	}
	nodes_vec = fb_vec_structs(&b, raw, l.nnodes, 16, 8);
	pfree(raw);

	/* Buffer structs: {offset: long, length: long} */
	raw = palloc(16 * Max(l.nbufs, 1));
	for (i = 0; i < l.nbufs; i++)
	{
		int			k;

		for (k = 0; k < 8; k++)
		{
			raw[16 * i + k] = (uint8) ((uint64) l.bufs[i].offset >> (8 * k));
			raw[16 * i + 8 + k] = (uint8) ((uint64) l.bufs[i].length >> (8 * k));
		}
	}
	bufs_vec = fb_vec_structs(&b, raw, l.nbufs, 16, 8);
	pfree(raw);

	fb_start(&b, 3);
	fb_add_scalar(&b, 0, (uint64) w->nrows, 8);
	fb_add_offset(&b, 1, nodes_vec);
	fb_add_offset(&b, 2, bufs_vec);
	rb = fb_end(&b);

	fb_finish(&b, build_message(&b, MH_RECORD_BATCH, rb, l.body_len),
			  &data, &len);
	encapsulate(out, data, len);
	*meta_len = out->len - start;
	appendBinaryStringInfo(out, body.data, body.len);
	*body_len = body.len;

	pfree(b.buf);
	pfree(body.data);
	pfree(l.bufs);
	pfree(l.nodes);
}

static void
emit_eos(StringInfo out)
{
	append_le32(out, 0xFFFFFFFF);
	append_le32(out, 0);
}

/* Footer block index entry */
typedef struct ArrowBlock
{
	int64		offset;
	int32		meta_len;
	int64		body_len;
} ArrowBlock;

static void
emit_footer(ArrowWriter *w, StringInfo out, ArrowBlock *blocks, int nblocks)
{
	FBBuilder	b;
	uint32		schema,
				dicts,
				batches,
				footer;
	uint8	   *raw;
	const uint8 *data;
	size_t		len;
	int			i;

	fb_init(&b);
	schema = build_schema(&b, w);

	/* Block struct: {offset: long, metaDataLength: int, pad, bodyLength: long} */
	raw = palloc0(24 * Max(nblocks, 1));
	for (i = 0; i < nblocks; i++)
	{
		int			k;

		for (k = 0; k < 8; k++)
		{
			raw[24 * i + k] = (uint8) ((uint64) blocks[i].offset >> (8 * k));
			raw[24 * i + 16 + k] = (uint8) ((uint64) blocks[i].body_len >> (8 * k));
		}
		for (k = 0; k < 4; k++)
			raw[24 * i + 8 + k] = (uint8) ((uint32) blocks[i].meta_len >> (8 * k));
	}
	batches = fb_vec_structs(&b, raw, nblocks, 24, 8);
	dicts = fb_vec_structs(&b, raw, 0, 24, 8);
	pfree(raw);

	fb_start(&b, 4);
	fb_add_offset(&b, 1, schema);
	fb_add_offset(&b, 2, dicts);
	fb_add_offset(&b, 3, batches);
	fb_add_scalar(&b, 0, METADATA_V5, 2);
	footer = fb_end(&b);
	fb_finish(&b, footer, &data, &len);

	appendBinaryStringInfo(out, (const char *) data, len);
	append_le32(out, (uint32) len);
	appendBinaryStringInfo(out, "ARROW1", 6);
	pfree(b.buf);
}

/* ================================================================
 * Shared query driver
 * ================================================================ */

typedef struct ArrowSink
{
	/* file mode */
	ExportFile *file;
	ArrowBlock *blocks;
	int			nblocks;
	int			blocks_cap;
	/* stream mode */
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
} ArrowSink;

static void
sink_emit(ArrowSink *s, StringInfo msg)
{
	if (s->file)
		export_file_write(s->file, msg->data, msg->len);
	else
	{
		bytea	   *v = palloc(VARHDRSZ + msg->len);
		Datum		val;
		bool		isnull = false;

		SET_VARSIZE(v, VARHDRSZ + msg->len);
		memcpy(VARDATA(v), msg->data, msg->len);
		val = PointerGetDatum(v);
		tuplestore_putvalues(s->tupstore, s->tupdesc, &val, &isnull);
		pfree(v);
	}
	resetStringInfo(msg);
}

static void
flush_batch(ArrowWriter *w, ArrowSink *s, StringInfo msg)
{
	int32		meta_len;
	int64		body_len;

	if (w->nrows == 0)
		return;

	if (s->file)
	{
		ArrowBlock *blk;

		if (s->nblocks >= s->blocks_cap)
		{
			s->blocks_cap = s->blocks_cap ? s->blocks_cap * 2 : 64;
			s->blocks = s->blocks
				? repalloc(s->blocks, sizeof(ArrowBlock) * s->blocks_cap)
				: palloc(sizeof(ArrowBlock) * s->blocks_cap);
		}
		blk = &s->blocks[s->nblocks++];
		blk->offset = (int64) s->file->offset;
		emit_batch_message(w, msg, &meta_len, &body_len);
		blk->meta_len = meta_len;
		blk->body_len = body_len;
	}
	else
		emit_batch_message(w, msg, &meta_len, &body_len);

	sink_emit(s, msg);
	aw_reset(w);
}

/*
 * Run the query and feed every row into Arrow batches written to the sink.
 * Caller is SPI-connected and has set up memory contexts.
 */
static int64
arrow_run_query(const char *query, int32 batch_rows, ArrowSink *s,
				MemoryContext cxt, MemoryContext rowcxt)
{
	Portal		portal;
	ExportColumn *ecols;
	int			ncols;
	ArrowWriter *w;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData msg;
	int64		total = 0;

	portal = export_open_cursor(query);
	ecols = export_describe(portal->tupDesc, &ncols);
	w = aw_create(ecols, ncols);
	values = palloc(sizeof(Datum) * ncols);
	nulls = palloc(sizeof(bool) * ncols);
	initStringInfo(&msg);

	if (s->file)
		appendBinaryStringInfo(&msg, "ARROW1\0\0", 8);
	emit_schema_message(w, &msg);
	sink_emit(s, &msg);

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
				aw_add_value(&w->cols[c], w->nrows, values[c], nulls[c]);
			MemoryContextSwitchTo(cxt);
			w->nrows++;
			total++;
			if (w->nrows >= batch_rows ||
				((w->nrows % 1024) == 0 &&
				 aw_buffered_bytes(w) >= EXPORT_MAX_BUFFERED))
				flush_batch(w, s, &msg);
		}
		SPI_freetuptable(tt);
		MemoryContextReset(rowcxt);
		CHECK_FOR_INTERRUPTS();
	}
	flush_batch(w, s, &msg);

	emit_eos(&msg);
	if (s->file)
		emit_footer(w, &msg, s->blocks, s->nblocks);
	sink_emit(s, &msg);

	SPI_cursor_close(portal);
	return total;
}

static void
check_batch_rows(int32 batch_rows)
{
	if (batch_rows < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("batch_rows must be at least 1")));
}

/*
 * write_arrow(query text, path text, batch_rows int) RETURNS bigint
 */
Datum
write_arrow(PG_FUNCTION_ARGS)
{
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int32		batch_rows = PG_GETARG_INT32(2);
	char	   *fullpath;
	MemoryContext cxt;
	MemoryContext rowcxt;
	MemoryContext oldcxt;
	ExportFile *volatile file = NULL;
	int64		total = 0;

	fullpath = export_check_output_path(path);
	check_batch_rows(batch_rows);

	cxt = AllocSetContextCreate(CurrentMemoryContext, "write_arrow",
								ALLOCSET_DEFAULT_SIZES);
	rowcxt = AllocSetContextCreate(cxt, "write_arrow rows",
								   ALLOCSET_DEFAULT_SIZES);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	oldcxt = MemoryContextSwitchTo(cxt);

	PG_TRY();
	{
		ArrowSink	sink;

		memset(&sink, 0, sizeof(sink));
		file = export_file_open(fullpath);
		sink.file = file;
		total = arrow_run_query(query, batch_rows, &sink, cxt, rowcxt);
		export_file_commit(file);
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

/*
 * arrow_ipc_stream(query text, batch_rows int) RETURNS SETOF bytea
 *
 * Returns the Arrow IPC stream: schema message, one row per record batch,
 * and the end-of-stream marker.  Concatenating the rows in order yields a
 * valid stream for pyarrow.ipc.open_stream() and friends.  No file access,
 * so no special privileges are required; the query runs as the caller.
 */
Datum
arrow_ipc_stream(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		batch_rows = PG_GETARG_INT32(1);
	MemoryContext cxt;
	MemoryContext rowcxt;
	MemoryContext oldcxt;
	ArrowSink	sink;

	check_batch_rows(batch_rows);
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	cxt = AllocSetContextCreate(CurrentMemoryContext, "arrow_ipc_stream",
								ALLOCSET_DEFAULT_SIZES);
	rowcxt = AllocSetContextCreate(cxt, "arrow_ipc_stream rows",
								   ALLOCSET_DEFAULT_SIZES);

	memset(&sink, 0, sizeof(sink));
	sink.tupstore = rsinfo->setResult;
	sink.tupdesc = rsinfo->setDesc;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	oldcxt = MemoryContextSwitchTo(cxt);

	(void) arrow_run_query(query, batch_rows, &sink, cxt, rowcxt);

	MemoryContextSwitchTo(oldcxt);
	SPI_finish();
	MemoryContextDelete(cxt);

	return (Datum) 0;
}
