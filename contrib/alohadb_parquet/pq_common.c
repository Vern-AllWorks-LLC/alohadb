/*-------------------------------------------------------------------------
 *
 * pq_common.c
 *	  Thrift compact-protocol codec and Snappy block codec shared by the
 *	  alohadb_parquet Parquet reader and writer.
 *
 *	  Thrift compact protocol: Apache Thrift specification (Apache-2.0),
 *	  as used by the Apache Parquet file format (Apache-2.0) for page
 *	  headers and the FileMetaData footer.
 *
 *	  Snappy: the raw block format documented in Google's open
 *	  format_description.txt (BSD-licensed reference implementation).  The
 *	  compressor below is a straightforward greedy LZ77-style matcher using
 *	  a single hash table of 4-byte prefixes (LZ77: Ziv & Lempel, 1977,
 *	  long-expired prior art).  It emits only literal and 2-byte-offset copy
 *	  elements, which every conforming Snappy decoder accepts.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/pq_common.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pq_common.h"

#define TR_MAX_DEPTH 64

static void
tr_corrupt(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("corrupt Parquet metadata (Thrift decoding failed)")));
}

/* ================================================================
 * Thrift compact decoder
 * ================================================================ */

uint64
tr_varint(ThriftReader *r)
{
	uint64		result = 0;
	int			shift = 0;

	for (;;)
	{
		uint8		b;

		if (r->pos >= r->len || shift > 63)
			tr_corrupt();
		b = r->data[r->pos++];
		result |= (uint64) (b & 0x7f) << shift;
		if ((b & 0x80) == 0)
			break;
		shift += 7;
	}
	return result;
}

int64
tr_zigzag(ThriftReader *r)
{
	uint64		n = tr_varint(r);

	return (int64) ((n >> 1) ^ (~(n & 1) + 1));
}

/*
 * Read the next field header of a struct.  Returns false at the STOP field.
 */
bool
tr_field(ThriftReader *r, int *last_fid, int *fid, int *ftype)
{
	uint8		h;
	int			delta;

	if (r->pos >= r->len)
		tr_corrupt();
	h = r->data[r->pos++];
	if (h == 0)
		return false;
	*ftype = h & 0x0f;
	delta = (h >> 4) & 0x0f;
	if (delta != 0)
		*fid = *last_fid + delta;
	else
		*fid = (int) tr_zigzag(r);
	*last_fid = *fid;
	return true;
}

void
tr_list_header(ThriftReader *r, int *elemtype, int *count)
{
	uint8		h;
	uint64		n;

	if (r->pos >= r->len)
		tr_corrupt();
	h = r->data[r->pos++];
	*elemtype = h & 0x0f;
	n = (h >> 4) & 0x0f;
	if (n == 15)
		n = tr_varint(r);
	/* each element needs at least one byte (except bools packed in header) */
	if (n > r->len - r->pos + 1 && n > 0)
		tr_corrupt();
	*count = (int) n;
}

/* Returns a palloc'd, NUL-terminated copy */
char *
tr_binary(ThriftReader *r, int *len)
{
	uint64		n = tr_varint(r);
	char	   *s;

	if (n > r->len - r->pos)
		tr_corrupt();
	s = palloc(n + 1);
	memcpy(s, r->data + r->pos, n);
	s[n] = '\0';
	r->pos += n;
	if (len)
		*len = (int) n;
	return s;
}

/* Value of a bool struct field (encoded in the field header) */
bool
tr_bool(ThriftReader *r, int ftype)
{
	if (ftype == TC_BOOL_TRUE)
		return true;
	if (ftype == TC_BOOL_FALSE)
		return false;
	tr_skip(r, ftype, 0);
	return false;
}

/*
 * Skip a value of the given type.  For struct fields, bool values live in
 * the field header and consume nothing; inside lists/sets/maps a bool is
 * one byte.  "depth" guards against malicious nesting.
 */
static void
tr_skip_elem(ThriftReader *r, int etype, int depth)
{
	if (etype == TC_BOOL_TRUE || etype == TC_BOOL_FALSE)
	{
		if (r->pos >= r->len)
			tr_corrupt();
		r->pos++;
	}
	else
		tr_skip(r, etype, depth);
}

void
tr_skip(ThriftReader *r, int ftype, int depth)
{
	if (depth > TR_MAX_DEPTH)
		tr_corrupt();

	switch (ftype)
	{
		case TC_BOOL_TRUE:
		case TC_BOOL_FALSE:
			break;
		case TC_BYTE:
			if (r->pos >= r->len)
				tr_corrupt();
			r->pos++;
			break;
		case TC_I16:
		case TC_I32:
		case TC_I64:
			(void) tr_varint(r);
			break;
		case TC_DOUBLE:
			if (r->len - r->pos < 8)
				tr_corrupt();
			r->pos += 8;
			break;
		case TC_BINARY:
			{
				uint64		n = tr_varint(r);

				if (n > r->len - r->pos)
					tr_corrupt();
				r->pos += n;
				break;
			}
		case TC_LIST:
		case TC_SET:
			{
				int			et,
							n,
							i;

				tr_list_header(r, &et, &n);
				for (i = 0; i < n; i++)
					tr_skip_elem(r, et, depth + 1);
				break;
			}
		case TC_MAP:
			{
				uint64		n = tr_varint(r);
				int			kt,
							vt;
				uint64		i;

				if (n == 0)
					break;
				if (r->pos >= r->len || n > r->len)
					tr_corrupt();
				kt = (r->data[r->pos] >> 4) & 0x0f;
				vt = r->data[r->pos] & 0x0f;
				r->pos++;
				for (i = 0; i < n; i++)
				{
					tr_skip_elem(r, kt, depth + 1);
					tr_skip_elem(r, vt, depth + 1);
				}
				break;
			}
		case TC_STRUCT:
			{
				int			last = 0,
							fid,
							ft;

				while (tr_field(r, &last, &fid, &ft))
					tr_skip(r, ft, depth + 1);
				break;
			}
		default:
			tr_corrupt();
	}
}

/* ================================================================
 * Thrift compact encoder
 * ================================================================ */

static void
tw_varint(StringInfo buf, uint64 v)
{
	while (v >= 0x80)
	{
		appendStringInfoChar(buf, (char) ((v & 0x7f) | 0x80));
		v >>= 7;
	}
	appendStringInfoChar(buf, (char) v);
}

static void
tw_zigzag(StringInfo buf, int64 v)
{
	tw_varint(buf, ((uint64) v << 1) ^ (uint64) (v >> 63));
}

void
tw_init(ThriftWriter *w, StringInfo buf)
{
	w->buf = buf;
	w->depth = 0;
	w->last_fid[0] = 0;
}

static void
tw_field_header(ThriftWriter *w, int fid, int ftype)
{
	int			delta = fid - w->last_fid[w->depth];

	if (delta > 0 && delta <= 15)
		appendStringInfoChar(w->buf, (char) ((delta << 4) | ftype));
	else
	{
		appendStringInfoChar(w->buf, (char) ftype);
		tw_zigzag(w->buf, fid);
	}
	w->last_fid[w->depth] = (int16) fid;
}

void
tw_field_i32(ThriftWriter *w, int fid, int32 v)
{
	tw_field_header(w, fid, TC_I32);
	tw_zigzag(w->buf, v);
}

void
tw_field_i64(ThriftWriter *w, int fid, int64 v)
{
	tw_field_header(w, fid, TC_I64);
	tw_zigzag(w->buf, v);
}

void
tw_field_bool(ThriftWriter *w, int fid, bool v)
{
	tw_field_header(w, fid, v ? TC_BOOL_TRUE : TC_BOOL_FALSE);
}

void
tw_field_byte(ThriftWriter *w, int fid, int8 v)
{
	tw_field_header(w, fid, TC_BYTE);
	appendStringInfoChar(w->buf, (char) v);
}

void
tw_field_binary(ThriftWriter *w, int fid, const char *data, int len)
{
	tw_field_header(w, fid, TC_BINARY);
	tw_varint(w->buf, (uint64) len);
	appendBinaryStringInfo(w->buf, data, len);
}

static void
tw_push(ThriftWriter *w)
{
	if (w->depth + 1 >= TW_MAX_DEPTH)
		elog(ERROR, "Thrift structure nesting too deep");
	w->depth++;
	w->last_fid[w->depth] = 0;
}

void
tw_field_struct_begin(ThriftWriter *w, int fid)
{
	tw_field_header(w, fid, TC_STRUCT);
	tw_push(w);
}

void
tw_field_list_begin(ThriftWriter *w, int fid, int elemtype, int count)
{
	tw_field_header(w, fid, TC_LIST);
	if (count < 15)
		appendStringInfoChar(w->buf, (char) ((count << 4) | elemtype));
	else
	{
		appendStringInfoChar(w->buf, (char) (0xf0 | elemtype));
		tw_varint(w->buf, (uint64) count);
	}
}

/* Start a struct that is an element of a list */
void
tw_list_struct_begin(ThriftWriter *w)
{
	tw_push(w);
}

void
tw_list_i32(ThriftWriter *w, int32 v)
{
	tw_zigzag(w->buf, v);
}

void
tw_list_binary(ThriftWriter *w, const char *data, int len)
{
	tw_varint(w->buf, (uint64) len);
	appendBinaryStringInfo(w->buf, data, len);
}

void
tw_struct_end(ThriftWriter *w)
{
	appendStringInfoChar(w->buf, 0);	/* STOP */
	Assert(w->depth > 0);
	w->depth--;
}

/* ================================================================
 * Snappy
 * ================================================================ */

static size_t
snappy_varint32(const uint8 *src, size_t len, uint32 *out)
{
	uint32		result = 0;
	size_t		pos = 0;
	int			shift = 0;

	while (pos < len && shift <= 28)
	{
		uint8		b = src[pos++];

		result |= (uint32) (b & 0x7f) << shift;
		if ((b & 0x80) == 0)
		{
			*out = result;
			return pos;
		}
		shift += 7;
	}
	return 0;
}

bool
snappy_uncompressed_length(const uint8 *src, size_t src_len, uint32 *result)
{
	return snappy_varint32(src, src_len, result) > 0;
}

/*
 * Decompress a raw Snappy block into dst, which must be exactly dst_len
 * bytes (the length announced in the block preamble).  Returns false on
 * any malformed input.
 */
bool
snappy_decompress(const uint8 *src, size_t src_len, uint8 *dst, size_t dst_len)
{
	uint32		ulen;
	size_t		ip;
	size_t		op = 0;

	ip = snappy_varint32(src, src_len, &ulen);
	if (ip == 0 || ulen != dst_len)
		return false;

	while (ip < src_len)
	{
		uint8		tag = src[ip++];
		size_t		len;
		size_t		offset;

		switch (tag & 3)
		{
			case 0:				/* literal */
				len = (tag >> 2) + 1;
				if (len > 60)
				{
					int			nb = (int) len - 60;
					int			i;

					if (src_len - ip < (size_t) nb)
						return false;
					len = 0;
					for (i = 0; i < nb; i++)
						len |= (size_t) src[ip + i] << (8 * i);
					len += 1;
					ip += nb;
				}
				if (src_len - ip < len || dst_len - op < len)
					return false;
				memcpy(dst + op, src + ip, len);
				ip += len;
				op += len;
				continue;
			case 1:
				if (ip >= src_len)
					return false;
				len = 4 + ((tag >> 2) & 7);
				offset = ((size_t) (tag >> 5) << 8) | src[ip];
				ip += 1;
				break;
			case 2:
				if (src_len - ip < 2)
					return false;
				len = 1 + (tag >> 2);
				offset = (size_t) src[ip] | ((size_t) src[ip + 1] << 8);
				ip += 2;
				break;
			default:
				if (src_len - ip < 4)
					return false;
				len = 1 + (tag >> 2);
				offset = (size_t) src[ip] | ((size_t) src[ip + 1] << 8) |
					((size_t) src[ip + 2] << 16) | ((size_t) src[ip + 3] << 24);
				ip += 4;
				break;
		}
		if (offset == 0 || offset > op || dst_len - op < len)
			return false;
		/* byte-by-byte: source and destination may overlap */
		{
			uint8	   *d = dst + op;
			const uint8 *s = d - offset;
			size_t		i;

			for (i = 0; i < len; i++)
				d[i] = s[i];
		}
		op += len;
	}
	return op == dst_len;
}

static void
snappy_emit_literal(StringInfo dst, const uint8 *lit, size_t len)
{
	size_t		n = len - 1;

	if (len == 0)
		return;
	if (n < 60)
		appendStringInfoChar(dst, (char) (n << 2));
	else
	{
		int			nb = 0;
		size_t		t = n;
		int			i;

		while (t > 0)
		{
			nb++;
			t >>= 8;
		}
		appendStringInfoChar(dst, (char) ((59 + nb) << 2));
		for (i = 0; i < nb; i++)
			appendStringInfoChar(dst, (char) ((n >> (8 * i)) & 0xff));
	}
	appendBinaryStringInfo(dst, (const char *) lit, len);
}

/* emit one copy element with 2-byte offset; 1 <= len <= 64 */
static void
snappy_emit_copy2(StringInfo dst, size_t offset, size_t len)
{
	appendStringInfoChar(dst, (char) (2 | ((len - 1) << 2)));
	appendStringInfoChar(dst, (char) (offset & 0xff));
	appendStringInfoChar(dst, (char) ((offset >> 8) & 0xff));
}

static void
snappy_emit_copy(StringInfo dst, size_t offset, size_t len)
{
	while (len >= 68)
	{
		snappy_emit_copy2(dst, offset, 64);
		len -= 64;
	}
	if (len > 64)
	{
		snappy_emit_copy2(dst, offset, 60);
		len -= 60;
	}
	snappy_emit_copy2(dst, offset, len);
}

#define SNAPPY_BLOCK	65536
#define SNAPPY_HBITS	14

static inline uint32
snappy_load32(const uint8 *p)
{
	uint32		v;

	memcpy(&v, p, 4);
	return v;
}

/*
 * Compress src into dst (appending) as one raw Snappy block.  Input is
 * processed in independent 64kB fragments so every back-reference fits in
 * a 2-byte offset.
 */
void
snappy_compress(const uint8 *src, size_t src_len, StringInfo dst)
{
	int32	   *table;
	size_t		base;
	uint64		v = src_len;

	if (src_len > PG_UINT32_MAX)
		elog(ERROR, "snappy input too large");

	/* preamble: uncompressed length */
	while (v >= 0x80)
	{
		appendStringInfoChar(dst, (char) ((v & 0x7f) | 0x80));
		v >>= 7;
	}
	appendStringInfoChar(dst, (char) v);

	table = palloc(sizeof(int32) << SNAPPY_HBITS);

	for (base = 0; base < src_len; base += SNAPPY_BLOCK)
	{
		size_t		end = Min(src_len, base + SNAPPY_BLOCK);
		size_t		ip = base;
		size_t		next_emit = base;

		memset(table, 0xff, sizeof(int32) << SNAPPY_HBITS);

		while (ip + 4 <= end)
		{
			uint32		cur = snappy_load32(src + ip);
			uint32		h = (cur * 0x1e35a7bdU) >> (32 - SNAPPY_HBITS);
			int32		cand = table[h];

			table[h] = (int32) (ip - base);
			if (cand >= 0 && snappy_load32(src + base + cand) == cur)
			{
				size_t		mpos = base + cand;
				size_t		mlen = 4;

				while (ip + mlen < end && src[ip + mlen] == src[mpos + mlen])
					mlen++;
				snappy_emit_literal(dst, src + next_emit, ip - next_emit);
				snappy_emit_copy(dst, ip - mpos, mlen);
				ip += mlen;
				next_emit = ip;
			}
			else
				ip++;
		}
		snappy_emit_literal(dst, src + next_emit, end - next_emit);
	}
	pfree(table);
}
