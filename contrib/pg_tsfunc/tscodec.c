/*-------------------------------------------------------------------------
 *
 * tscodec.c
 *	  Standalone value codecs for whole time series stored in one bytea.
 *
 *	  ts_encode_timestamps / ts_decode_timestamps:
 *	    delta-of-delta + zigzag + bit-level variable-length integers
 *	  ts_encode_floats / ts_decode_floats:
 *	    XOR with the previous value + leading/trailing-zero bit packing
 *	  (both after Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory
 *	  Time Series Database", VLDB 2015).
 *
 *	  These are pure functions from an array to a bytea and back.  They do
 *	  not touch any table; see the patent notes in pg_tsfunc.c.
 *
 *	  Format (all versions):
 *	    byte 0      codec id: 'T' (timestamps) or 'F' (float8)
 *	    byte 1      format version (1)
 *	    byte 2      flags: bit 0 = null bitmap present; other bits zero
 *	    bytes 3-6   element count, uint32 little-endian
 *	    [bitmap]    if flag bit 0: ceil(count/8) bytes, bit i (LSB first)
 *	                set = element i is NOT NULL; unused bits zero
 *	    bitstream   MSB-first, the non-NULL values only, zero padded to a
 *	                byte boundary; no trailing bytes allowed
 *
 *	  Timestamp stream: first value as 64 raw bits; then for each value
 *	  dod = (t[i]-t[i-1]) - (t[i-1]-t[i-2]) (modulo 2^64, previous delta
 *	  of the first value is 0), z = zigzag(dod):
 *	    z == 0 : '0'
 *	    else   : '1' then z-1 as '0'+7 bits | '10'+14 | '110'+20 |
 *	             '1110'+32 | '1111'+64 bits
 *
 *	  Float stream: first value as 64 raw bits; then x = bits ^ prevbits:
 *	    x == 0                         : '0'
 *	    fits previous lead/trail window: '10' + window bits
 *	    otherwise                      : '11' + 5 bits leading zeros
 *	                                     (capped at 31) + 6 bits
 *	                                     (significant bits - 1) + bits
 *	  Everything works on the raw 64-bit patterns, so round trips are
 *	  bit-exact (NaN payloads, -0.0, infinities, denormals, and
 *	  +/-infinity timestamps included).
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_tsfunc/tscodec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "port/pg_bitutils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#define TSC_ID_TIMESTAMP	'T'
#define TSC_ID_FLOAT		'F'
#define TSC_VERSION			1
#define TSC_FLAG_NULLS		0x01
#define TSC_HEADER_SIZE		7

/* ----------------------------------------------------------------
 * Bit writer / reader
 * ----------------------------------------------------------------
 */
typedef struct BitWriter
{
	uint8	   *buf;
	size_t		cap;			/* bytes */
	uint64		nbits;			/* bits written */
} BitWriter;

static void
bw_init(BitWriter *bw, size_t initial)
{
	bw->cap = Max(initial, 16);
	bw->buf = palloc_extended(bw->cap, MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
	bw->nbits = 0;
}

static void
bw_put(BitWriter *bw, uint64 val, int nbits)
{
	size_t		need = (size_t) ((bw->nbits + nbits + 7) / 8);

	if (need > bw->cap)
	{
		size_t		newcap = bw->cap * 2;

		while (newcap < need)
			newcap *= 2;
		if (newcap > MaxAllocSize)
			newcap = MaxAllocSize;
		if (need > newcap)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("encoded series is too large")));
		bw->buf = repalloc_huge(bw->buf, newcap);
		memset(bw->buf + bw->cap, 0, newcap - bw->cap);
		bw->cap = newcap;
	}
	for (int i = nbits - 1; i >= 0; i--)
	{
		if ((val >> i) & 1)
			bw->buf[bw->nbits >> 3] |= (uint8) (0x80 >> (bw->nbits & 7));
		bw->nbits++;
	}
}

typedef struct BitReader
{
	const uint8 *buf;
	uint64		nbits;			/* total bits available */
	uint64		pos;
} BitReader;

static void
corrupt(const char *detail)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("invalid or corrupted encoded time series"),
			 errdetail_internal("%s", detail)));
}

static uint64
br_get(BitReader *br, int nbits)
{
	uint64		v = 0;

	if (br->nbits - br->pos < (uint64) nbits)
		corrupt("bit stream ends prematurely");
	for (int i = 0; i < nbits; i++)
	{
		v = (v << 1) | ((br->buf[br->pos >> 3] >> (7 - (br->pos & 7))) & 1);
		br->pos++;
	}
	return v;
}

/* After decoding: only zero padding up to the next byte may remain */
static void
br_finish(BitReader *br)
{
	uint64		rest = br->nbits - br->pos;

	if (rest >= 8)
		corrupt("trailing data after the last value");
	if (rest > 0 && br_get(br, (int) rest) != 0)
		corrupt("non-zero padding bits");
}

/* ----------------------------------------------------------------
 * Header handling
 * ----------------------------------------------------------------
 */
typedef struct TscHeader
{
	char		id;
	int			version;
	uint32		count;
	uint32		nnonnull;
	const uint8 *bitmap;		/* NULL if no nulls */
	const uint8 *stream;
	size_t		stream_len;
} TscHeader;

static void
tsc_parse_header(const uint8 *data, size_t len, TscHeader *h)
{
	uint8		flags;
	size_t		off = TSC_HEADER_SIZE;

	if (len < TSC_HEADER_SIZE)
		corrupt("input is shorter than the header");
	h->id = (char) data[0];
	if (h->id != TSC_ID_TIMESTAMP && h->id != TSC_ID_FLOAT)
		corrupt("unknown codec id");
	h->version = data[1];
	if (h->version != TSC_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("unsupported encoded time series version %d", h->version)));
	flags = data[2];
	if ((flags & ~TSC_FLAG_NULLS) != 0)
		corrupt("unknown flag bits");
	h->count = (uint32) data[3] | ((uint32) data[4] << 8) |
		((uint32) data[5] << 16) | ((uint32) data[6] << 24);
	if (h->count > MaxArraySize)
		corrupt("element count too large");

	h->bitmap = NULL;
	h->nnonnull = h->count;
	if (flags & TSC_FLAG_NULLS)
	{
		size_t		blen = (h->count + 7) / 8;

		if (h->count == 0)
			corrupt("null bitmap present for an empty series");
		if (len - off < blen)
			corrupt("null bitmap is truncated");
		h->bitmap = data + off;
		h->nnonnull = 0;
		for (uint32 i = 0; i < h->count; i++)
			if (h->bitmap[i >> 3] & (1 << (i & 7)))
				h->nnonnull++;
		if ((h->count & 7) != 0 &&
			(h->bitmap[blen - 1] >> (h->count & 7)) != 0)
			corrupt("non-zero unused bits in null bitmap");
		off += blen;
	}
	h->stream = data + off;
	h->stream_len = len - off;

	/*
	 * Each value after the first takes at least one bit and the first takes
	 * 64; reject counts the stream cannot possibly hold before allocating.
	 */
	if (h->nnonnull == 0)
	{
		if (h->stream_len != 0)
			corrupt("trailing data after the last value");
	}
	else if ((uint64) h->stream_len * 8 < 64 + (uint64) (h->nnonnull - 1))
		corrupt("bit stream too short for the element count");
}

/* Write header + optional null bitmap; returns the start of a buffer */
static bytea *
tsc_assemble(char id, int count, const bool *nulls, BitWriter *bw)
{
	bool		hasnulls = false;
	size_t		blen = 0;
	size_t		slen = (size_t) ((bw->nbits + 7) / 8);
	size_t		total;
	bytea	   *res;
	uint8	   *p;

	for (int i = 0; i < count; i++)
		if (nulls[i])
		{
			hasnulls = true;
			break;
		}
	if (hasnulls)
		blen = ((size_t) count + 7) / 8;
	total = TSC_HEADER_SIZE + blen + slen;
	if (total + VARHDRSZ > MaxAllocSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("encoded series is too large")));
	res = (bytea *) palloc0(total + VARHDRSZ);
	SET_VARSIZE(res, total + VARHDRSZ);
	p = (uint8 *) VARDATA(res);
	p[0] = (uint8) id;
	p[1] = TSC_VERSION;
	p[2] = hasnulls ? TSC_FLAG_NULLS : 0;
	p[3] = (uint8) (count & 0xFF);
	p[4] = (uint8) ((count >> 8) & 0xFF);
	p[5] = (uint8) ((count >> 16) & 0xFF);
	p[6] = (uint8) ((count >> 24) & 0xFF);
	if (hasnulls)
		for (int i = 0; i < count; i++)
			if (!nulls[i])
				p[TSC_HEADER_SIZE + (i >> 3)] |= (uint8) (1 << (i & 7));
	if (slen > 0)
		memcpy(p + TSC_HEADER_SIZE + blen, bw->buf, slen);
	return res;
}

static void
check_1d(ArrayType *arr)
{
	if (ARR_NDIM(arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("only one-dimensional arrays are supported")));
}

static inline uint64
zigzag(uint64 v)
{
	return (v << 1) ^ (0 - (v >> 63));
}

static inline uint64
unzigzag(uint64 z)
{
	return (z >> 1) ^ (0 - (z & 1));
}

/* ----------------------------------------------------------------
 * Timestamps
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(ts_encode_timestamps);
Datum
ts_encode_timestamps(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *d;
	bool	   *nulls;
	int			n;
	BitWriter	bw;
	bool		first = true;
	uint64		prev = 0,
				prevdelta = 0;

	check_1d(arr);
	deconstruct_array(arr, TIMESTAMPTZOID, sizeof(int64), FLOAT8PASSBYVAL,
					  TYPALIGN_DOUBLE, &d, &nulls, &n);
	bw_init(&bw, (size_t) n / 4 + 16);

	for (int i = 0; i < n; i++)
	{
		uint64		v,
					delta,
					z;

		if (nulls[i])
			continue;
		v = (uint64) DatumGetTimestampTz(d[i]);
		if (first)
		{
			bw_put(&bw, v, 64);
			prev = v;
			first = false;
			continue;
		}
		delta = v - prev;
		z = zigzag(delta - prevdelta);
		prev = v;
		prevdelta = delta;
		if (z == 0)
		{
			bw_put(&bw, 0, 1);
			continue;
		}
		z--;
		if (z < (UINT64CONST(1) << 7))
		{
			bw_put(&bw, 0x2, 2);	/* '1' '0' */
			bw_put(&bw, z, 7);
		}
		else if (z < (UINT64CONST(1) << 14))
		{
			bw_put(&bw, 0x6, 3);	/* '1' '10' */
			bw_put(&bw, z, 14);
		}
		else if (z < (UINT64CONST(1) << 20))
		{
			bw_put(&bw, 0xE, 4);	/* '1' '110' */
			bw_put(&bw, z, 20);
		}
		else if (z < (UINT64CONST(1) << 32))
		{
			bw_put(&bw, 0x1E, 5);	/* '1' '1110' */
			bw_put(&bw, z, 32);
		}
		else
		{
			bw_put(&bw, 0x1F, 5);	/* '1' '1111' */
			bw_put(&bw, z, 64);
		}
	}
	PG_RETURN_BYTEA_P(tsc_assemble(TSC_ID_TIMESTAMP, n, nulls, &bw));
}

PG_FUNCTION_INFO_V1(ts_decode_timestamps);
Datum
ts_decode_timestamps(PG_FUNCTION_ARGS)
{
	bytea	   *in = PG_GETARG_BYTEA_PP(0);
	TscHeader	h;
	BitReader	br;
	Datum	   *d;
	bool	   *nulls;
	uint64		prev = 0,
				prevdelta = 0;
	int			dims[1];
	int			lbs[1] = {1};

	tsc_parse_header((const uint8 *) VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in), &h);
	if (h.id != TSC_ID_TIMESTAMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input is not an encoded timestamp series"),
				 errhint("Use ts_decode_floats() for float series.")));
	if (h.count == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(TIMESTAMPTZOID));

	br.buf = h.stream;
	br.nbits = (uint64) h.stream_len * 8;
	br.pos = 0;
	d = palloc_extended(sizeof(Datum) * h.count, MCXT_ALLOC_HUGE);
	nulls = palloc_extended(sizeof(bool) * h.count, MCXT_ALLOC_HUGE);

	for (uint32 i = 0, k = 0; i < h.count; i++)
	{
		uint64		v;

		if (h.bitmap && !(h.bitmap[i >> 3] & (1 << (i & 7))))
		{
			nulls[i] = true;
			d[i] = (Datum) 0;
			continue;
		}
		nulls[i] = false;
		if (k++ == 0)
			v = br_get(&br, 64);
		else
		{
			uint64		z = 0;

			if (br_get(&br, 1) != 0)
			{
				int			nb;

				if (br_get(&br, 1) == 0)
					nb = 7;
				else if (br_get(&br, 1) == 0)
					nb = 14;
				else if (br_get(&br, 1) == 0)
					nb = 20;
				else if (br_get(&br, 1) == 0)
					nb = 32;
				else
					nb = 64;
				z = br_get(&br, nb) + 1;
			}
			prevdelta = prevdelta + unzigzag(z);
			v = prev + prevdelta;
		}
		/* reject bit patterns that are not valid timestamps */
		if (!IS_VALID_TIMESTAMP((int64) v) && !TIMESTAMP_NOT_FINITE((int64) v))
			corrupt("decoded timestamp out of range");
		prev = v;
		d[i] = TimestampTzGetDatum((int64) v);
	}
	br_finish(&br);

	dims[0] = (int) h.count;
	PG_RETURN_ARRAYTYPE_P(construct_md_array(d, nulls, 1, dims, lbs,
											 TIMESTAMPTZOID, sizeof(int64),
											 FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
}

/* ----------------------------------------------------------------
 * Floats
 * ----------------------------------------------------------------
 */
static inline uint64
float8_bits(float8 f)
{
	uint64		u;

	memcpy(&u, &f, sizeof(u));
	return u;
}

static inline float8
bits_float8(uint64 u)
{
	float8		f;

	memcpy(&f, &u, sizeof(f));
	return f;
}

PG_FUNCTION_INFO_V1(ts_encode_floats);
Datum
ts_encode_floats(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *d;
	bool	   *nulls;
	int			n;
	BitWriter	bw;
	bool		first = true;
	uint64		prev = 0;
	bool		have_window = false;
	int			wlead = 0,
				wtrail = 0;

	check_1d(arr);
	deconstruct_array_builtin(arr, FLOAT8OID, &d, &nulls, &n);
	bw_init(&bw, (size_t) n * 2 + 16);

	for (int i = 0; i < n; i++)
	{
		uint64		v,
					x;
		int			lead,
					trail;

		if (nulls[i])
			continue;
		v = float8_bits(DatumGetFloat8(d[i]));
		if (first)
		{
			bw_put(&bw, v, 64);
			prev = v;
			first = false;
			continue;
		}
		x = v ^ prev;
		prev = v;
		if (x == 0)
		{
			bw_put(&bw, 0, 1);
			continue;
		}
		lead = 63 - pg_leftmost_one_pos64(x);
		trail = pg_rightmost_one_pos64(x);
		if (lead > 31)
			lead = 31;
		if (have_window && lead >= wlead && trail >= wtrail)
		{
			int			sig = 64 - wlead - wtrail;

			bw_put(&bw, 0x2, 2);	/* '10' */
			bw_put(&bw, x >> wtrail, sig);
		}
		else
		{
			int			sig = 64 - lead - trail;

			bw_put(&bw, 0x3, 2);	/* '11' */
			bw_put(&bw, (uint64) lead, 5);
			bw_put(&bw, (uint64) (sig - 1), 6);
			bw_put(&bw, x >> trail, sig);
			have_window = true;
			wlead = lead;
			wtrail = trail;
		}
	}
	PG_RETURN_BYTEA_P(tsc_assemble(TSC_ID_FLOAT, n, nulls, &bw));
}

PG_FUNCTION_INFO_V1(ts_decode_floats);
Datum
ts_decode_floats(PG_FUNCTION_ARGS)
{
	bytea	   *in = PG_GETARG_BYTEA_PP(0);
	TscHeader	h;
	BitReader	br;
	Datum	   *d;
	bool	   *nulls;
	uint64		prev = 0;
	bool		have_window = false;
	int			wlead = 0,
				wtrail = 0;
	int			dims[1];
	int			lbs[1] = {1};

	tsc_parse_header((const uint8 *) VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in), &h);
	if (h.id != TSC_ID_FLOAT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input is not an encoded float series"),
				 errhint("Use ts_decode_timestamps() for timestamp series.")));
	if (h.count == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(FLOAT8OID));

	br.buf = h.stream;
	br.nbits = (uint64) h.stream_len * 8;
	br.pos = 0;
	d = palloc_extended(sizeof(Datum) * h.count, MCXT_ALLOC_HUGE);
	nulls = palloc_extended(sizeof(bool) * h.count, MCXT_ALLOC_HUGE);

	for (uint32 i = 0, k = 0; i < h.count; i++)
	{
		uint64		v;

		if (h.bitmap && !(h.bitmap[i >> 3] & (1 << (i & 7))))
		{
			nulls[i] = true;
			d[i] = (Datum) 0;
			continue;
		}
		nulls[i] = false;
		if (k++ == 0)
			v = br_get(&br, 64);
		else if (br_get(&br, 1) == 0)
			v = prev;
		else if (br_get(&br, 1) == 0)
		{
			if (!have_window)
				corrupt("window reuse before any window was defined");
			v = prev ^ (br_get(&br, 64 - wlead - wtrail) << wtrail);
		}
		else
		{
			int			lead = (int) br_get(&br, 5);
			int			sig = (int) br_get(&br, 6) + 1;

			if (lead + sig > 64)
				corrupt("invalid leading-zero/length combination");
			wlead = lead;
			wtrail = 64 - lead - sig;
			have_window = true;
			v = prev ^ (br_get(&br, sig) << wtrail);
		}
		prev = v;
		d[i] = Float8GetDatum(bits_float8(v));
	}
	br_finish(&br);

	dims[0] = (int) h.count;
	PG_RETURN_ARRAYTYPE_P(construct_md_array(d, nulls, 1, dims, lbs,
											 FLOAT8OID, sizeof(float8),
											 FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
}

/* ----------------------------------------------------------------
 * ts_codec_stats(bytea) -> (codec, version, count, nulls, bytes,
 *                           bits_per_value)
 * Validates the header only; use the decoders for a full check.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(ts_codec_stats);
Datum
ts_codec_stats(PG_FUNCTION_ARGS)
{
	bytea	   *in = PG_GETARG_BYTEA_PP(0);
	size_t		len = VARSIZE_ANY_EXHDR(in);
	TscHeader	h;
	TupleDesc	tupdesc;
	Datum		values[6];
	bool		nulls[6] = {0};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	tsc_parse_header((const uint8 *) VARDATA_ANY(in), len, &h);

	values[0] = CStringGetTextDatum(h.id == TSC_ID_TIMESTAMP ? "timestamptz" : "float8");
	values[1] = Int32GetDatum(h.version);
	values[2] = Int32GetDatum((int32) h.count);
	values[3] = Int32GetDatum((int32) (h.count - h.nnonnull));
	values[4] = Int32GetDatum((int32) len);
	if (h.count > 0)
		values[5] = Float8GetDatum((double) len * 8.0 / (double) h.count);
	else
		nulls[5] = true;

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
