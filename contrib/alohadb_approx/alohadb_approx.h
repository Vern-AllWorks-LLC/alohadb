/*-------------------------------------------------------------------------
 *
 * alohadb_approx.h
 *	  Shared declarations for the alohadb_approx extension.
 *
 * contrib/alohadb_approx/alohadb_approx.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_APPROX_H
#define ALOHADB_APPROX_H

#include "postgres.h"
#include "fmgr.h"
#include "varatt.h"

/*
 * HLL precision parameter.  p=14 gives 2^14 = 16384 registers
 * and approximately 0.8% standard error.
 */
#define HLL_BIT_WIDTH		14

/*
 * Count-Min Sketch varlena layout:
 *   [vl_len_][width][depth][counters...]
 * where counters is an array of width*depth uint32 values.
 */
typedef struct CmsData
{
	int32		vl_len_;		/* varlena header (do not touch directly) */
	uint32		width;
	uint32		depth;
	uint32		counters[FLEXIBLE_ARRAY_MEMBER];
} CmsData;

#define CMS_HEADER_SIZE		(offsetof(CmsData, counters))
#define CMS_SIZE(w, d)		(CMS_HEADER_SIZE + sizeof(uint32) * (w) * (d))

typedef CmsData *Cms;

#define DatumGetCms(X)		((Cms) PG_DETOAST_DATUM(X))
#define DatumGetCmsCopy(X)	((Cms) PG_DETOAST_DATUM_COPY(X))
#define CmsGetDatum(X)		PointerGetDatum(X)
#define PG_GETARG_CMS(n)	DatumGetCms(PG_GETARG_DATUM(n))
#define PG_GETARG_CMS_COPY(n) DatumGetCmsCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_CMS(x)	PG_RETURN_POINTER(x)

/* HLL aggregate functions (hll_agg.c) */
extern Datum approx_count_distinct_transfn(PG_FUNCTION_ARGS);
extern Datum approx_count_distinct_finalfn(PG_FUNCTION_ARGS);

/* Count-Min Sketch I/O and functions (cms.c) */
extern Datum cms_in(PG_FUNCTION_ARGS);
extern Datum cms_out(PG_FUNCTION_ARGS);
extern Datum cms_create(PG_FUNCTION_ARGS);
extern Datum cms_add(PG_FUNCTION_ARGS);
extern Datum cms_estimate(PG_FUNCTION_ARGS);
extern Datum cms_merge(PG_FUNCTION_ARGS);

/* Top-K aggregate and SRF wrapper (topk.c) */
extern Datum approx_topk_transfn(PG_FUNCTION_ARGS);
extern Datum approx_topk_finalfn(PG_FUNCTION_ARGS);
extern Datum approx_topk(PG_FUNCTION_ARGS);

/*
 * Bloom filter varlena layout (bloom_filter.c):
 *   [vl_len_][m][k][n_added][bits...]
 * where bits is a variable-length bit array of ceil(m/8) bytes.
 */
typedef struct BloomData
{
	int32		vl_len_;		/* varlena header (do not touch directly) */
	uint32		m;				/* number of bits */
	uint32		k;				/* number of hash functions */
	uint32		n_added;		/* count of items added */
	uint8		bits[FLEXIBLE_ARRAY_MEMBER];
} BloomData;

#define BLOOM_HEADER_SIZE		(offsetof(BloomData, bits))
#define BLOOM_SIZE(m)			(BLOOM_HEADER_SIZE + (((m) + 7) / 8))

typedef BloomData *Bloom;

#define DatumGetBloom(X)		((Bloom) PG_DETOAST_DATUM(X))
#define DatumGetBloomCopy(X)	((Bloom) PG_DETOAST_DATUM_COPY(X))
#define BloomGetDatum(X)		PointerGetDatum(X)
#define PG_GETARG_BLOOM(n)		DatumGetBloom(PG_GETARG_DATUM(n))
#define PG_GETARG_BLOOM_COPY(n) DatumGetBloomCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_BLOOM(x)		PG_RETURN_POINTER(x)

/* Bloom filter functions (bloom_filter.c) */
extern Datum bloom_in(PG_FUNCTION_ARGS);
extern Datum bloom_out(PG_FUNCTION_ARGS);
extern Datum bloom_create(PG_FUNCTION_ARGS);
extern Datum bloom_add(PG_FUNCTION_ARGS);
extern Datum bloom_contains(PG_FUNCTION_ARGS);
extern Datum bloom_merge(PG_FUNCTION_ARGS);
extern Datum bloom_stats(PG_FUNCTION_ARGS);
extern Datum bloom_agg_transfn(PG_FUNCTION_ARGS);
extern Datum bloom_agg_finalfn(PG_FUNCTION_ARGS);

/*
 * T-Digest varlena layout (tdigest.c):
 *   [vl_len_][compression][num_centroids][max_centroids]
 *   [total_weight][min_val][max_val][centroids...]
 * where centroids is an array of Centroid structs.
 */
typedef struct Centroid
{
	float8		mean;
	float8		weight;
} Centroid;

typedef struct TDigestData
{
	int32		vl_len_;		/* varlena header (do not touch directly) */
	float8		compression;	/* compression parameter (default 100) */
	int32		num_centroids;
	int32		max_centroids;
	float8		total_weight;
	float8		min_val;
	float8		max_val;
	Centroid	centroids[FLEXIBLE_ARRAY_MEMBER];
} TDigestData;

#define TDIGEST_HEADER_SIZE		(offsetof(TDigestData, centroids))
#define TDIGEST_SIZE(n)			(TDIGEST_HEADER_SIZE + sizeof(Centroid) * (n))

typedef TDigestData *TDigest;

#define DatumGetTDigest(X)			((TDigest) PG_DETOAST_DATUM(X))
#define DatumGetTDigestCopy(X)		((TDigest) PG_DETOAST_DATUM_COPY(X))
#define TDigestGetDatum(X)			PointerGetDatum(X)
#define PG_GETARG_TDIGEST(n)		DatumGetTDigest(PG_GETARG_DATUM(n))
#define PG_GETARG_TDIGEST_COPY(n)	DatumGetTDigestCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_TDIGEST(x)		PG_RETURN_POINTER(x)

/* T-Digest functions (tdigest.c) */
extern Datum tdigest_in(PG_FUNCTION_ARGS);
extern Datum tdigest_out(PG_FUNCTION_ARGS);
extern Datum tdigest_create(PG_FUNCTION_ARGS);
extern Datum tdigest_add(PG_FUNCTION_ARGS);
extern Datum tdigest_quantile(PG_FUNCTION_ARGS);
extern Datum tdigest_cdf(PG_FUNCTION_ARGS);
extern Datum tdigest_merge(PG_FUNCTION_ARGS);
extern Datum approx_percentile_transfn(PG_FUNCTION_ARGS);
extern Datum approx_percentile_finalfn(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_APPROX_H */
