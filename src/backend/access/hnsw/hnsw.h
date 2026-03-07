/*-------------------------------------------------------------------------
 *
 * hnsw.h
 *	  Header for the HNSW (Hierarchical Navigable Small World) index
 *	  access method.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/backend/access/hnsw/hnsw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HNSW_H
#define HNSW_H

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "access/reloptions.h"
#include "catalog/index.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "utils/vector.h"

/*
 * Constants
 */
#define HNSW_MAX_DIM			16384
#define HNSW_DEFAULT_M			16
#define HNSW_DEFAULT_EF_CONSTRUCTION 64
#define HNSW_MAX_LEVEL			30
#define HNSW_DEFAULT_EF_SEARCH	40

/* Strategy numbers matching vector.h */
#define HNSW_L2_STRATEGY		1	/* <-> L2 distance */
#define HNSW_COSINE_STRATEGY	2	/* <=> cosine distance */
#define HNSW_IP_STRATEGY		3	/* <#> inner product distance */

/* Support function numbers */
#define HNSW_DISTANCE_PROC		1
#define HNSW_NUM_PROCS			1

/*
 * Page layout constants.
 * Metapage is always block 0.  Element pages follow, then neighbor list pages.
 */
#define HNSW_METAPAGE_BLKNO		0
#define HNSW_HEAD_BLKNO			1	/* first data page */

#define HNSW_PAGE_ID			0xFF90	/* for PageSetSpecial identification */

/* Page types stored in HnswPageOpaqueData.flags */
#define HNSW_PAGE_META			(1 << 0)
#define HNSW_PAGE_ELEMENT		(1 << 1)
#define HNSW_PAGE_NEIGHBOR		(1 << 2)

/*
 * Page opaque data, stored at the end of each page.
 */
typedef struct HnswPageOpaqueData
{
	BlockNumber nextblkno;		/* next page of the same type, or InvalidBlockNumber */
	uint16		flags;			/* page type flags */
	uint16		unused;			/* padding */
} HnswPageOpaqueData;

typedef HnswPageOpaqueData *HnswPageOpaque;

#define HnswPageGetOpaque(page) \
	((HnswPageOpaque) PageGetSpecialPointer(page))

/*
 * Metapage data, stored on page 0.
 */
typedef struct HnswMetaPageData
{
	uint32		magicNumber;	/* magic number for validation */
	BlockNumber entryBlkno;		/* block of the entry point element */
	OffsetNumber entryOffset;	/* offset of the entry point element */
	uint16		entryLevel;		/* level of the entry point */
	int32		maxLevel;		/* current maximum level in the graph */
	int64		elementCount;	/* total number of elements */
	int32		m;				/* max connections per layer */
	int32		efConstruction;	/* ef parameter used during construction */
	int32		dimensions;		/* vector dimensionality */
} HnswMetaPageData;

#define HNSW_MAGIC_NUMBER		0x484E5357	/* "HNSW" */

/*
 * Each element stored in the index.  This is the on-disk format for element
 * tuples stored on HNSW_PAGE_ELEMENT pages.
 *
 * The element contains the heap TID, the level assigned to this element,
 * the number of dimensions, and the vector data.  Neighbor lists are stored
 * separately on HNSW_PAGE_NEIGHBOR pages.
 */
typedef struct HnswElementTupleData
{
	ItemPointerData heaptid;	/* heap tuple identifier */
	int16		level;			/* number of layers this element participates in */
	int16		dim;			/* vector dimensionality */
	/* neighbor list pointers: one (blkno, offset) pair per layer */
	BlockNumber neighborPage;	/* first neighbor page for this element */
	OffsetNumber neighborOffset;	/* offset on that page */
	uint16		neighborCount;	/* total neighbor slots used across all layers */
	float		vec[FLEXIBLE_ARRAY_MEMBER];	/* vector data */
} HnswElementTupleData;

typedef HnswElementTupleData *HnswElementTuple;

#define HNSW_ELEMENT_TUPLE_SIZE(dim) \
	(offsetof(HnswElementTupleData, vec) + sizeof(float) * (dim))

/*
 * A single neighbor entry, stored on HNSW_PAGE_NEIGHBOR pages.
 * Each entry records a directed edge: source element -> neighbor element,
 * along with the layer and pre-computed distance.
 */
typedef struct HnswNeighborTupleData
{
	ItemPointerData heaptid;		/* heap TID of the neighbor element */
	BlockNumber srcBlkno;			/* block number of the source element */
	OffsetNumber srcOffset;			/* offset of the source element */
	BlockNumber elementBlkno;		/* block number of the neighbor (target) element */
	OffsetNumber elementOffset;		/* offset of the neighbor (target) element */
	uint16		layer;				/* which layer this neighbor connection is on */
	float		distance;			/* pre-computed distance */
} HnswNeighborTupleData;

typedef HnswNeighborTupleData *HnswNeighborTuple;

/*
 * In-memory representation of an HNSW element, used during build and scan.
 */
typedef struct HnswElement
{
	ItemPointerData heaptid;	/* heap tuple ID */
	BlockNumber blkno;			/* index page where element is stored */
	OffsetNumber offset;		/* offset on that page */
	int			level;			/* assigned level */
	int			dim;			/* dimensionality */
	float	   *vec;			/* vector data (palloc'd) */

	/* Neighbor lists: neighbors[i] is the list for layer i */
	List	  **neighbors;		/* array of Lists of HnswCandidate* */
} HnswElement;

/*
 * A candidate during search: element pointer + distance.
 */
typedef struct HnswCandidate
{
	HnswElement *element;
	float		distance;
	pairingheap_node ph_node;	/* for use in pairing heap */
} HnswCandidate;

/*
 * HNSW reloptions
 */
typedef struct HnswOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly) */
	int			m;				/* max connections per layer */
	int			efConstruction;	/* ef during construction */
} HnswOptions;

#define HnswGetM(relation) \
	((relation)->rd_options ? \
	 ((HnswOptions *) (relation)->rd_options)->m : \
	 HNSW_DEFAULT_M)

#define HnswGetEfConstruction(relation) \
	((relation)->rd_options ? \
	 ((HnswOptions *) (relation)->rd_options)->efConstruction : \
	 HNSW_DEFAULT_EF_CONSTRUCTION)

/*
 * Build state, used during index construction.
 */
typedef struct HnswBuildState
{
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;

	/* Build parameters */
	int			m;				/* max connections per layer */
	int			efConstruction;	/* ef during construction */
	int			dimensions;		/* vector dimensionality (set from first tuple) */
	double		ml;				/* level generation factor: 1/ln(m) */

	/* Graph state */
	HnswElement *entryPoint;	/* current entry point */
	int			maxLevel;		/* current maximum level */
	List	   *elements;		/* list of all HnswElement* inserted so far */
	int64		elementCount;	/* total elements */

	/* Memory management */
	MemoryContext tmpCtx;		/* per-tuple context */
	MemoryContext buildCtx;		/* long-lived build context */

	/* Flush state */
	int			flushedCount;	/* how many elements already written to disk */
} HnswBuildState;

/*
 * Scan state
 */
typedef struct HnswScanOpaqueData
{
	/* Query */
	Vector	   *queryVec;		/* the query vector */
	int			strategy;		/* distance strategy number */
	FmgrInfo	distanceFn;		/* distance function */

	/* Search parameters */
	int			efSearch;		/* ef_search value from GUC */

	/* Results */
	pairingheap *resultQueue;	/* min-heap of HnswCandidate by distance */
	bool		firstCall;		/* true if amgettuple hasn't been called yet */
	List	   *resultList;		/* list of HnswCandidate* results, sorted */
	int			resultIdx;		/* current position in resultList */

	/* Memory management */
	MemoryContext scanCtx;
} HnswScanOpaqueData;

typedef HnswScanOpaqueData *HnswScanOpaque;

/*
 * HNSW GUC variable: ef_search
 */
extern int	hnsw_ef_search;

/*
 * Function declarations -- handler
 */
extern Datum hnswhandler(PG_FUNCTION_ARGS);

/* hnswbuild.c */
extern IndexBuildResult *hnswbuild(Relation heap, Relation index,
								   struct IndexInfo *indexInfo);
extern void hnswbuildempty(Relation index);

/* hnswinsert.c */
extern bool hnswinsert(Relation index, Datum *values, bool *isnull,
					   ItemPointer ht_ctid, Relation heapRel,
					   IndexUniqueCheck checkUnique, bool indexUnchanged,
					   struct IndexInfo *indexInfo);

/* hnsw_scan.c */
extern IndexScanDesc hnswbeginscan(Relation index, int nkeys, int norderbys);
extern void hnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
					   ScanKey orderbys, int norderbys);
extern bool hnswgettuple(IndexScanDesc scan, ScanDirection dir);
extern void hnswendscan(IndexScanDesc scan);

/* hnswvacuum.c */
extern IndexBulkDeleteResult *hnswbulkdelete(IndexVacuumInfo *info,
											 IndexBulkDeleteResult *stats,
											 IndexBulkDeleteCallback callback,
											 void *callback_state);
extern IndexBulkDeleteResult *hnswvacuumcleanup(IndexVacuumInfo *info,
												IndexBulkDeleteResult *stats);

/* hnswcost.c */
extern void hnswcostestimate(struct PlannerInfo *root,
							 struct IndexPath *path,
							 double loop_count,
							 Cost *indexStartupCost,
							 Cost *indexTotalCost,
							 Selectivity *indexSelectivity,
							 double *indexCorrelation,
							 double *indexPages);

/* hnswvalidate.c */
extern bool hnswvalidate(Oid opclassoid);

/* hnswoptions.c */
extern bytea *hnswoptions(Datum reloptions, bool validate);

/*
 * Utility functions (in hnswutils.c)
 */

/* Distance computation */
typedef double (*HnswDistanceFunc)(const Vector *a, const Vector *b);
extern HnswDistanceFunc hnsw_get_distance_func(int strategy);
extern float hnsw_calc_distance(const Vector *a, const Vector *b, int strategy);

/* Graph search helpers */
extern List *hnsw_search_layer(HnswElement *entryPoint, const Vector *query,
							   int efSearch, int layer, int strategy,
							   List *elements);
extern int	hnsw_random_level(double ml);
extern int	hnsw_get_max_neighbors(int m, int layer);

#endif							/* HNSW_H */
