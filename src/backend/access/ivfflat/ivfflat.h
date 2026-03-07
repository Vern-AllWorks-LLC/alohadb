/*-------------------------------------------------------------------------
 *
 * ivfflat.h
 *	  Header for IVFFlat (Inverted File with Flat quantization) index AM.
 *
 * The IVFFlat index partitions vectors into Voronoi cells using k-means
 * clustering.  At search time, only the posting lists of the nearest
 * centroids are scanned, trading recall for speed.
 *
 * Page layout:
 *	  Block 0           - metapage (IvfflatMetaPageData)
 *	  Blocks 1..N       - centroid pages (IvfflatListData entries)
 *	  Blocks N+1..end   - posting list pages (IvfflatEntryData entries)
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/backend/access/ivfflat/ivfflat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IVFFLAT_H
#define IVFFLAT_H

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/reloptions.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "utils/vector.h"

/* ----------
 * Constants
 * ----------
 */
#define IVFFLAT_DEFAULT_LISTS	100
#define IVFFLAT_MAX_LISTS		10000
#define IVFFLAT_MIN_LISTS		1
#define IVFFLAT_DEFAULT_PROBES	1
#define IVFFLAT_MAX_PROBES		IVFFLAT_MAX_LISTS

/* Maximum vectors to sample for k-means training */
#define IVFFLAT_KMEANS_SAMPLE_SIZE	10000

/* Maximum k-means iterations */
#define IVFFLAT_KMEANS_MAX_ITERATIONS	20

/* Strategy numbers -- must match pg_amop.dat and vector.h */
#define IVFFLAT_L2_DISTANCE_STRATEGY	VECTOR_L2_DISTANCE_STRATEGY
#define IVFFLAT_COSINE_DISTANCE_STRATEGY	VECTOR_COSINE_DISTANCE_STRATEGY
#define IVFFLAT_IP_DISTANCE_STRATEGY	VECTOR_IP_DISTANCE_STRATEGY

/* Number of strategies and support functions */
#define IVFFLAT_NUM_STRATEGIES		3
#define IVFFLAT_NUM_SUPPORT			1

/* Metapage is always block 0 */
#define IVFFLAT_METAPAGE_BLKNO		0

/* Metapage magic number */
#define IVFFLAT_META_MAGIC			0x49564646	/* "IVFF" */

/* Page types for special space */
#define IVFFLAT_PAGE_META			0x01
#define IVFFLAT_PAGE_CENTROID		0x02
#define IVFFLAT_PAGE_POSTING		0x03

/* ----------
 * Page special data -- stored at the end of every IVFFlat page
 * ----------
 */
typedef struct IvfflatPageOpaqueData
{
	BlockNumber nextblkno;		/* next page in chain, or InvalidBlockNumber */
	uint16		page_type;		/* IVFFLAT_PAGE_* */
	uint16		unused;			/* padding */
} IvfflatPageOpaqueData;

typedef IvfflatPageOpaqueData *IvfflatPageOpaque;

#define IvfflatPageGetOpaque(page) \
	((IvfflatPageOpaque) PageGetSpecialPointer(page))

/* ----------
 * Metapage data -- stored on block 0
 * ----------
 */
typedef struct IvfflatMetaPageData
{
	uint32		magic;			/* must be IVFFLAT_META_MAGIC */
	uint32		num_lists;		/* number of centroid lists (k) */
	uint32		dimensions;		/* vector dimensionality */
	uint32		num_vectors;	/* total number of indexed vectors */
	BlockNumber first_centroid_blkno;	/* first centroid page */
} IvfflatMetaPageData;

/* ----------
 * Centroid list descriptor -- one per Voronoi cell.
 * Stored contiguously on centroid pages.
 *
 * The centroid vector data follows immediately after this struct
 * (variable length, VECTOR_SIZE(dim) bytes).
 * ----------
 */
typedef struct IvfflatListData
{
	BlockNumber first_posting_blkno;	/* head of posting list chain */
	BlockNumber last_posting_blkno;		/* tail of posting list chain */
	uint32		num_entries;			/* number of vectors in this list */

	/* centroid vector data follows (variable length) */
} IvfflatListData;

/* Size of a centroid entry including the vector */
#define IVFFLAT_LIST_SIZE(dim) \
	(offsetof(IvfflatListData, num_entries) + sizeof(uint32) + VECTOR_SIZE(dim))

/* Get the centroid vector from a list descriptor */
#define IvfflatListGetCentroid(list) \
	((Vector *) ((char *)(list) + offsetof(IvfflatListData, num_entries) + sizeof(uint32)))

/* ----------
 * Posting list entry -- one per indexed vector.
 * Stored contiguously on posting list pages.
 *
 * The vector data follows immediately after the heap TID.
 * ----------
 */
typedef struct IvfflatEntryData
{
	ItemPointerData heap_tid;	/* pointer to heap tuple */

	/* vector data follows (variable length, VECTOR_SIZE(dim) bytes) */
} IvfflatEntryData;

/* Size of a posting entry including the vector */
#define IVFFLAT_ENTRY_SIZE(dim) \
	MAXALIGN(offsetof(IvfflatEntryData, heap_tid) + sizeof(ItemPointerData) + VECTOR_SIZE(dim))

/* Get the vector from a posting entry */
#define IvfflatEntryGetVector(entry) \
	((Vector *) ((char *)(entry) + offsetof(IvfflatEntryData, heap_tid) + sizeof(ItemPointerData)))

/* ----------
 * Reloptions for CREATE INDEX ... WITH (lists = N)
 * ----------
 */
typedef struct IvfflatOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			lists;			/* number of centroid lists */
} IvfflatOptions;

#define IvfflatGetLists(relation) \
	((relation)->rd_options ? \
	 ((IvfflatOptions *) (relation)->rd_options)->lists : \
	 IVFFLAT_DEFAULT_LISTS)

/* ----------
 * Scan opaque data
 * ----------
 */

/* Result entry in the pairing heap */
typedef struct IvfflatScanEntry
{
	pairingheap_node ph_node;	/* pairing heap node -- must be first */
	ItemPointerData heap_tid;	/* heap tuple identifier */
	double		distance;		/* distance from query to this vector */
} IvfflatScanEntry;

typedef struct IvfflatScanOpaqueData
{
	/* Query vector (copied from scan keys) */
	Vector	   *query;
	int			dimensions;

	/* Strategy (distance metric) */
	StrategyNumber strategy;

	/* Probes setting */
	int			nprobes;

	/* Distance function (selected based on strategy) */
	double		(*distfunc)(const Vector *a, const Vector *b);

	/* Results heap (min-heap sorted by distance) */
	pairingheap *results;

	/* Whether we have scanned the posting lists yet */
	bool		scan_done;

	/* Number of results in the heap */
	int			num_results;

	/* Memory context for scan-lifetime allocations */
	MemoryContext scan_ctx;
} IvfflatScanOpaqueData;

typedef IvfflatScanOpaqueData *IvfflatScanOpaque;

/* ----------
 * Build state
 * ----------
 */
typedef struct IvfflatBuildState
{
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;

	int			dimensions;
	int			num_lists;

	/* k-means training data */
	Vector	  **samples;
	int			num_samples;
	int			max_samples;

	/* Centroids computed by k-means */
	Vector	  **centroids;

	/* Per-list posting list tracking */
	BlockNumber *list_first_blkno;
	BlockNumber *list_last_blkno;
	uint32	   *list_counts;

	/* Page being filled for posting list writes */
	Buffer		current_buffer;
	Page		current_page;
	int			current_list;

	/* Counts */
	double		heap_tuples;
	double		index_tuples;

	/* Distance function for assignment */
	double		(*distfunc)(const Vector *a, const Vector *b);
} IvfflatBuildState;

/* ----------
 * GUC variable (declared in src/include/access/ivfflat_guc.h)
 * ----------
 */
#include "access/ivfflat_guc.h"

/* ----------
 * Function prototypes -- ivfflatutils.c
 * ----------
 */
extern void ivfflat_page_init(Page page, uint16 page_type);
extern double ivfflat_get_distance(StrategyNumber strategy,
								   const Vector *a, const Vector *b);
typedef double (*IvfflatDistFunc)(const Vector *a, const Vector *b);
extern IvfflatDistFunc ivfflat_get_distfunc(StrategyNumber strategy);

/* ----------
 * Function prototypes -- ivfflatbuild.c
 * ----------
 */
extern IndexBuildResult *ivfflatbuild(Relation heap, Relation index,
									  struct IndexInfo *indexInfo);
extern void ivfflatbuildempty(Relation index);

/* ----------
 * Function prototypes -- ivfflatinsert.c
 * ----------
 */
extern bool ivfflatinsert(Relation index, Datum *values, bool *isnull,
						  ItemPointer ht_ctid, Relation heapRel,
						  IndexUniqueCheck checkUnique,
						  bool indexUnchanged,
						  struct IndexInfo *indexInfo);

/* ----------
 * Function prototypes -- ivfflatscan.c
 * ----------
 */
extern IndexScanDesc ivfflatbeginscan(Relation index, int nkeys,
									  int norderbys);
extern void ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
						  ScanKey orderbys, int norderbys);
extern bool ivfflatgettuple(IndexScanDesc scan, ScanDirection dir);
extern void ivfflatendscan(IndexScanDesc scan);

/* ----------
 * Function prototypes -- ivfflatvacuum.c
 * ----------
 */
extern IndexBulkDeleteResult *ivfflatbulkdelete(IndexVacuumInfo *info,
												IndexBulkDeleteResult *stats,
												IndexBulkDeleteCallback callback,
												void *callback_state);
extern IndexBulkDeleteResult *ivfflatvacuumcleanup(IndexVacuumInfo *info,
												   IndexBulkDeleteResult *stats);

/* ----------
 * Function prototypes -- ivfflatcost.c
 * ----------
 */
extern void ivfflatcostestimate(struct PlannerInfo *root,
								struct IndexPath *path,
								double loop_count,
								Cost *indexStartupCost,
								Cost *indexTotalCost,
								Selectivity *indexSelectivity,
								double *indexCorrelation,
								double *indexPages);

/* ----------
 * Function prototypes -- ivfflatvalidate.c
 * ----------
 */
extern bool ivfflatvalidate(Oid opclassoid);

/* ----------
 * Function prototypes -- ivfflatoptions.c
 * ----------
 */
extern bytea *ivfflatoptions(Datum reloptions, bool validate);

#endif							/* IVFFLAT_H */
