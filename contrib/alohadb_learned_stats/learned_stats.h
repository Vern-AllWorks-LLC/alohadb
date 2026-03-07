/*-------------------------------------------------------------------------
 *
 * learned_stats.h
 *		ML-Assisted Cardinality Estimation for PostgreSQL
 *
 * This extension uses a lightweight gradient-boosted tree model to
 * improve cardinality estimates for complex predicate queries and
 * vector similarity searches.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_learned_stats/learned_stats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEARNED_STATS_H
#define LEARNED_STATS_H

#include "postgres.h"
#include "nodes/pathnodes.h"
#include "nodes/parsenodes.h"
#include "utils/selfuncs.h"
#include "utils/timestamp.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/*
 * Configuration constants
 */
#define LSTAT_MAX_SAMPLES			4096	/* ring buffer capacity */
#define LSTAT_NUM_FEATURES			8		/* feature vector width */
#define LSTAT_MAX_TREES				32		/* max trees in ensemble */
#define LSTAT_MAX_DEPTH				6		/* max depth per tree */
#define LSTAT_MAX_NODES				127		/* max nodes per tree (2^depth - 1) */
#define LSTAT_MIN_TRAIN_SAMPLES		64		/* minimum samples before training */
#define LSTAT_LEARNING_RATE			0.1		/* gradient boosting step size */
#define LSTAT_CONFIDENCE_THRESHOLD	0.3		/* min confidence to use model */

/*
 * Feature indices for the feature vector.
 * These correspond to the columns in our training data.
 */
#define FEAT_LOG_TABLE_SIZE			0	/* log2(relation pages) */
#define FEAT_LOG_NTUPLES			1	/* log2(relation tuples) */
#define FEAT_NUM_PREDICATES			2	/* number of restriction clauses */
#define FEAT_PREDICATE_TYPE			3	/* encoded predicate type mix */
#define FEAT_N_DISTINCT				4	/* stadistinct from pg_statistic */
#define FEAT_CORRELATION			5	/* stacorrelation */
#define FEAT_LOG_ESTIMATED			6	/* log2(planner estimated rows) */
#define FEAT_INDEX_SCAN				7	/* 1.0 if index scan, else 0.0 */

/*
 * Predicate type encoding values, combined by OR-ing.
 */
#define PRED_TYPE_EQUALITY			0x01
#define PRED_TYPE_RANGE				0x02
#define PRED_TYPE_LIKE				0x04
#define PRED_TYPE_VECTOR_SIM		0x08
#define PRED_TYPE_FUNCTION			0x10
#define PRED_TYPE_OTHER				0x20

/*
 * A single decision tree node.
 * Internal nodes split on a feature; leaf nodes hold a prediction value.
 */
typedef struct GBTreeNode
{
	int			feature_index;	/* which feature to split on (-1 = leaf) */
	double		threshold;		/* split threshold for internal nodes */
	double		value;			/* prediction value (used in leaves, also as
								 * node value for computing gradients) */
	int			left_child;		/* index of left child (-1 = none) */
	int			right_child;	/* index of right child (-1 = none) */
} GBTreeNode;

/*
 * A single decision tree in the ensemble.
 */
typedef struct GBTree
{
	GBTreeNode	nodes[LSTAT_MAX_NODES];
	int			num_nodes;		/* how many nodes are populated */
	bool		valid;			/* true if tree has been trained */
} GBTree;

/*
 * The full gradient-boosted tree ensemble model.
 */
typedef struct GBTModel
{
	GBTree		trees[LSTAT_MAX_TREES];
	int			num_trees;		/* number of trained trees */
	double		base_prediction;	/* initial constant prediction (mean) */
	bool		trained;		/* true once at least one tree is trained */
	double		learning_rate;
} GBTModel;

/*
 * A single training sample stored in the ring buffer.
 * Records the features at plan time and the actual row count at
 * execution time for later model training.
 */
typedef struct LearnedStatsSample
{
	double		features[LSTAT_NUM_FEATURES];
	double		log_actual_rows;	/* log2(actual_rows + 1) */
	Oid			relid;				/* relation OID */
	bool		valid;				/* entry has been filled */
} LearnedStatsSample;

/*
 * Shared-memory state for the extension.
 * Protected by the named LWLock.
 */
typedef struct LearnedStatsSharedState
{
	LWLock	   *lock;

	/* Ring buffer of training samples */
	LearnedStatsSample samples[LSTAT_MAX_SAMPLES];
	int			sample_head;		/* next write position */
	int			num_samples;		/* total valid samples (capped) */

	/* The trained model */
	GBTModel	model;

	/* Metadata */
	TimestampTz last_train_time;	/* when the model was last trained */
	int			total_queries_logged;
	int			total_predictions_made;
	int			total_fallbacks;		/* times we fell back to standard */
	bool		enabled;				/* runtime enable/disable */
} LearnedStatsSharedState;

/* ---------------------------------------------------------------
 * Function prototypes: model.c
 * ---------------------------------------------------------------
 */

/* Train the ensemble on the current ring buffer samples */
extern void lstat_model_train(LearnedStatsSharedState *shared);

/* Predict log2(rows) for a feature vector; returns confidence [0,1] */
extern double lstat_model_predict(const GBTModel *model,
								  const double *features,
								  double *confidence);

/* Reset the model to untrained state */
extern void lstat_model_reset(GBTModel *model);

/* ---------------------------------------------------------------
 * Function prototypes: learned_stats.c
 * ---------------------------------------------------------------
 */

/* Shared state pointer (set during shmem startup) */
extern LearnedStatsSharedState *lstat_shared;

/* GUC variable */
extern bool lstat_enabled;

/* Shared memory sizing */
extern Size lstat_shmem_size(void);

#endif							/* LEARNED_STATS_H */
