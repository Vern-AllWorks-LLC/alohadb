/*-------------------------------------------------------------------------
 *
 * model.c
 *		Lightweight gradient-boosted tree for cardinality prediction.
 *
 * Implements a basic gradient-boosted regression tree ensemble trained
 * on (feature_vector, log_actual_rows) pairs collected from executed
 * queries.  The model is stored entirely in shared memory so that all
 * backends can use it for prediction without serialization overhead.
 *
 * The implementation deliberately avoids external dependencies: no
 * XGBoost, no LightGBM.  It is a from-scratch CART-style decision
 * tree builder with squared-error loss and gradient boosting.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_learned_stats/model.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <float.h>
#include <string.h>

#include "learned_stats.h"
#include "utils/memutils.h"

/*
 * Internal structures used only during tree construction.
 * We work with indices into the shared sample array to avoid copying.
 */

/* Scratch space for building one tree.  Allocated in a temp memory context. */
typedef struct BuildContext
{
	int		   *indices;		/* sample indices in current working set */
	int			n_indices;		/* number of valid indices */
	double	   *residuals;		/* current residuals for all samples */
	int			n_samples;		/* total number of samples */
	const LearnedStatsSample *samples;	/* pointer to sample array */
} BuildContext;

/* -------------------------------------------------------------------
 * Helper: safe log2 with floor at 0
 * -------------------------------------------------------------------
 */
static inline double
safe_log2(double v)
{
	if (v <= 0.0)
		return 0.0;
	return log2(v);
}

/* -------------------------------------------------------------------
 * Compute the mean of residuals for a set of sample indices.
 * -------------------------------------------------------------------
 */
static double
compute_mean(const double *residuals, const int *indices, int n)
{
	double		sum = 0.0;
	int			i;

	if (n <= 0)
		return 0.0;
	for (i = 0; i < n; i++)
		sum += residuals[indices[i]];
	return sum / n;
}

/* -------------------------------------------------------------------
 * Compute the sum of squared errors around the mean.
 * -------------------------------------------------------------------
 */
static double
compute_sse(const double *residuals, const int *indices, int n)
{
	double		mean;
	double		sse = 0.0;
	double		diff;
	int			i;

	if (n <= 1)
		return 0.0;

	mean = compute_mean(residuals, indices, n);
	for (i = 0; i < n; i++)
	{
		diff = residuals[indices[i]] - mean;
		sse += diff * diff;
	}
	return sse;
}

/* -------------------------------------------------------------------
 * Find the best split for a set of samples.
 *
 * We enumerate each feature and try a fixed number of candidate
 * thresholds (the distinct values present in the data for that
 * feature).  We pick the split that maximally reduces SSE.
 *
 * Returns true if a valid split was found.
 * -------------------------------------------------------------------
 */
static bool
find_best_split(BuildContext *ctx,
				const int *indices, int n,
				int *best_feature, double *best_threshold,
				int **left_idx, int *left_n,
				int **right_idx, int *right_n)
{
	double		best_gain = 0.0;
	double		parent_sse;
	bool		found = false;
	int			f, i, j;

	/*
	 * Minimum samples to attempt a split.  Prevents overfitting on tiny
	 * partitions.
	 */
	if (n < 4)
		return false;

	parent_sse = compute_sse(ctx->residuals, indices, n);

	/* If there is essentially no variance, don't bother splitting */
	if (parent_sse < 1e-12)
		return false;

	for (f = 0; f < LSTAT_NUM_FEATURES; f++)
	{
		/*
		 * Collect distinct feature values as candidate thresholds.  We use a
		 * simple O(n^2) uniqueness check which is fine for our bounded sample
		 * sizes.
		 */
		double	   *candidates = palloc(sizeof(double) * n);
		int			n_candidates = 0;

		for (i = 0; i < n; i++)
		{
			double		val = ctx->samples[indices[i]].features[f];
			bool		dup = false;

			for (j = 0; j < n_candidates; j++)
			{
				if (fabs(candidates[j] - val) < 1e-12)
				{
					dup = true;
					break;
				}
			}
			if (!dup && n_candidates < LSTAT_MAX_SAMPLES)
				candidates[n_candidates++] = val;
		}

		/* Try each midpoint between consecutive sorted values */
		/* Simple insertion sort for candidates */
		for (i = 1; i < n_candidates; i++)
		{
			double		key = candidates[i];

			j = i - 1;
			while (j >= 0 && candidates[j] > key)
			{
				candidates[j + 1] = candidates[j];
				j--;
			}
			candidates[j + 1] = key;
		}

		for (i = 0; i < n_candidates - 1; i++)
		{
			double		threshold = (candidates[i] + candidates[i + 1]) / 2.0;
			int			ln = 0,
						rn = 0;
			double		left_sse,
						right_sse,
						gain;

			/* Partition indices */
			for (j = 0; j < n; j++)
			{
				if (ctx->samples[indices[j]].features[f] <= threshold)
					ln++;
				else
					rn++;
			}

			if (ln == 0 || rn == 0)
				continue;

			/*
			 * Build temporary left/right index arrays and compute SSE.
			 * We reuse stack-allocated arrays since n <= LSTAT_MAX_SAMPLES.
			 */
			{
				int		   *tmp_left = palloc(sizeof(int) * ln);
				int		   *tmp_right = palloc(sizeof(int) * rn);
				int			li = 0,
							ri = 0;

				for (j = 0; j < n; j++)
				{
					if (ctx->samples[indices[j]].features[f] <= threshold)
						tmp_left[li++] = indices[j];
					else
						tmp_right[ri++] = indices[j];
				}

				left_sse = compute_sse(ctx->residuals, tmp_left, ln);
				right_sse = compute_sse(ctx->residuals, tmp_right, rn);

				gain = parent_sse - left_sse - right_sse;

				if (gain > best_gain)
				{
					best_gain = gain;
					*best_feature = f;
					*best_threshold = threshold;

					/* Free previous best if any */
					if (found)
					{
						pfree(*left_idx);
						pfree(*right_idx);
					}
					*left_idx = tmp_left;
					*left_n = ln;
					*right_idx = tmp_right;
					*right_n = rn;
					found = true;
				}
				else
				{
					pfree(tmp_left);
					pfree(tmp_right);
				}
			}
		}

		pfree(candidates);
	}

	return found;
}

/* -------------------------------------------------------------------
 * Recursively build a decision tree node.
 *
 * Returns the index of the created node in tree->nodes[].
 * -------------------------------------------------------------------
 */
static int
build_tree_recursive(BuildContext *ctx, GBTree *tree,
					 const int *indices, int n, int depth)
{
	int			node_idx;
	int			best_feature = -1;
	double		best_threshold = 0.0;
	int		   *left_idx = NULL;
	int		   *right_idx = NULL;
	int			left_n = 0,
				right_n = 0;

	/* Allocate a node -- check capacity first */
	if (tree->num_nodes >= LSTAT_MAX_NODES)
	{
		/*
		 * Tree is full.  Return -1 to signal that no node could be
		 * created; the caller will treat this as a missing subtree.
		 */
		return -1;
	}

	node_idx = tree->num_nodes++;

	/* Check stopping conditions */
	if (depth >= LSTAT_MAX_DEPTH || n < 4 ||
		!find_best_split(ctx, indices, n,
						 &best_feature, &best_threshold,
						 &left_idx, &left_n,
						 &right_idx, &right_n))
	{
		/* Make a leaf */
		tree->nodes[node_idx].feature_index = -1;
		tree->nodes[node_idx].value = compute_mean(ctx->residuals, indices, n);
		tree->nodes[node_idx].left_child = -1;
		tree->nodes[node_idx].right_child = -1;
		return node_idx;
	}

	/* Internal node */
	tree->nodes[node_idx].feature_index = best_feature;
	tree->nodes[node_idx].threshold = best_threshold;
	tree->nodes[node_idx].value = compute_mean(ctx->residuals, indices, n);

	/* Recurse for children */
	tree->nodes[node_idx].left_child =
		build_tree_recursive(ctx, tree, left_idx, left_n, depth + 1);
	tree->nodes[node_idx].right_child =
		build_tree_recursive(ctx, tree, right_idx, right_n, depth + 1);

	pfree(left_idx);
	pfree(right_idx);

	/*
	 * If either child could not be created (tree full), degrade this node
	 * to a leaf so that prediction traversal never follows a -1 pointer.
	 */
	if (tree->nodes[node_idx].left_child < 0 ||
		tree->nodes[node_idx].right_child < 0)
	{
		tree->nodes[node_idx].feature_index = -1;
		tree->nodes[node_idx].left_child = -1;
		tree->nodes[node_idx].right_child = -1;
	}

	return node_idx;
}

/* -------------------------------------------------------------------
 * Build a single regression tree on the current residuals.
 * -------------------------------------------------------------------
 */
static void
build_one_tree(BuildContext *ctx, GBTree *tree)
{
	int		   *all_indices;
	int			i;

	memset(tree, 0, sizeof(GBTree));
	tree->num_nodes = 0;

	all_indices = palloc(sizeof(int) * ctx->n_indices);
	memcpy(all_indices, ctx->indices, sizeof(int) * ctx->n_indices);

	build_tree_recursive(ctx, tree, all_indices, ctx->n_indices, 0);
	tree->valid = true;

	/* Update residuals: subtract learning_rate * prediction for each sample */
	for (i = 0; i < ctx->n_indices; i++)
	{
		int			idx = ctx->indices[i];
		double		pred;

		/* Traverse the just-built tree to get prediction */
		{
			int			nid = 0;

			while (tree->nodes[nid].feature_index >= 0)
			{
				if (ctx->samples[idx].features[tree->nodes[nid].feature_index]
					<= tree->nodes[nid].threshold)
					nid = tree->nodes[nid].left_child;
				else
					nid = tree->nodes[nid].right_child;

				if (nid < 0 || nid >= tree->num_nodes)
					break;
			}
			pred = (nid >= 0 && nid < tree->num_nodes) ?
				tree->nodes[nid].value : 0.0;
		}

		ctx->residuals[idx] -= LSTAT_LEARNING_RATE * pred;
	}

	pfree(all_indices);
}

/* -------------------------------------------------------------------
 * Predict using a single tree.
 * -------------------------------------------------------------------
 */
static double
predict_one_tree(const GBTree *tree, const double *features)
{
	int			nid = 0;

	if (!tree->valid || tree->num_nodes == 0)
		return 0.0;

	while (tree->nodes[nid].feature_index >= 0)
	{
		if (features[tree->nodes[nid].feature_index]
			<= tree->nodes[nid].threshold)
			nid = tree->nodes[nid].left_child;
		else
			nid = tree->nodes[nid].right_child;

		if (nid < 0 || nid >= tree->num_nodes)
			return 0.0;
	}

	return tree->nodes[nid].value;
}

/* ===================================================================
 * Public API
 * ===================================================================
 */

/*
 * lstat_model_train
 *		Train the gradient-boosted tree ensemble on the samples in the
 *		ring buffer.  Caller must hold shared->lock in exclusive mode.
 *
 * We work in a temporary memory context to avoid leaking during the
 * potentially large number of palloc/pfree calls in tree building.
 */
void
lstat_model_train(LearnedStatsSharedState *shared)
{
	MemoryContext train_ctx;
	MemoryContext old_ctx;
	BuildContext  bctx;
	int			  n_valid;
	int			  i;
	double		  sum_target;
	GBTModel	 *model = &shared->model;

	/* Count valid samples */
	n_valid = 0;
	for (i = 0; i < LSTAT_MAX_SAMPLES; i++)
	{
		if (shared->samples[i].valid)
			n_valid++;
	}

	if (n_valid < LSTAT_MIN_TRAIN_SAMPLES)
	{
		elog(NOTICE, "alohadb_learned_stats: only %d samples, need %d to train",
			 n_valid, LSTAT_MIN_TRAIN_SAMPLES);
		return;
	}

	train_ctx = AllocSetContextCreate(CurrentMemoryContext,
									  "learned_stats_train",
									  ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(train_ctx);

	/* Build index array and compute initial residuals */
	bctx.indices = palloc(sizeof(int) * n_valid);
	bctx.residuals = palloc(sizeof(double) * LSTAT_MAX_SAMPLES);
	bctx.n_indices = 0;
	bctx.n_samples = LSTAT_MAX_SAMPLES;
	bctx.samples = shared->samples;

	sum_target = 0.0;
	for (i = 0; i < LSTAT_MAX_SAMPLES; i++)
	{
		if (shared->samples[i].valid)
		{
			bctx.indices[bctx.n_indices++] = i;
			sum_target += shared->samples[i].log_actual_rows;
		}
	}

	/* Base prediction is the mean of log(actual_rows) */
	model->base_prediction = sum_target / n_valid;
	model->learning_rate = LSTAT_LEARNING_RATE;
	model->num_trees = 0;

	/* Initialize residuals = target - base_prediction */
	for (i = 0; i < bctx.n_indices; i++)
	{
		int			idx = bctx.indices[i];

		bctx.residuals[idx] =
			shared->samples[idx].log_actual_rows - model->base_prediction;
	}

	/* Iteratively add trees */
	for (i = 0; i < LSTAT_MAX_TREES; i++)
	{
		double		max_abs_resid = 0.0;
		int			j;

		/* Check if residuals are small enough to stop early */
		for (j = 0; j < bctx.n_indices; j++)
		{
			double		absval = fabs(bctx.residuals[bctx.indices[j]]);

			if (absval > max_abs_resid)
				max_abs_resid = absval;
		}
		if (max_abs_resid < 0.01)
			break;

		build_one_tree(&bctx, &model->trees[i]);
		model->num_trees = i + 1;
	}

	model->trained = true;
	shared->last_train_time = GetCurrentTimestamp();

	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(train_ctx);

	elog(LOG, "alohadb_learned_stats: trained model with %d trees on %d samples",
		 model->num_trees, n_valid);
}

/*
 * lstat_model_predict
 *		Predict log2(rows) for the given feature vector using the
 *		trained ensemble.
 *
 * Returns the predicted log2(rows).  Sets *confidence to a value in
 * [0, 1] indicating how reliable the prediction is.  Confidence is
 * based on the variance of individual tree predictions; high variance
 * means low confidence.
 */
double
lstat_model_predict(const GBTModel *model,
					const double *features,
					double *confidence)
{
	double		prediction;
	double		sum_sq = 0.0;
	double		mean_tree_pred;
	double		variance;
	int			i;

	if (!model->trained || model->num_trees == 0)
	{
		*confidence = 0.0;
		return 0.0;
	}

	/* Sum base prediction + learning_rate * sum of tree predictions */
	prediction = model->base_prediction;
	mean_tree_pred = 0.0;

	for (i = 0; i < model->num_trees; i++)
	{
		double		tp = predict_one_tree(&model->trees[i], features);

		prediction += model->learning_rate * tp;
		mean_tree_pred += tp;
		sum_sq += tp * tp;
	}

	/* Compute confidence from inter-tree agreement */
	if (model->num_trees > 1)
	{
		mean_tree_pred /= model->num_trees;
		variance = (sum_sq / model->num_trees) - (mean_tree_pred * mean_tree_pred);

		/*
		 * Map variance to confidence: confidence = 1 / (1 + variance). This
		 * gives 1.0 when trees perfectly agree and approaches 0 as variance
		 * grows.
		 */
		if (variance < 0.0)
			variance = 0.0;
		*confidence = 1.0 / (1.0 + variance);
	}
	else
	{
		/* Single tree: moderate confidence */
		*confidence = 0.5;
	}

	return prediction;
}

/*
 * lstat_model_reset
 *		Reset the model to untrained state.
 *		Caller must hold the lock in exclusive mode.
 */
void
lstat_model_reset(GBTModel *model)
{
	memset(model, 0, sizeof(GBTModel));
	model->trained = false;
	model->num_trees = 0;
	model->base_prediction = 0.0;
	model->learning_rate = LSTAT_LEARNING_RATE;
}
