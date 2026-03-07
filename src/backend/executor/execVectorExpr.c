/*-------------------------------------------------------------------------
 *
 * execVectorExpr.c
 *	  Vectorized (batch) expression evaluation for the vectorized
 *	  execution engine.
 *
 * This file implements a small bytecode interpreter that evaluates
 * filter predicates and projection expressions over a VectorBatch
 * (columnar, 1024-row batch) using SIMD intrinsics where available.
 *
 * Supported SIMD back-ends:
 *   - x86-64 AVX2  (256-bit, 4x double / 8x float / 4x int64 / 8x int32)
 *   - ARM64 NEON   (128-bit, 2x double / 4x float / 2x int64 / 4x int32)
 *   - Scalar fallback (all platforms)
 *
 * Scope of vectorized operators (Phase 2.1):
 *   - Arithmetic: +, -, *, / on int4, int8, float4, float8
 *   - Comparison: =, <>, <, <=, >, >= on int4, int8, float4, float8
 *   - Boolean:    AND, OR, NOT
 *   - Filter:     compact a batch according to a boolean predicate column
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execVectorExpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/execVectorExpr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* -----------------------------------------------------------------------
 * SIMD abstraction layer
 * -----------------------------------------------------------------------
 *
 * We define thin wrappers so the evaluation loops can be written once.
 * When neither AVX2 nor NEON is available we fall back to plain C loops
 * which modern compilers auto-vectorize reasonably well.
 * -----------------------------------------------------------------------
 */

#if defined(__x86_64__) && defined(__AVX2__)
#include <immintrin.h>
#define HAVE_AVX2 1
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#define HAVE_NEON 1
#endif


/* -----------------------------------------------------------------------
 * VectorBatch lifecycle
 * -----------------------------------------------------------------------
 */

/*
 * CreateVectorBatch
 *
 * Allocate a fresh VectorBatch with room for VECTOR_BATCH_SIZE rows and
 * the columns described by tupdesc.  Columns are typed from the TupleDesc.
 */
VectorBatch *
CreateVectorBatch(int ncols, TupleDesc tupdesc)
{
	VectorBatch *batch;
	int			i;

	batch = (VectorBatch *) palloc0(sizeof(VectorBatch));
	batch->ncols = ncols;
	batch->nrows = 0;
	batch->nsel = 0;

	/* selection vector */
	batch->sel = (int *) palloc(sizeof(int) * VECTOR_BATCH_SIZE);

	/* columns */
	batch->cols = (VectorColumn *) palloc0(sizeof(VectorColumn) * ncols);

	for (i = 0; i < ncols; i++)
	{
		Form_pg_attribute attr = (i < tupdesc->natts)
			? TupleDescAttr(tupdesc, i)
			: NULL;

		if (attr)
		{
			batch->cols[i].typid = attr->atttypid;
			batch->cols[i].typlen = attr->attlen;
			batch->cols[i].typbyval = attr->attbyval;
		}
		else
		{
			/* working column with unknown type -- set later */
			batch->cols[i].typid = InvalidOid;
			batch->cols[i].typlen = sizeof(Datum);
			batch->cols[i].typbyval = true;
		}

		batch->cols[i].values = (Datum *) palloc0(sizeof(Datum) * VECTOR_BATCH_SIZE);
		batch->cols[i].nulls = (bool *) palloc0(sizeof(bool) * VECTOR_BATCH_SIZE);
	}

	return batch;
}

/*
 * ResetVectorBatch -- prepare for reuse.
 */
void
ResetVectorBatch(VectorBatch *batch)
{
	batch->nrows = 0;
	batch->nsel = 0;
}

/*
 * FreeVectorBatch -- release all memory.
 */
void
FreeVectorBatch(VectorBatch *batch)
{
	int			i;

	if (batch == NULL)
		return;

	for (i = 0; i < batch->ncols; i++)
	{
		if (batch->cols[i].values)
			pfree(batch->cols[i].values);
		if (batch->cols[i].nulls)
			pfree(batch->cols[i].nulls);
	}
	if (batch->cols)
		pfree(batch->cols);
	if (batch->sel)
		pfree(batch->sel);
	pfree(batch);
}


/* -----------------------------------------------------------------------
 * Expression vectorizability check
 * -----------------------------------------------------------------------
 */

/*
 * Mapping from built-in function OIDs to vectorized opcodes.  We keep this
 * as a simple static table checked via linear scan -- the table is tiny.
 */
typedef struct VecOpMapping
{
	Oid			funcid;
	VecExprOpcode opcode;
} VecOpMapping;

/*
 * Built-in operator function OIDs.  These are the regproc OIDs for the
 * common numeric operators in pg_proc.  We define them as macros so the
 * table below is readable.  Values taken from the bootstrap catalog.
 */
#define F_INT4PL	177
#define F_INT4MI	181
#define F_INT4MUL	141
#define F_INT4EQ	65
#define F_INT4NE	144
#define F_INT4LT	66
#define F_INT4LE	149
#define F_INT4GT	147
#define F_INT4GE	150

#define F_INT8PL	463
#define F_INT8MI	464
#define F_INT8MUL	465
#define F_INT8EQ	467
#define F_INT8NE	468
#define F_INT8LT	469
#define F_INT8LE	470
#define F_INT8GT	471
#define F_INT8GE	472

#define F_FLOAT4PL	204
#define F_FLOAT4MI	205
#define F_FLOAT4MUL	202
#define F_FLOAT4DIV	203
#define F_FLOAT4EQ	287
#define F_FLOAT4NE	288
#define F_FLOAT4LT	289
#define F_FLOAT4LE	290
#define F_FLOAT4GT	291
#define F_FLOAT4GE	292

#define F_FLOAT8PL	218
#define F_FLOAT8MI	219
#define F_FLOAT8MUL	216
#define F_FLOAT8DIV	217
#define F_FLOAT8EQ	293
#define F_FLOAT8NE	294
#define F_FLOAT8LT	295
#define F_FLOAT8LE	296
#define F_FLOAT8GT	297
#define F_FLOAT8GE	298

static const VecOpMapping vec_op_map[] =
{
	/* int4 arithmetic */
	{F_INT4PL,	VEC_OP_INT4_ADD},
	{F_INT4MI,	VEC_OP_INT4_SUB},
	{F_INT4MUL, VEC_OP_INT4_MUL},

	/* int8 arithmetic */
	{F_INT8PL,	VEC_OP_INT8_ADD},
	{F_INT8MI,	VEC_OP_INT8_SUB},
	{F_INT8MUL, VEC_OP_INT8_MUL},

	/* float4 arithmetic */
	{F_FLOAT4PL,  VEC_OP_FLOAT4_ADD},
	{F_FLOAT4MI,  VEC_OP_FLOAT4_SUB},
	{F_FLOAT4MUL, VEC_OP_FLOAT4_MUL},
	{F_FLOAT4DIV, VEC_OP_FLOAT4_DIV},

	/* float8 arithmetic */
	{F_FLOAT8PL,  VEC_OP_FLOAT8_ADD},
	{F_FLOAT8MI,  VEC_OP_FLOAT8_SUB},
	{F_FLOAT8MUL, VEC_OP_FLOAT8_MUL},
	{F_FLOAT8DIV, VEC_OP_FLOAT8_DIV},

	/* int4 comparison */
	{F_INT4EQ, VEC_OP_INT4_EQ},
	{F_INT4NE, VEC_OP_INT4_NE},
	{F_INT4LT, VEC_OP_INT4_LT},
	{F_INT4LE, VEC_OP_INT4_LE},
	{F_INT4GT, VEC_OP_INT4_GT},
	{F_INT4GE, VEC_OP_INT4_GE},

	/* int8 comparison */
	{F_INT8EQ, VEC_OP_INT8_EQ},
	{F_INT8NE, VEC_OP_INT8_NE},
	{F_INT8LT, VEC_OP_INT8_LT},
	{F_INT8LE, VEC_OP_INT8_LE},
	{F_INT8GT, VEC_OP_INT8_GT},
	{F_INT8GE, VEC_OP_INT8_GE},

	/* float4 comparison */
	{F_FLOAT4EQ, VEC_OP_FLOAT4_EQ},
	{F_FLOAT4NE, VEC_OP_FLOAT4_NE},
	{F_FLOAT4LT, VEC_OP_FLOAT4_LT},
	{F_FLOAT4LE, VEC_OP_FLOAT4_LE},
	{F_FLOAT4GT, VEC_OP_FLOAT4_GT},
	{F_FLOAT4GE, VEC_OP_FLOAT4_GE},

	/* float8 comparison */
	{F_FLOAT8EQ, VEC_OP_FLOAT8_EQ},
	{F_FLOAT8NE, VEC_OP_FLOAT8_NE},
	{F_FLOAT8LT, VEC_OP_FLOAT8_LT},
	{F_FLOAT8LE, VEC_OP_FLOAT8_LE},
	{F_FLOAT8GT, VEC_OP_FLOAT8_GT},
	{F_FLOAT8GE, VEC_OP_FLOAT8_GE},

	/* sentinel */
	{InvalidOid, VEC_OP_DONE}
};

/*
 * LookupVecOpcode
 *
 * Return the vectorized opcode for the given function OID, or VEC_OP_DONE
 * if no mapping exists.
 */
static VecExprOpcode
LookupVecOpcode(Oid funcid)
{
	const VecOpMapping *m;

	for (m = vec_op_map; m->funcid != InvalidOid; m++)
	{
		if (m->funcid == funcid)
			return m->opcode;
	}
	return VEC_OP_DONE;
}

/*
 * is_vectorizable_type
 *
 * Return true if the given type OID is one of the numeric types that
 * our vectorized engine can handle.
 */
static bool
is_vectorizable_type(Oid typid)
{
	return (typid == INT4OID ||
			typid == INT8OID ||
			typid == FLOAT4OID ||
			typid == FLOAT8OID ||
			typid == BOOLOID);
}

/*
 * is_vectorizable_expr_walker
 *
 * Recursive check: returns true if the expression tree is fully
 * vectorizable by our engine.
 */
static bool
is_vectorizable_expr(Node *node)
{
	if (node == NULL)
		return true;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		return is_vectorizable_type(var->vartype);
	}

	if (IsA(node, Const))
	{
		Const	   *c = (Const *) node;

		return is_vectorizable_type(c->consttype);
	}

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		VecExprOpcode opc;

		opc = LookupVecOpcode(op->opfuncid);
		if (opc == VEC_OP_DONE)
			return false;

		/* recursively check arguments */
		return is_vectorizable_expr((Node *) op->args);
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr   *boolexpr = (BoolExpr *) node;
		ListCell   *lc;

		foreach(lc, boolexpr->args)
		{
			if (!is_vectorizable_expr((Node *) lfirst(lc)))
				return false;
		}
		return true;
	}

	if (IsA(node, List))
	{
		ListCell   *lc;

		foreach(lc, (List *) node)
		{
			if (!is_vectorizable_expr((Node *) lfirst(lc)))
				return false;
		}
		return true;
	}

	if (IsA(node, TargetEntry))
	{
		TargetEntry *tle = (TargetEntry *) node;

		return is_vectorizable_expr((Node *) tle->expr);
	}

	/* anything else is not vectorizable */
	return false;
}

/*
 * VecExprIsVectorizable
 *
 * Public entry point: return true if the given qual list and target list
 * can be fully handled by the vectorized engine.
 */
bool
VecExprIsVectorizable(List *quals, List *targetlist)
{
	ListCell   *lc;

	/* Check quals */
	foreach(lc, quals)
	{
		Node	   *qual = (Node *) lfirst(lc);

		if (!is_vectorizable_expr(qual))
			return false;

		/* Reject volatile functions */
		if (contain_volatile_functions(qual))
			return false;
	}

	/* Check target list */
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!is_vectorizable_expr((Node *) tle->expr))
			return false;
	}

	return true;
}


/* -----------------------------------------------------------------------
 * Expression compilation
 * -----------------------------------------------------------------------
 *
 * We compile a List of qual expressions and a target list into a flat
 * array of VecExprStep instructions.  Column indices in the working
 * batch are assigned as follows:
 *
 *   [0 .. scandesc->natts-1]   -- scan columns (filled by the scan node)
 *   [scandesc->natts .. ]      -- intermediate / result working columns
 *
 * The result column of the last step in the target list is the output.
 * -----------------------------------------------------------------------
 */

/* Compilation context */
typedef struct VecCompileCtx
{
	VecExprStep *steps;
	int			nsteps;
	int			maxsteps;
	int			next_workcol;	/* next free working column index */
	int			scan_natts;		/* number of scan-level columns */
} VecCompileCtx;

static int	vec_compile_expr(VecCompileCtx *ctx, Node *node);

static void
vec_emit(VecCompileCtx *ctx, VecExprStep *step)
{
	if (ctx->nsteps >= ctx->maxsteps)
	{
		ctx->maxsteps *= 2;
		ctx->steps = (VecExprStep *)
			repalloc(ctx->steps, sizeof(VecExprStep) * ctx->maxsteps);
	}
	ctx->steps[ctx->nsteps++] = *step;
}

static int
vec_alloc_col(VecCompileCtx *ctx)
{
	return ctx->next_workcol++;
}

/*
 * vec_compile_expr
 *
 * Compile a single expression node, returning the working column index
 * where its result will be stored.
 */
static int
vec_compile_expr(VecCompileCtx *ctx, Node *node)
{
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		VecExprStep step;
		int			col;

		/*
		 * For scan Vars we reference the scan column directly rather than
		 * emitting a copy instruction -- the data is already in the batch.
		 */
		col = var->varattno - 1;	/* 0-based */
		if (col >= 0 && col < ctx->scan_natts)
			return col;

		/* Otherwise emit a SCAN_VAR copy step */
		col = vec_alloc_col(ctx);
		memset(&step, 0, sizeof(step));
		step.opcode = VEC_OP_SCAN_VAR;
		step.result_col = col;
		step.d.scan_var.attnum = var->varattno - 1;
		vec_emit(ctx, &step);
		return col;
	}

	if (IsA(node, Const))
	{
		Const	   *c = (Const *) node;
		VecExprStep step;
		int			col;

		col = vec_alloc_col(ctx);
		memset(&step, 0, sizeof(step));
		step.opcode = VEC_OP_CONST;
		step.result_col = col;
		step.d.constval.constval = c->constvalue;
		step.d.constval.constisnull = c->constisnull;
		vec_emit(ctx, &step);
		return col;
	}

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		VecExprOpcode opc;
		VecExprStep step;
		int			left_col,
					right_col,
					res_col;

		opc = LookupVecOpcode(op->opfuncid);
		Assert(opc != VEC_OP_DONE);

		left_col = vec_compile_expr(ctx, (Node *) linitial(op->args));
		right_col = vec_compile_expr(ctx, (Node *) lsecond(op->args));
		res_col = vec_alloc_col(ctx);

		memset(&step, 0, sizeof(step));
		step.opcode = opc;
		step.result_col = res_col;
		step.d.binop.left_col = left_col;
		step.d.binop.right_col = right_col;
		vec_emit(ctx, &step);
		return res_col;
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr   *boolexpr = (BoolExpr *) node;
		ListCell   *lc;
		int			prev_col = -1;

		if (boolexpr->boolop == NOT_EXPR)
		{
			VecExprStep step;
			int			arg_col,
						res_col;

			arg_col = vec_compile_expr(ctx,
									   (Node *) linitial(boolexpr->args));
			res_col = vec_alloc_col(ctx);
			memset(&step, 0, sizeof(step));
			step.opcode = VEC_OP_BOOL_NOT;
			step.result_col = res_col;
			step.d.unop.arg_col = arg_col;
			vec_emit(ctx, &step);
			return res_col;
		}

		/* AND / OR: chain pairwise */
		foreach(lc, boolexpr->args)
		{
			int			cur_col = vec_compile_expr(ctx, (Node *) lfirst(lc));

			if (prev_col < 0)
			{
				prev_col = cur_col;
			}
			else
			{
				VecExprStep step;
				int			res_col;

				res_col = vec_alloc_col(ctx);
				memset(&step, 0, sizeof(step));
				step.opcode = (boolexpr->boolop == AND_EXPR)
					? VEC_OP_BOOL_AND
					: VEC_OP_BOOL_OR;
				step.result_col = res_col;
				step.d.binop.left_col = prev_col;
				step.d.binop.right_col = cur_col;
				vec_emit(ctx, &step);
				prev_col = res_col;
			}
		}
		return prev_col;
	}

	if (IsA(node, TargetEntry))
	{
		TargetEntry *tle = (TargetEntry *) node;

		return vec_compile_expr(ctx, (Node *) tle->expr);
	}

	/* Should not reach here for verified-vectorizable expressions */
	elog(ERROR, "vectorized expression compiler: unsupported node type %d",
		 (int) nodeTag(node));
	return -1;					/* keep compiler quiet */
}

/*
 * VecExprCompile
 *
 * Compile a combined filter + projection program.  Returns a VecExprState
 * and sets *out_ncols to the number of working columns needed.
 *
 * The program is laid out as:
 *   1. Evaluate each qual and emit a FILTER step.
 *   2. Evaluate each target-list entry (projection).
 */
VecExprState *
VecExprCompile(List *quals, List *targetlist,
			   TupleDesc scandesc, int *out_ncols)
{
	VecCompileCtx ctx;
	VecExprState *vstate;
	ListCell   *lc;
	VecExprStep done_step;

	memset(&ctx, 0, sizeof(ctx));
	ctx.maxsteps = 64;
	ctx.steps = (VecExprStep *) palloc(sizeof(VecExprStep) * ctx.maxsteps);
	ctx.nsteps = 0;
	ctx.scan_natts = scandesc->natts;
	ctx.next_workcol = scandesc->natts;

	/* Compile quals and emit filter steps */
	foreach(lc, quals)
	{
		Node	   *qual = (Node *) lfirst(lc);
		int			pred_col;
		VecExprStep step;

		pred_col = vec_compile_expr(&ctx, qual);

		memset(&step, 0, sizeof(step));
		step.opcode = VEC_OP_FILTER;
		step.result_col = -1;	/* filter modifies the selection vector */
		step.d.filter.pred_col = pred_col;
		vec_emit(&ctx, &step);
	}

	/* Compile target list entries */
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;

		/* The compilation will place the result in a working column */
		(void) vec_compile_expr(&ctx, (Node *) tle);
	}

	/* Emit DONE sentinel */
	memset(&done_step, 0, sizeof(done_step));
	done_step.opcode = VEC_OP_DONE;
	vec_emit(&ctx, &done_step);

	/* Package up the result */
	vstate = (VecExprState *) palloc(sizeof(VecExprState));
	vstate->nsteps = ctx.nsteps;
	vstate->steps = ctx.steps;
	vstate->nworkcols = ctx.next_workcol;

	if (out_ncols)
		*out_ncols = ctx.next_workcol;

	return vstate;
}


/* -----------------------------------------------------------------------
 * SIMD evaluation helpers
 * -----------------------------------------------------------------------
 *
 * Each macro/function processes a tight loop over nrows elements.
 * When the batch has a selection vector, callers should first compact
 * the columns (done by VEC_OP_FILTER) so the inner loops are dense.
 * -----------------------------------------------------------------------
 */

/*
 * Helper: apply a scalar binary operation over arrays.
 * We use macros to avoid code duplication across types.
 */

/* ---------- INT4 arithmetic ---------- */

static void
vec_int4_add(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i = 0;

#ifdef HAVE_AVX2
	for (; i + 7 < nrows; i += 8)
	{
		__m256i va = _mm256_loadu_si256((const __m256i *) &a[i]);
		__m256i vb = _mm256_loadu_si256((const __m256i *) &b[i]);
		/* Datums are int64 on 64-bit; we treat low 32 bits */
		__m256i vr = _mm256_add_epi64(va, vb);
		_mm256_storeu_si256((__m256i *) &dst[i], vr);
	}
#endif
#ifdef HAVE_NEON
	for (; i + 1 < nrows; i += 2)
	{
		int64x2_t va = vld1q_s64((const int64_t *) &a[i]);
		int64x2_t vb = vld1q_s64((const int64_t *) &b[i]);
		int64x2_t vr = vaddq_s64(va, vb);
		vst1q_s64((int64_t *) &dst[i], vr);
	}
#endif
	for (; i < nrows; i++)
	{
		int32		va = DatumGetInt32(a[i]);
		int32		vb = DatumGetInt32(b[i]);

		dst[i] = Int32GetDatum(va + vb);
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_int4_sub(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Int32GetDatum(DatumGetInt32(a[i]) - DatumGetInt32(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_int4_mul(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Int32GetDatum(DatumGetInt32(a[i]) * DatumGetInt32(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

/* ---------- INT8 arithmetic ---------- */

static void
vec_int8_add(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i = 0;

#ifdef HAVE_AVX2
	for (; i + 3 < nrows; i += 4)
	{
		__m256i va = _mm256_loadu_si256((const __m256i *) &a[i]);
		__m256i vb = _mm256_loadu_si256((const __m256i *) &b[i]);
		__m256i vr = _mm256_add_epi64(va, vb);
		_mm256_storeu_si256((__m256i *) &dst[i], vr);
	}
#endif
#ifdef HAVE_NEON
	for (; i + 1 < nrows; i += 2)
	{
		int64x2_t va = vld1q_s64((const int64_t *) &a[i]);
		int64x2_t vb = vld1q_s64((const int64_t *) &b[i]);
		int64x2_t vr = vaddq_s64(va, vb);
		vst1q_s64((int64_t *) &dst[i], vr);
	}
#endif
	for (; i < nrows; i++)
	{
		dst[i] = Int64GetDatum(DatumGetInt64(a[i]) + DatumGetInt64(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_int8_sub(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Int64GetDatum(DatumGetInt64(a[i]) - DatumGetInt64(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_int8_mul(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Int64GetDatum(DatumGetInt64(a[i]) * DatumGetInt64(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

/* ---------- FLOAT4 arithmetic ---------- */

static void
vec_float4_add(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Float4GetDatum(DatumGetFloat4(a[i]) + DatumGetFloat4(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_float4_sub(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Float4GetDatum(DatumGetFloat4(a[i]) - DatumGetFloat4(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_float4_mul(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dst[i] = Float4GetDatum(DatumGetFloat4(a[i]) * DatumGetFloat4(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_float4_div(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		float4		bv = DatumGetFloat4(b[i]);

		if (bv == 0.0f && !bn[i])
			ereport(ERROR,
					(errcode(ERRCODE_DIVISION_BY_ZERO),
					 errmsg("division by zero")));
		dst[i] = Float4GetDatum(DatumGetFloat4(a[i]) / bv);
		dn[i] = an[i] || bn[i];
	}
}

/* ---------- FLOAT8 arithmetic (SIMD where possible) ---------- */

static void
vec_float8_add(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i = 0;

#ifdef HAVE_AVX2
	/* Datum == int64 on 64-bit, but float8 is stored via memcpy/union */
	for (; i + 3 < nrows; i += 4)
	{
		__m256d va = _mm256_loadu_pd((const double *) &a[i]);
		__m256d vb = _mm256_loadu_pd((const double *) &b[i]);
		__m256d vr = _mm256_add_pd(va, vb);
		_mm256_storeu_pd((double *) &dst[i], vr);
	}
#endif
#ifdef HAVE_NEON
	for (; i + 1 < nrows; i += 2)
	{
		float64x2_t va = vld1q_f64((const double *) &a[i]);
		float64x2_t vb = vld1q_f64((const double *) &b[i]);
		float64x2_t vr = vaddq_f64(va, vb);
		vst1q_f64((double *) &dst[i], vr);
	}
#endif
	for (; i < nrows; i++)
	{
		dst[i] = Float8GetDatum(DatumGetFloat8(a[i]) + DatumGetFloat8(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_float8_sub(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i = 0;

#ifdef HAVE_AVX2
	for (; i + 3 < nrows; i += 4)
	{
		__m256d va = _mm256_loadu_pd((const double *) &a[i]);
		__m256d vb = _mm256_loadu_pd((const double *) &b[i]);
		__m256d vr = _mm256_sub_pd(va, vb);
		_mm256_storeu_pd((double *) &dst[i], vr);
	}
#endif
#ifdef HAVE_NEON
	for (; i + 1 < nrows; i += 2)
	{
		float64x2_t va = vld1q_f64((const double *) &a[i]);
		float64x2_t vb = vld1q_f64((const double *) &b[i]);
		float64x2_t vr = vsubq_f64(va, vb);
		vst1q_f64((double *) &dst[i], vr);
	}
#endif
	for (; i < nrows; i++)
	{
		dst[i] = Float8GetDatum(DatumGetFloat8(a[i]) - DatumGetFloat8(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_float8_mul(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i = 0;

#ifdef HAVE_AVX2
	for (; i + 3 < nrows; i += 4)
	{
		__m256d va = _mm256_loadu_pd((const double *) &a[i]);
		__m256d vb = _mm256_loadu_pd((const double *) &b[i]);
		__m256d vr = _mm256_mul_pd(va, vb);
		_mm256_storeu_pd((double *) &dst[i], vr);
	}
#endif
#ifdef HAVE_NEON
	for (; i + 1 < nrows; i += 2)
	{
		float64x2_t va = vld1q_f64((const double *) &a[i]);
		float64x2_t vb = vld1q_f64((const double *) &b[i]);
		float64x2_t vr = vmulq_f64(va, vb);
		vst1q_f64((double *) &dst[i], vr);
	}
#endif
	for (; i < nrows; i++)
	{
		dst[i] = Float8GetDatum(DatumGetFloat8(a[i]) * DatumGetFloat8(b[i]));
		dn[i] = an[i] || bn[i];
	}
}

static void
vec_float8_div(Datum *dst, const Datum *a, const Datum *b,
			   bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i = 0;

#ifdef HAVE_AVX2
	for (; i + 3 < nrows; i += 4)
	{
		__m256d va = _mm256_loadu_pd((const double *) &a[i]);
		__m256d vb = _mm256_loadu_pd((const double *) &b[i]);
		__m256d vr = _mm256_div_pd(va, vb);
		_mm256_storeu_pd((double *) &dst[i], vr);
	}
#endif
#ifdef HAVE_NEON
	for (; i + 1 < nrows; i += 2)
	{
		float64x2_t va = vld1q_f64((const double *) &a[i]);
		float64x2_t vb = vld1q_f64((const double *) &b[i]);
		float64x2_t vr = vdivq_f64(va, vb);
		vst1q_f64((double *) &dst[i], vr);
	}
#endif
	for (; i < nrows; i++)
	{
		float8		bv = DatumGetFloat8(b[i]);

		if (bv == 0.0 && !bn[i])
			ereport(ERROR,
					(errcode(ERRCODE_DIVISION_BY_ZERO),
					 errmsg("division by zero")));
		dst[i] = Float8GetDatum(DatumGetFloat8(a[i]) / bv);
		dn[i] = an[i] || bn[i];
	}
}

/* ---------- Comparison helpers (produce BoolGetDatum) ---------- */

/*
 * Generate comparison functions via macro to avoid repetition.
 */
#define DEFINE_CMP_FUNC(name, ctype, getter, op) \
static void \
name(Datum *dst, const Datum *a, const Datum *b, \
	 bool *dn, const bool *an, const bool *bn, int nrows) \
{ \
	int i; \
	for (i = 0; i < nrows; i++) \
	{ \
		dn[i] = an[i] || bn[i]; \
		if (dn[i]) \
			dst[i] = BoolGetDatum(false); \
		else \
			dst[i] = BoolGetDatum((ctype)getter(a[i]) op (ctype)getter(b[i])); \
	} \
}

DEFINE_CMP_FUNC(vec_int4_eq, int32, DatumGetInt32, ==)
DEFINE_CMP_FUNC(vec_int4_ne, int32, DatumGetInt32, !=)
DEFINE_CMP_FUNC(vec_int4_lt, int32, DatumGetInt32, <)
DEFINE_CMP_FUNC(vec_int4_le, int32, DatumGetInt32, <=)
DEFINE_CMP_FUNC(vec_int4_gt, int32, DatumGetInt32, >)
DEFINE_CMP_FUNC(vec_int4_ge, int32, DatumGetInt32, >=)

DEFINE_CMP_FUNC(vec_int8_eq, int64, DatumGetInt64, ==)
DEFINE_CMP_FUNC(vec_int8_ne, int64, DatumGetInt64, !=)
DEFINE_CMP_FUNC(vec_int8_lt, int64, DatumGetInt64, <)
DEFINE_CMP_FUNC(vec_int8_le, int64, DatumGetInt64, <=)
DEFINE_CMP_FUNC(vec_int8_gt, int64, DatumGetInt64, >)
DEFINE_CMP_FUNC(vec_int8_ge, int64, DatumGetInt64, >=)

DEFINE_CMP_FUNC(vec_float4_eq, float4, DatumGetFloat4, ==)
DEFINE_CMP_FUNC(vec_float4_ne, float4, DatumGetFloat4, !=)
DEFINE_CMP_FUNC(vec_float4_lt, float4, DatumGetFloat4, <)
DEFINE_CMP_FUNC(vec_float4_le, float4, DatumGetFloat4, <=)
DEFINE_CMP_FUNC(vec_float4_gt, float4, DatumGetFloat4, >)
DEFINE_CMP_FUNC(vec_float4_ge, float4, DatumGetFloat4, >=)

DEFINE_CMP_FUNC(vec_float8_eq, float8, DatumGetFloat8, ==)
DEFINE_CMP_FUNC(vec_float8_ne, float8, DatumGetFloat8, !=)
DEFINE_CMP_FUNC(vec_float8_lt, float8, DatumGetFloat8, <)
DEFINE_CMP_FUNC(vec_float8_le, float8, DatumGetFloat8, <=)
DEFINE_CMP_FUNC(vec_float8_gt, float8, DatumGetFloat8, >)
DEFINE_CMP_FUNC(vec_float8_ge, float8, DatumGetFloat8, >=)

/* ---------- Boolean logic ---------- */

static void
vec_bool_and(Datum *dst, const Datum *a, const Datum *b,
			 bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dn[i] = an[i] || bn[i];
		if (dn[i])
			dst[i] = BoolGetDatum(false);
		else
			dst[i] = BoolGetDatum(DatumGetBool(a[i]) && DatumGetBool(b[i]));
	}
}

static void
vec_bool_or(Datum *dst, const Datum *a, const Datum *b,
			bool *dn, const bool *an, const bool *bn, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dn[i] = an[i] || bn[i];
		if (dn[i])
			dst[i] = BoolGetDatum(false);
		else
			dst[i] = BoolGetDatum(DatumGetBool(a[i]) || DatumGetBool(b[i]));
	}
}

static void
vec_bool_not(Datum *dst, const Datum *a,
			 bool *dn, const bool *an, int nrows)
{
	int			i;

	for (i = 0; i < nrows; i++)
	{
		dn[i] = an[i];
		if (dn[i])
			dst[i] = BoolGetDatum(false);
		else
			dst[i] = BoolGetDatum(!DatumGetBool(a[i]));
	}
}


/* -----------------------------------------------------------------------
 * VecExprEval -- main evaluation entry point
 * -----------------------------------------------------------------------
 *
 * Walk through the step array and evaluate each instruction over the
 * entire batch.  The batch must already have its scan columns populated.
 */

/*
 * Ensure the batch has enough working columns.  If the batch was created
 * with fewer columns than the expression program needs, we extend it.
 */
static void
vec_ensure_cols(VectorBatch *batch, int needed)
{
	if (needed <= batch->ncols)
		return;

	batch->cols = (VectorColumn *)
		repalloc(batch->cols, sizeof(VectorColumn) * needed);

	while (batch->ncols < needed)
	{
		VectorColumn *vc = &batch->cols[batch->ncols];

		vc->typid = InvalidOid;
		vc->typlen = sizeof(Datum);
		vc->typbyval = true;
		vc->values = (Datum *) palloc0(sizeof(Datum) * VECTOR_BATCH_SIZE);
		vc->nulls = (bool *) palloc0(sizeof(bool) * VECTOR_BATCH_SIZE);
		batch->ncols++;
	}
}

void
VecExprEval(VecExprState *vstate, VectorBatch *batch)
{
	int			pc;
	int			nrows = batch->nrows;

	if (nrows == 0)
		return;

	/* Make sure the batch has all the working columns we need */
	vec_ensure_cols(batch, vstate->nworkcols);

	for (pc = 0; pc < vstate->nsteps; pc++)
	{
		VecExprStep *step = &vstate->steps[pc];
		VectorColumn *res;

		switch (step->opcode)
		{
			case VEC_OP_DONE:
				return;

			case VEC_OP_SCAN_VAR:
				{
					int			src = step->d.scan_var.attnum;

					res = &batch->cols[step->result_col];
					memcpy(res->values, batch->cols[src].values,
						   sizeof(Datum) * nrows);
					memcpy(res->nulls, batch->cols[src].nulls,
						   sizeof(bool) * nrows);
				}
				break;

			case VEC_OP_CONST:
				{
					int			i;

					res = &batch->cols[step->result_col];
					for (i = 0; i < nrows; i++)
					{
						res->values[i] = step->d.constval.constval;
						res->nulls[i] = step->d.constval.constisnull;
					}
				}
				break;

			/* ---- INT4 arithmetic ---- */
			case VEC_OP_INT4_ADD:
				res = &batch->cols[step->result_col];
				vec_int4_add(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;
			case VEC_OP_INT4_SUB:
				res = &batch->cols[step->result_col];
				vec_int4_sub(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;
			case VEC_OP_INT4_MUL:
				res = &batch->cols[step->result_col];
				vec_int4_mul(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;

			/* ---- INT8 arithmetic ---- */
			case VEC_OP_INT8_ADD:
				res = &batch->cols[step->result_col];
				vec_int8_add(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;
			case VEC_OP_INT8_SUB:
				res = &batch->cols[step->result_col];
				vec_int8_sub(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;
			case VEC_OP_INT8_MUL:
				res = &batch->cols[step->result_col];
				vec_int8_mul(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;

			/* ---- FLOAT4 arithmetic ---- */
			case VEC_OP_FLOAT4_ADD:
				res = &batch->cols[step->result_col];
				vec_float4_add(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;
			case VEC_OP_FLOAT4_SUB:
				res = &batch->cols[step->result_col];
				vec_float4_sub(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;
			case VEC_OP_FLOAT4_MUL:
				res = &batch->cols[step->result_col];
				vec_float4_mul(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;
			case VEC_OP_FLOAT4_DIV:
				res = &batch->cols[step->result_col];
				vec_float4_div(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;

			/* ---- FLOAT8 arithmetic ---- */
			case VEC_OP_FLOAT8_ADD:
				res = &batch->cols[step->result_col];
				vec_float8_add(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;
			case VEC_OP_FLOAT8_SUB:
				res = &batch->cols[step->result_col];
				vec_float8_sub(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;
			case VEC_OP_FLOAT8_MUL:
				res = &batch->cols[step->result_col];
				vec_float8_mul(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;
			case VEC_OP_FLOAT8_DIV:
				res = &batch->cols[step->result_col];
				vec_float8_div(res->values,
							   batch->cols[step->d.binop.left_col].values,
							   batch->cols[step->d.binop.right_col].values,
							   res->nulls,
							   batch->cols[step->d.binop.left_col].nulls,
							   batch->cols[step->d.binop.right_col].nulls,
							   nrows);
				break;

			/* ---- INT4 comparison ---- */
			case VEC_OP_INT4_EQ:
				res = &batch->cols[step->result_col];
				vec_int4_eq(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT4_NE:
				res = &batch->cols[step->result_col];
				vec_int4_ne(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT4_LT:
				res = &batch->cols[step->result_col];
				vec_int4_lt(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT4_LE:
				res = &batch->cols[step->result_col];
				vec_int4_le(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT4_GT:
				res = &batch->cols[step->result_col];
				vec_int4_gt(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT4_GE:
				res = &batch->cols[step->result_col];
				vec_int4_ge(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;

			/* ---- INT8 comparison ---- */
			case VEC_OP_INT8_EQ:
				res = &batch->cols[step->result_col];
				vec_int8_eq(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT8_NE:
				res = &batch->cols[step->result_col];
				vec_int8_ne(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT8_LT:
				res = &batch->cols[step->result_col];
				vec_int8_lt(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT8_LE:
				res = &batch->cols[step->result_col];
				vec_int8_le(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT8_GT:
				res = &batch->cols[step->result_col];
				vec_int8_gt(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;
			case VEC_OP_INT8_GE:
				res = &batch->cols[step->result_col];
				vec_int8_ge(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;

			/* ---- FLOAT4 comparison ---- */
			case VEC_OP_FLOAT4_EQ:
				res = &batch->cols[step->result_col];
				vec_float4_eq(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT4_NE:
				res = &batch->cols[step->result_col];
				vec_float4_ne(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT4_LT:
				res = &batch->cols[step->result_col];
				vec_float4_lt(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT4_LE:
				res = &batch->cols[step->result_col];
				vec_float4_le(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT4_GT:
				res = &batch->cols[step->result_col];
				vec_float4_gt(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT4_GE:
				res = &batch->cols[step->result_col];
				vec_float4_ge(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;

			/* ---- FLOAT8 comparison ---- */
			case VEC_OP_FLOAT8_EQ:
				res = &batch->cols[step->result_col];
				vec_float8_eq(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT8_NE:
				res = &batch->cols[step->result_col];
				vec_float8_ne(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT8_LT:
				res = &batch->cols[step->result_col];
				vec_float8_lt(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT8_LE:
				res = &batch->cols[step->result_col];
				vec_float8_le(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT8_GT:
				res = &batch->cols[step->result_col];
				vec_float8_gt(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;
			case VEC_OP_FLOAT8_GE:
				res = &batch->cols[step->result_col];
				vec_float8_ge(res->values,
							  batch->cols[step->d.binop.left_col].values,
							  batch->cols[step->d.binop.right_col].values,
							  res->nulls,
							  batch->cols[step->d.binop.left_col].nulls,
							  batch->cols[step->d.binop.right_col].nulls,
							  nrows);
				break;

			/* ---- Boolean operators ---- */
			case VEC_OP_BOOL_AND:
				res = &batch->cols[step->result_col];
				vec_bool_and(res->values,
							 batch->cols[step->d.binop.left_col].values,
							 batch->cols[step->d.binop.right_col].values,
							 res->nulls,
							 batch->cols[step->d.binop.left_col].nulls,
							 batch->cols[step->d.binop.right_col].nulls,
							 nrows);
				break;

			case VEC_OP_BOOL_OR:
				res = &batch->cols[step->result_col];
				vec_bool_or(res->values,
							batch->cols[step->d.binop.left_col].values,
							batch->cols[step->d.binop.right_col].values,
							res->nulls,
							batch->cols[step->d.binop.left_col].nulls,
							batch->cols[step->d.binop.right_col].nulls,
							nrows);
				break;

			case VEC_OP_BOOL_NOT:
				res = &batch->cols[step->result_col];
				vec_bool_not(res->values,
							 batch->cols[step->d.unop.arg_col].values,
							 res->nulls,
							 batch->cols[step->d.unop.arg_col].nulls,
							 nrows);
				break;

			/* ---- Filter (compact by predicate) ---- */
			case VEC_OP_FILTER:
				{
					VectorColumn *pred;
					int			new_nrows;
					int			i,
								j,
								c;

					pred = &batch->cols[step->d.filter.pred_col];
					new_nrows = 0;

					/* Build selection vector */
					for (i = 0; i < nrows; i++)
					{
						if (!pred->nulls[i] && DatumGetBool(pred->values[i]))
							batch->sel[new_nrows++] = i;
					}

					/*
					 * Compact all columns in-place using the selection
					 * vector.  This makes subsequent steps operate on a
					 * dense array with no gaps.
					 */
					if (new_nrows < nrows)
					{
						for (c = 0; c < batch->ncols; c++)
						{
							VectorColumn *vc = &batch->cols[c];

							for (j = 0; j < new_nrows; j++)
							{
								int			idx = batch->sel[j];

								if (j != idx)
								{
									vc->values[j] = vc->values[idx];
									vc->nulls[j] = vc->nulls[idx];
								}
							}
						}
					}

					nrows = new_nrows;
					batch->nrows = nrows;
					batch->nsel = nrows;
				}
				break;

			default:
				elog(ERROR, "unrecognized vectorized opcode: %d",
					 (int) step->opcode);
				break;
		}
	}
}
