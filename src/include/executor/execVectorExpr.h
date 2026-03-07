/*-------------------------------------------------------------------------
 *
 * execVectorExpr.h
 *	  Vectorized batch expression evaluation types and function signatures.
 *
 * This header defines the columnar batch representation used by the
 * vectorized execution engine, together with batch-level expression
 * evaluation helpers that process 1024 tuples at a time through SIMD
 * (AVX2/NEON) or scalar fallback code paths.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execVectorExpr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXEC_VECTOR_EXPR_H
#define EXEC_VECTOR_EXPR_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/tuplestore.h"

/*
 * Maximum number of tuples processed per batch.  Must be a power of 2 and
 * a multiple of the widest SIMD register width (256 bits = 4 doubles).
 */
#define VECTOR_BATCH_SIZE		1024

/*
 * VectorColumn
 *
 * A single column of a VectorBatch stored in a dense, type-homogeneous
 * array.  For fixed-width types (int4, int8, float4, float8) the values
 * array is directly usable by SIMD instructions; for other types each
 * entry is a Datum that must be interpreted per the column's type OID.
 */
typedef struct VectorColumn
{
	Oid			typid;			/* pg type OID for this column */
	int16		typlen;			/* pg_type.typlen */
	bool		typbyval;		/* pg_type.typbyval */
	Datum	   *values;			/* palloc'd array [VECTOR_BATCH_SIZE] */
	bool	   *nulls;			/* palloc'd array [VECTOR_BATCH_SIZE] */
} VectorColumn;

/*
 * VectorBatch
 *
 * Represents up to VECTOR_BATCH_SIZE tuples in columnar layout.
 *
 * nrows  -- actual number of valid rows (0..VECTOR_BATCH_SIZE)
 * ncols  -- number of columns
 * sel    -- optional selection vector; when non-NULL only the row indices
 *           listed in sel[0..nsel-1] are "active" (pass the filter).
 * nsel   -- number of entries in sel (ignored when sel == NULL)
 * cols   -- array of VectorColumn, one per output column
 */
typedef struct VectorBatch
{
	int			nrows;			/* rows populated in the columns */
	int			ncols;			/* number of columns */
	int		   *sel;			/* selection vector (palloc'd) */
	int			nsel;			/* number of selected rows */
	VectorColumn *cols;			/* palloc'd array of VectorColumn */
} VectorBatch;

/*
 * Vectorized operation opcodes for the batch expression evaluator.
 */
typedef enum VecExprOpcode
{
	VEC_OP_DONE = 0,			/* sentinel / end of program */

	/* Column references ---------------------------------------------------*/
	VEC_OP_SCAN_VAR,			/* copy a scan-tuple column into result col */

	/* Constants -----------------------------------------------------------*/
	VEC_OP_CONST,				/* broadcast a constant into result col */

	/* Arithmetic on int4 --------------------------------------------------*/
	VEC_OP_INT4_ADD,
	VEC_OP_INT4_SUB,
	VEC_OP_INT4_MUL,

	/* Arithmetic on int8 --------------------------------------------------*/
	VEC_OP_INT8_ADD,
	VEC_OP_INT8_SUB,
	VEC_OP_INT8_MUL,

	/* Arithmetic on float4 ------------------------------------------------*/
	VEC_OP_FLOAT4_ADD,
	VEC_OP_FLOAT4_SUB,
	VEC_OP_FLOAT4_MUL,
	VEC_OP_FLOAT4_DIV,

	/* Arithmetic on float8 ------------------------------------------------*/
	VEC_OP_FLOAT8_ADD,
	VEC_OP_FLOAT8_SUB,
	VEC_OP_FLOAT8_MUL,
	VEC_OP_FLOAT8_DIV,

	/* Comparison operators (produce bool column) --------------------------*/
	VEC_OP_INT4_EQ,
	VEC_OP_INT4_NE,
	VEC_OP_INT4_LT,
	VEC_OP_INT4_LE,
	VEC_OP_INT4_GT,
	VEC_OP_INT4_GE,

	VEC_OP_INT8_EQ,
	VEC_OP_INT8_NE,
	VEC_OP_INT8_LT,
	VEC_OP_INT8_LE,
	VEC_OP_INT8_GT,
	VEC_OP_INT8_GE,

	VEC_OP_FLOAT4_EQ,
	VEC_OP_FLOAT4_NE,
	VEC_OP_FLOAT4_LT,
	VEC_OP_FLOAT4_LE,
	VEC_OP_FLOAT4_GT,
	VEC_OP_FLOAT4_GE,

	VEC_OP_FLOAT8_EQ,
	VEC_OP_FLOAT8_NE,
	VEC_OP_FLOAT8_LT,
	VEC_OP_FLOAT8_LE,
	VEC_OP_FLOAT8_GT,
	VEC_OP_FLOAT8_GE,

	/* Boolean logic -------------------------------------------------------*/
	VEC_OP_BOOL_AND,
	VEC_OP_BOOL_OR,
	VEC_OP_BOOL_NOT,

	/* Apply selection vector from a bool column ---------------------------*/
	VEC_OP_FILTER					/* compact batch by bool column */
} VecExprOpcode;

/*
 * VecExprStep
 *
 * One instruction in the vectorized expression program.  The program is a
 * flat array terminated by VEC_OP_DONE.
 */
typedef struct VecExprStep
{
	VecExprOpcode opcode;

	/*
	 * result_col: column index in the working batch where the result of this
	 * step is stored.
	 */
	int			result_col;

	union
	{
		/* VEC_OP_SCAN_VAR */
		struct
		{
			int		attnum;		/* 0-based attribute number in scan tuple */
		}			scan_var;

		/* VEC_OP_CONST */
		struct
		{
			Datum	constval;
			bool	constisnull;
		}			constval;

		/* Binary operators: two input column indices */
		struct
		{
			int		left_col;
			int		right_col;
		}			binop;

		/* Unary operators (e.g. BOOL_NOT): one input column index */
		struct
		{
			int		arg_col;
		}			unop;

		/* VEC_OP_FILTER: which bool column to use as predicate */
		struct
		{
			int		pred_col;
		}			filter;
	}			d;
} VecExprStep;

/*
 * VecExprState
 *
 * Compiled vectorized expression program and associated working storage.
 */
typedef struct VecExprState
{
	int			nsteps;
	VecExprStep *steps;			/* palloc'd array */
	int			nworkcols;		/* total working columns needed */
} VecExprState;

/* -----------------------------------------------------------------------
 * Public API
 * -----------------------------------------------------------------------
 */

/* Lifecycle */
extern VectorBatch *CreateVectorBatch(int ncols, TupleDesc tupdesc);
extern void ResetVectorBatch(VectorBatch *batch);
extern void FreeVectorBatch(VectorBatch *batch);

/* Expression compilation */
extern VecExprState *VecExprCompile(List *quals, List *targetlist,
									TupleDesc scandesc, int *out_ncols);
extern bool VecExprIsVectorizable(List *quals, List *targetlist);

/* Expression evaluation -- operates on an entire VectorBatch in place */
extern void VecExprEval(VecExprState *vstate, VectorBatch *batch);

/* Hook registration (called from _PG_init or backend startup) */
extern void VectorScan_RegisterHook(void);

/* GUC variable */
extern bool enable_vectorized_scan;

#endif							/* EXEC_VECTOR_EXPR_H */
