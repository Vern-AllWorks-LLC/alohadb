/*-------------------------------------------------------------------------
 *
 * jit.c
 *	  Provider independent JIT infrastructure.
 *
 * Code related to loading JIT providers, redirecting calls into JIT providers
 * and error handling.  No code specific to a specific JIT implementation
 * should end up here.
 *
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/jit/jit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "fmgr.h"
#include "jit/jit.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "portability/instr_time.h"
#include "utils/fmgrprotos.h"

/* GUCs */
bool		jit_enabled = true;
char	   *jit_provider = NULL;
bool		jit_debugging_support = false;
bool		jit_dump_bitcode = false;
bool		jit_expressions = true;
bool		jit_profiling_support = false;
bool		jit_tuple_deforming = true;
double		jit_above_cost = 100000;
double		jit_inline_above_cost = 500000;
double		jit_optimize_above_cost = 500000;
int			jit_tier1_threshold = 5;
int			jit_tier2_threshold = 50;

static JitProviderCallbacks provider;
static bool provider_successfully_loaded = false;
static bool provider_failed_loading = false;


static bool provider_init(void);


/*
 * SQL level function returning whether JIT is available in the current
 * backend. Will attempt to load JIT provider if necessary.
 */
Datum
pg_jit_available(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(provider_init());
}


/*
 * Return whether a JIT provider has successfully been loaded, caching the
 * result.
 */
static bool
provider_init(void)
{
	char		path[MAXPGPATH];
	JitProviderInit init;

	/* don't even try to load if not enabled */
	if (!jit_enabled)
		return false;

	/*
	 * Don't retry loading after failing - attempting to load JIT provider
	 * isn't cheap.
	 */
	if (provider_failed_loading)
		return false;
	if (provider_successfully_loaded)
		return true;

	/*
	 * Check whether shared library exists. We do that check before actually
	 * attempting to load the shared library (via load_external_function()),
	 * because that'd error out in case the shlib isn't available.
	 */
	snprintf(path, MAXPGPATH, "%s/%s%s", pkglib_path, jit_provider, DLSUFFIX);
	elog(DEBUG1, "probing availability of JIT provider at %s", path);
	if (!pg_file_exists(path))
	{
		elog(DEBUG1,
			 "provider not available, disabling JIT for current session");
		provider_failed_loading = true;
		return false;
	}

	/*
	 * If loading functions fails, signal failure. We do so because
	 * load_external_function() might error out despite the above check if
	 * e.g. the library's dependencies aren't installed. We want to signal
	 * ERROR in that case, so the user is notified, but we don't want to
	 * continually retry.
	 */
	provider_failed_loading = true;

	/* and initialize */
	init = (JitProviderInit)
		load_external_function(path, "_PG_jit_provider_init", true, NULL);
	init(&provider);

	provider_successfully_loaded = true;
	provider_failed_loading = false;

	elog(DEBUG1, "successfully loaded JIT provider in current session");

	return true;
}

/*
 * Reset JIT provider's error handling. This'll be called after an error has
 * been thrown and the main-loop has re-established control.
 */
void
jit_reset_after_error(void)
{
	if (provider_successfully_loaded)
		provider.reset_after_error();
}

/*
 * Release resources required by one JIT context.
 */
void
jit_release_context(JitContext *context)
{
	if (provider_successfully_loaded)
		provider.release_context(context);

	pfree(context);
}

/*
 * Ask provider to JIT compile an expression.
 *
 * Returns true if successful, false if not.
 *
 * With tiered compilation enabled, this is called from the initial
 * ExecReadyExpr path (the traditional immediate-compile path).
 * When jit_tier1_threshold > 0, the expression starts interpreted
 * and is compiled later by jit_compile_expr_tiered().
 */
bool
jit_compile_expr(struct ExprState *state)
{
	/*
	 * We can easily create a one-off context for functions without an
	 * associated PlanState (and thus EState). But because there's no executor
	 * shutdown callback that could deallocate the created function, they'd
	 * live to the end of the transactions, where they'd be cleaned up by the
	 * resowner machinery. That can lead to a noticeable amount of memory
	 * usage, and worse, trigger some quadratic behaviour in gdb. Therefore,
	 * at least for now, don't create a JITed function in those circumstances.
	 */
	if (!state->parent)
		return false;

	/* if no jitting should be performed at all */
	if (!(state->parent->state->es_jit_flags & PGJIT_PERFORM))
		return false;

	/* or if expressions aren't JITed */
	if (!(state->parent->state->es_jit_flags & PGJIT_EXPR))
		return false;

	/*
	 * If tiered JIT is enabled (tier1 threshold > 0), defer compilation.
	 * The expression will start in interpreted mode and be promoted to
	 * JIT tiers based on execution count.  Return false so that the
	 * interpreter is set up instead.
	 */
	if (jit_tier1_threshold > 0)
		return false;

	/* this also takes !jit_enabled into account */
	if (provider_init())
		return provider.compile_expr(state);

	return false;
}

/*
 * Ask provider to JIT compile an expression at a specific tier.
 *
 * This is called from the tiered JIT dispatch wrapper when an expression
 * has reached enough executions to warrant compilation or recompilation.
 *
 * For Tier 1 (PGJIT_TIER_FAST_COMPILE): compile with -O0, no inlining.
 * For Tier 2 (PGJIT_TIER_OPT_COMPILE): recompile with -O3 + inlining.
 *
 * The caller adjusts es_jit_flags temporarily to control optimization level
 * before calling this function.
 *
 * Returns true if successful, false if not.
 */
bool
jit_compile_expr_tiered(struct ExprState *state, int tier)
{
	int			saved_flags;

	if (!state->parent)
		return false;

	if (!(state->parent->state->es_jit_flags & PGJIT_PERFORM))
		return false;

	if (!(state->parent->state->es_jit_flags & PGJIT_EXPR))
		return false;

	if (!provider_init())
		return false;

	/*
	 * Temporarily adjust JIT flags based on the requested tier.
	 *
	 * Tier 1: fast compile -- strip OPT3 and INLINE flags for -O0.
	 * Tier 2: optimized compile -- ensure OPT3 and INLINE flags are set.
	 */
	saved_flags = state->parent->state->es_jit_flags;

	if (tier == PGJIT_TIER_FAST_COMPILE)
	{
		state->parent->state->es_jit_flags &= ~(PGJIT_OPT3 | PGJIT_INLINE);

		elog(DEBUG2, "tiered JIT: promoting expression to tier 1 (fast compile, -O0)");
	}
	else if (tier == PGJIT_TIER_OPT_COMPILE)
	{
		state->parent->state->es_jit_flags |= (PGJIT_OPT3 | PGJIT_INLINE);

		elog(DEBUG2, "tiered JIT: promoting expression to tier 2 (optimized compile, -O3 + inline)");
	}

	/*
	 * For tier 2 recompilation, update the existing JIT context's flags
	 * so the LLVM compilation layer sees the right optimization level.
	 * The old tier-1 compiled code remains in the context's handles list
	 * and will be cleaned up when the context is released.
	 */
	if (state->parent->state->es_jit)
	{
		JitContext *jit_ctx = state->parent->state->es_jit;

		if (tier == PGJIT_TIER_FAST_COMPILE)
			jit_ctx->flags &= ~(PGJIT_OPT3 | PGJIT_INLINE);
		else if (tier == PGJIT_TIER_OPT_COMPILE)
			jit_ctx->flags |= (PGJIT_OPT3 | PGJIT_INLINE);
	}

	if (!provider.compile_expr(state))
	{
		state->parent->state->es_jit_flags = saved_flags;
		return false;
	}

	/* Update tier tracking in the JIT context */
	if (state->parent->state->es_jit)
	{
		state->parent->state->es_jit->jit_current_tier = tier;
	}

	state->parent->state->es_jit_flags = saved_flags;

	return true;
}

/* Aggregate JIT instrumentation information */
void
InstrJitAgg(JitInstrumentation *dst, JitInstrumentation *add)
{
	dst->created_functions += add->created_functions;
	INSTR_TIME_ADD(dst->generation_counter, add->generation_counter);
	INSTR_TIME_ADD(dst->deform_counter, add->deform_counter);
	INSTR_TIME_ADD(dst->inlining_counter, add->inlining_counter);
	INSTR_TIME_ADD(dst->optimization_counter, add->optimization_counter);
	INSTR_TIME_ADD(dst->emission_counter, add->emission_counter);
}
