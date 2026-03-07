#ifndef ALOHADB_ANALYTICS_H
#define ALOHADB_ANALYTICS_H

#include "postgres.h"
#include "fmgr.h"

/* Metadata table */
#define ANALYTICS_CONT_AGGS_TABLE "alohadb_analytics_cont_aggs"

/* Continuous aggregate management */
extern Datum analytics_create_cont_agg(PG_FUNCTION_ARGS);
extern Datum analytics_drop_cont_agg(PG_FUNCTION_ARGS);
extern Datum analytics_refresh_cont_agg(PG_FUNCTION_ARGS);
extern Datum analytics_cont_agg_status(PG_FUNCTION_ARGS);

/* Hyperfunctions */
extern Datum analytics_interpolate(PG_FUNCTION_ARGS);
extern Datum analytics_moving_avg(PG_FUNCTION_ARGS);
extern Datum analytics_moving_sum(PG_FUNCTION_ARGS);
extern Datum analytics_gap_fill(PG_FUNCTION_ARGS);

/* Aggregates */
extern Datum analytics_first_transfn(PG_FUNCTION_ARGS);
extern Datum analytics_first_finalfn(PG_FUNCTION_ARGS);
extern Datum analytics_last_transfn(PG_FUNCTION_ARGS);
extern Datum analytics_last_finalfn(PG_FUNCTION_ARGS);
extern Datum analytics_delta(PG_FUNCTION_ARGS);
extern Datum analytics_rate(PG_FUNCTION_ARGS);

/* Projections */
extern Datum analytics_create_projection(PG_FUNCTION_ARGS);
extern Datum analytics_drop_projection(PG_FUNCTION_ARGS);

#endif /* ALOHADB_ANALYTICS_H */
