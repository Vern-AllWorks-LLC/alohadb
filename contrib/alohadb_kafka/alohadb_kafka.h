#ifndef ALOHADB_KAFKA_H
#define ALOHADB_KAFKA_H

#include "postgres.h"
#include "fmgr.h"

extern int	kafka_poll_interval_ms;
extern char *kafka_database;

pg_noreturn void		alohadb_kafka_worker_main(Datum main_arg);

/* kafka_consumer.c */
extern int64 kafka_poll_once(int64 consumer_id,
							 const char *consumer_name,
							 const char *consumer_type,
							 const char *brokers,
							 const char *topic,
							 const char *group_id);

/* SQL-callable consumer management */
extern Datum kafka_consumer_start(PG_FUNCTION_ARGS);
extern Datum kafka_consumer_stop(PG_FUNCTION_ARGS);
extern Datum kafka_consumer_status(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_KAFKA_H */
