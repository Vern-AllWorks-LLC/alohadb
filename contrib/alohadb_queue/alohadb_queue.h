/*-------------------------------------------------------------------------
 *
 * alohadb_queue.h
 *	  Shared declarations for the alohadb_queue extension.
 *
 *	  Provides a lightweight, transactional message queue built on top
 *	  of PostgreSQL tables.  Supports visibility timeouts, batch
 *	  send/receive, consumer groups with offset tracking, and standard
 *	  ack/nack message lifecycle management.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_queue/alohadb_queue.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_QUEUE_H
#define ALOHADB_QUEUE_H

#include "postgres.h"
#include "fmgr.h"

/* ----------------------------------------------------------------
 * Table and index names used by the extension
 * ---------------------------------------------------------------- */
#define QUEUE_TABLE_QUEUES		"alohadb_queue_queues"
#define QUEUE_TABLE_MESSAGES	"alohadb_queue_messages"
#define QUEUE_TABLE_CONSUMERS	"alohadb_queue_consumers"

/* ----------------------------------------------------------------
 * Message status constants
 * ---------------------------------------------------------------- */
#define QUEUE_STATUS_READY			"ready"
#define QUEUE_STATUS_DELIVERED		"delivered"
#define QUEUE_STATUS_ACKNOWLEDGED	"acknowledged"

/* ----------------------------------------------------------------
 * SQL-callable functions (implemented in queue_ops.c)
 * ---------------------------------------------------------------- */
extern Datum queue_create(PG_FUNCTION_ARGS);
extern Datum queue_drop(PG_FUNCTION_ARGS);
extern Datum queue_send(PG_FUNCTION_ARGS);
extern Datum queue_send_batch(PG_FUNCTION_ARGS);
extern Datum queue_receive(PG_FUNCTION_ARGS);
extern Datum queue_ack(PG_FUNCTION_ARGS);
extern Datum queue_nack(PG_FUNCTION_ARGS);
extern Datum queue_purge(PG_FUNCTION_ARGS);
extern Datum queue_stats(PG_FUNCTION_ARGS);
extern Datum queue_subscribe(PG_FUNCTION_ARGS);
extern Datum queue_poll(PG_FUNCTION_ARGS);
extern Datum queue_commit_offset(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_QUEUE_H */
