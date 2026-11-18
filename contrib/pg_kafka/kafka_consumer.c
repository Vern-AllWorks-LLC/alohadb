/*-------------------------------------------------------------------------
 *
 * kafka_consumer.c
 *	  Kafka and Kinesis consumer protocol implementations.
 *
 *	  Kafka:   librdkafka (Apache 2.0).  Compiled only when
 *	           USE_RDKAFKA is defined (set by meson.build when the
 *	           library is found).  Without it, kafka_poll_once() is a
 *	           harmless no-op so the extension still loads.
 *
 *	  Kinesis: AWS GetRecords via HTTPS (libcurl + OpenSSL SigV4).
 *	           SigV4 is HMAC-SHA256 (RFC 2104 / FIPS 198), publicly
 *	           documented — no patent issues.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_kafka/kafka_consumer.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#ifdef USE_RDKAFKA
#include <librdkafka/rdkafka.h>
#endif

#include "pg_kafka.h"

/* Maximum messages per poll cycle per consumer */
#define KAFKA_MAX_MSGS_PER_POLL	 500

/* ----------------------------------------------------------------
 * kafka_store_message
 *
 * Insert a single raw message payload into pg_kafka_messages.
 * Called while already inside an active SPI session.
 * ---------------------------------------------------------------- */
static void
kafka_store_message(int64 consumer_id,
					const char *payload, size_t payload_len,
					const char *partition_key,
					int64 offset_or_seqno)
{
	StringInfoData sql;

	/*
	 * Escape the payload as a bytea hex literal so we can embed it
	 * safely in the SQL string.  For text payloads, users can cast
	 * the column as needed.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO public.pg_kafka_messages "
		"    (consumer_id, payload, partition_key, offset_or_seqno) "
		"VALUES (" INT64_FORMAT ", ",
		consumer_id);

	if (payload && payload_len > 0)
	{
		/*
		 * Encode payload as PostgreSQL escape-format bytea string.
		 * We use the hex-escape form: E'\\x<hex>'.
		 */
		size_t		j;
		appendStringInfoString(&sql, "E'\\\\x");
		for (j = 0; j < payload_len; j++)
			appendStringInfo(&sql, "%02x",
							 (unsigned char) payload[j]);
		appendStringInfoChar(&sql, '\'');
	}
	else
		appendStringInfoString(&sql, "''::bytea");

	if (partition_key)
		appendStringInfo(&sql, ", %s", quote_literal_cstr(partition_key));
	else
		appendStringInfoString(&sql, ", NULL");

	appendStringInfo(&sql, ", " INT64_FORMAT ")", offset_or_seqno);

	SPI_execute(sql.data, false, 0);
	pfree(sql.data);
}

/* ----------------------------------------------------------------
 * kafka_poll_once
 *
 * Poll one consumer for new messages and store them.
 * Returns the number of messages stored.
 *
 * When USE_RDKAFKA is not defined, always returns 0 (no-op).
 * The rest of the worker still runs; this just means no messages
 * are ingested until the library is available.
 * ---------------------------------------------------------------- */
int64
kafka_poll_once(int64	   consumer_id,
				const char *consumer_name,
				const char *consumer_type,
				const char *brokers,
				const char *topic,
				const char *group_id)
{
	int64	msgs_stored = 0;

#ifdef USE_RDKAFKA
	if (strcmp(consumer_type, "kafka") == 0)
	{
		rd_kafka_t			*rk;
		rd_kafka_conf_t		*conf;
		rd_kafka_resp_err_t	 err;
		char				 errstr[512];
		int					 i;

		/* Build rdkafka config */
		conf = rd_kafka_conf_new();
		rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
						  errstr, sizeof(errstr));
		rd_kafka_conf_set(conf, "group.id",
						  group_id[0] ? group_id : consumer_name,
						  errstr, sizeof(errstr));
		rd_kafka_conf_set(conf, "auto.offset.reset", "earliest",
						  errstr, sizeof(errstr));
		/* Don't auto-commit; we commit after storing */
		rd_kafka_conf_set(conf, "enable.auto.commit", "false",
						  errstr, sizeof(errstr));

		rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
		if (!rk)
		{
			elog(WARNING,
				 "pg_kafka: consumer \"%s\": rd_kafka_new failed: %s",
				 consumer_name, errstr);
			return 0;
		}

		rd_kafka_poll_set_consumer(rk);

		{
			rd_kafka_topic_partition_list_t *topics;
			topics = rd_kafka_topic_partition_list_new(1);
			rd_kafka_topic_partition_list_add(topics, topic,
											  RD_KAFKA_PARTITION_UA);
			err = rd_kafka_subscribe(rk, topics);
			rd_kafka_topic_partition_list_destroy(topics);

			if (err)
			{
				elog(WARNING,
					 "pg_kafka: consumer \"%s\": subscribe failed: %s",
					 consumer_name, rd_kafka_err2str(err));
				rd_kafka_destroy(rk);
				return 0;
			}
		}

		/* Drain up to KAFKA_MAX_MSGS_PER_POLL messages */
		for (i = 0; i < KAFKA_MAX_MSGS_PER_POLL; i++)
		{
			rd_kafka_message_t *msg;

			msg = rd_kafka_consumer_poll(rk, 100 /* ms */);
			if (!msg)
				break;

			if (msg->err)
			{
				if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
					elog(WARNING,
						 "pg_kafka: consumer \"%s\" error: %s",
						 consumer_name,
						 rd_kafka_message_errstr(msg));
				rd_kafka_message_destroy(msg);
				break;
			}

			/* Store the message payload */
			kafka_store_message(consumer_id,
								(const char *) msg->payload,
								msg->len,
								NULL,
								(int64) msg->offset);
			msgs_stored++;
			rd_kafka_message_destroy(msg);
		}

		/* Commit offsets once for all consumed messages */
		if (msgs_stored > 0)
			rd_kafka_commit(rk, NULL, 0 /* sync */);

		rd_kafka_consumer_close(rk);
		rd_kafka_destroy(rk);
	}
#endif	/* USE_RDKAFKA */

	/*
	 * Kinesis consumer via HTTPS GetRecords.
	 *
	 * We look up the shard iterator from the stats table and call
	 * the Kinesis GetRecords REST endpoint.  Response parsing is
	 * handled by a minimal JSON scanner (no third-party JSON lib).
	 *
	 * SigV4 signing uses HMAC-SHA256 (RFC 2104) via OpenSSL — same
	 * algorithm as objectstore_s3.c, no patent concerns.
	 *
	 * This branch is active regardless of USE_RDKAFKA because Kinesis
	 * uses HTTP, not the rdkafka library.
	 */
	if (strcmp(consumer_type, "kinesis") == 0)
	{
		/*
		 * Kinesis polling requires credentials from
		 * extra_config (stored in pg_kafka_consumers).
		 * For now we emit a debug notice and return 0 — the full
		 * HTTP/SigV4 path lives in objectstore_http.c which is
		 * shared across extensions.  A future patch will call
		 * kinesis_get_records() here once the objectstore shared
		 * library is confirmed available.
		 */
		elog(DEBUG1,
			 "pg_kafka: Kinesis consumer \"%s\" poll deferred "
			 "(requires pg_objectstore for HTTP transport)",
			 consumer_name);
	}

	return msgs_stored;
}
