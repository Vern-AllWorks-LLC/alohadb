#ifndef ALOHADB_OBJECTSTORE_H
#define ALOHADB_OBJECTSTORE_H

#include "postgres.h"
#include "fmgr.h"

#define OBJSTORE_PROVIDER_S3    "s3"
#define OBJSTORE_PROVIDER_GCS   "gcs"
#define OBJSTORE_PROVIDER_AZURE "azure"

#define OBJSTORE_MAX_URI_LEN    2048
#define OBJSTORE_MAX_REGION_LEN 64

/*
 * Result record for list operations.
 * Matches the SQL return type of objectstore_list().
 */
typedef struct ObjstoreListResult
{
	char	key[1024];
	int64	size_bytes;
	int64	last_modified;	/* Unix timestamp, 0 if unavailable */
	char	etag[256];
} ObjstoreListResult;

/* SigV4 signing (objectstore_s3.c) */
extern void s3_sign_request(const char *method, const char *host,
							const char *uri_path, const char *query_string,
							const char *payload_hash,
							const char *region, const char *service,
							const char *access_key, const char *secret_key,
							char *out_auth_header, char *out_date_header,
							char *out_xamzdate_header);

/* S3 operations (objectstore_s3.c) */
extern bytea *s3_object_get(const char *uri, const char *config_json);
extern bool   s3_object_put(const char *uri, bytea *data, const char *config_json);
extern bool   s3_object_delete(const char *uri, const char *config_json);
extern ObjstoreListResult *s3_object_list(const char *prefix,
										  const char *config_json,
										  int max_keys, int *out_nresults);

/* GCS / Azure operations (objectstore_http.c) */
extern bytea *http_object_get(const char *uri, const char *config_json,
							  const char *provider);
extern bool   http_object_put(const char *uri, bytea *data,
							  const char *config_json, const char *provider);
extern bool   http_object_delete(const char *uri, const char *config_json,
								 const char *provider);
extern ObjstoreListResult *http_object_list(const char *prefix,
											const char *config_json,
											const char *provider,
											int max_keys, int *out_nresults);

/* SQL-callable functions */
extern Datum objectstore_get(PG_FUNCTION_ARGS);
extern Datum objectstore_get_text(PG_FUNCTION_ARGS);
extern Datum objectstore_put(PG_FUNCTION_ARGS);
extern Datum objectstore_list(PG_FUNCTION_ARGS);
extern Datum objectstore_delete(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_OBJECTSTORE_H */
