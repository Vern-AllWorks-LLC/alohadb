/*-------------------------------------------------------------------------
 *
 * objectstore_s3.c
 *	  AWS S3 operations using SigV4 request signing.
 *
 *	  SigV4 algorithm:
 *	    HMAC-SHA256 (RFC 2104 / FIPS 198) — publicly documented
 *	    AWS signing method; no patent claims apply.
 *
 *	  HTTP transport: libcurl (MIT license).
 *	  SHA-256/HMAC: OpenSSL EVP_MAC (Apache 2.0 / OpenSSL license).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_objectstore/objectstore_s3.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "varatt.h"

#include "pg_objectstore.h"

#include <curl/curl.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <time.h>
#include <string.h>
#include <stdio.h>

/* ----------------------------------------------------------------
 * JSON field extractor
 *
 * Extremely lightweight: find "key":"value" in a JSON object.
 * Only handles simple string values (no nesting needed here).
 * ---------------------------------------------------------------- */
static bool
json_extract_str(const char *json, const char *key,
				 char *out, size_t out_len)
{
	const char *p;
	char		search[256];
	size_t		key_len;

	snprintf(search, sizeof(search), "\"%s\"", key);
	p = strstr(json, search);
	if (!p)
		return false;

	p += strlen(search);
	while (*p == ' ' || *p == ':' || *p == ' ') p++;
	if (*p != '"')
		return false;
	p++;

	key_len = 0;
	while (*p && *p != '"' && key_len < out_len - 1)
		out[key_len++] = *p++;
	out[key_len] = '\0';
	return key_len > 0;
}

/* ----------------------------------------------------------------
 * s3_hex_encode: encode raw bytes as lowercase hex string.
 * out must have at least 2*len+1 bytes.
 * ---------------------------------------------------------------- */
static void
s3_hex_encode(const unsigned char *in, size_t len, char *out)
{
	static const char hex[] = "0123456789abcdef";
	size_t i;

	for (i = 0; i < len; i++)
	{
		out[2*i]   = hex[(in[i] >> 4) & 0xf];
		out[2*i+1] = hex[in[i] & 0xf];
	}
	out[2*len] = '\0';
}

/* ----------------------------------------------------------------
 * hmac_sha256: compute HMAC-SHA256 of data with key.
 * out must be at least SHA256_DIGEST_LENGTH (32) bytes.
 * ---------------------------------------------------------------- */
static void
hmac_sha256(const unsigned char *key, size_t key_len,
			const char *data, size_t data_len,
			unsigned char *out)
{
	unsigned int out_len = SHA256_DIGEST_LENGTH;

	HMAC(EVP_sha256(), key, (int) key_len,
		 (const unsigned char *) data, data_len,
		 out, &out_len);
}

/* ----------------------------------------------------------------
 * sha256_hex: SHA-256 hash of data, returned as hex string.
 * out_hex must be at least 65 bytes.
 * ---------------------------------------------------------------- */
static void
sha256_hex(const char *data, size_t data_len, char *out_hex)
{
	unsigned char hash[SHA256_DIGEST_LENGTH];

	SHA256((const unsigned char *) data, data_len, hash);
	s3_hex_encode(hash, SHA256_DIGEST_LENGTH, out_hex);
}

/* ----------------------------------------------------------------
 * s3_sign_request
 *
 * Build SigV4 Authorization header for an S3 request.
 * All output buffers must be caller-supplied and large enough.
 *
 * out_auth_header:   "Authorization: AWS4-HMAC-SHA256 ..."
 * out_date_header:   "Date: YYYYMMDD"  (for Host: verification)
 * out_xamzdate_header: "x-amz-date: YYYYMMDDTHHmmssZ"
 * ---------------------------------------------------------------- */
void
s3_sign_request(const char *method,
				const char *host,
				const char *uri_path,
				const char *query_string,
				const char *payload_hash,
				const char *region,
				const char *service,
				const char *access_key,
				const char *secret_key,
				char *out_auth_header,
				char *out_date_header,
				char *out_xamzdate_header)
{
	time_t		now;
	struct tm	gmt;
	char		date_str[16];       /* YYYYMMDD */
	char		datetime_str[20];   /* YYYYMMDDTHHmmssZ */

	char		canonical_request[4096];
	char		string_to_sign[1024];
	char		credential_scope[128];

	unsigned char signing_key[SHA256_DIGEST_LENGTH];
	unsigned char tmp[SHA256_DIGEST_LENGTH];
	char		signature_hex[65];

	char		key_secret[256];

	/* ---- Timestamp ---- */
	time(&now);
	gmtime_r(&now, &gmt);
	strftime(date_str, sizeof(date_str), "%Y%m%d", &gmt);
	strftime(datetime_str, sizeof(datetime_str), "%Y%m%dT%H%M%SZ", &gmt);

	snprintf(out_date_header, 32, "%s", date_str);
	snprintf(out_xamzdate_header, 32, "%s", datetime_str);

	/* ---- Canonical request ---- */
	snprintf(canonical_request, sizeof(canonical_request),
		"%s\n"                       /* Method */
		"%s\n"                       /* URI */
		"%s\n"                       /* Query string */
		"host:%s\n"                  /* Canonical headers */
		"x-amz-date:%s\n"
		"\n"
		"host;x-amz-date\n"          /* Signed headers */
		"%s",                        /* Payload hash */
		method,
		uri_path,
		query_string ? query_string : "",
		host,
		datetime_str,
		payload_hash);

	/* ---- Credential scope ---- */
	snprintf(credential_scope, sizeof(credential_scope),
			 "%s/%s/%s/aws4_request",
			 date_str, region, service);

	/* ---- String to sign ---- */
	{
		char cr_hash[65];
		sha256_hex(canonical_request, strlen(canonical_request), cr_hash);

		snprintf(string_to_sign, sizeof(string_to_sign),
			"AWS4-HMAC-SHA256\n"
			"%s\n"
			"%s\n"
			"%s",
			datetime_str,
			credential_scope,
			cr_hash);
	}

	/* ---- Derive signing key ---- */
	/* kSecret = "AWS4" + secret_key */
	snprintf(key_secret, sizeof(key_secret), "AWS4%s", secret_key);
	hmac_sha256((const unsigned char *) key_secret, strlen(key_secret),
				date_str, strlen(date_str), tmp);
	hmac_sha256(tmp, SHA256_DIGEST_LENGTH,
				region, strlen(region), signing_key);
	hmac_sha256(signing_key, SHA256_DIGEST_LENGTH,
				service, strlen(service), tmp);
	hmac_sha256(tmp, SHA256_DIGEST_LENGTH,
				"aws4_request", strlen("aws4_request"), signing_key);

	/* ---- Signature ---- */
	hmac_sha256(signing_key, SHA256_DIGEST_LENGTH,
				string_to_sign, strlen(string_to_sign), tmp);
	s3_hex_encode(tmp, SHA256_DIGEST_LENGTH, signature_hex);

	/* ---- Authorization header ---- */
	snprintf(out_auth_header, 1024,
		"Authorization: AWS4-HMAC-SHA256 "
		"Credential=%s/%s, "
		"SignedHeaders=host;x-amz-date, "
		"Signature=%s",
		access_key, credential_scope, signature_hex);
}

/* ----------------------------------------------------------------
 * libcurl response buffer
 * ---------------------------------------------------------------- */
typedef struct CurlBuffer
{
	char   *data;
	size_t	len;
	size_t	alloc;
} CurlBuffer;

static size_t
curl_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata)
{
	CurlBuffer *buf = (CurlBuffer *) userdata;
	size_t		total = size * nmemb;

	if (buf->len + total + 1 > buf->alloc)
	{
		size_t	new_alloc = buf->alloc * 2 + total + 1024;
		buf->data = repalloc(buf->data, new_alloc);
		buf->alloc = new_alloc;
	}
	memcpy(buf->data + buf->len, ptr, total);
	buf->len += total;
	buf->data[buf->len] = '\0';
	return total;
}

/* ----------------------------------------------------------------
 * s3_do_request
 *
 * Execute an HTTP request against S3 with SigV4 signing.
 * Returns the response body as a palloc'd bytea, or NULL on error.
 * ---------------------------------------------------------------- */
static bytea *
s3_do_request(const char *method,
			  const char *url,
			  const char *host,
			  const char *uri_path,
			  const char *region,
			  const char *access_key,
			  const char *secret_key,
			  const char *body,
			  size_t		body_len)
{
	CURL		   *curl;
	CURLcode		res;
	CurlBuffer		buf;
	struct curl_slist *headers = NULL;
	char			auth_header[1024];
	char			date_str[32];
	char			xamzdate_str[32];
	char			date_header[64];
	char			xamzdate_header[64];
	char			payload_hash[65];
	bytea		   *result = NULL;

	sha256_hex(body ? body : "", body_len, payload_hash);

	s3_sign_request(method, host, uri_path, NULL,
					payload_hash, region, "s3",
					access_key, secret_key,
					auth_header, date_str, xamzdate_str);

	snprintf(date_header, sizeof(date_header), "Date: %s", date_str);
	snprintf(xamzdate_header, sizeof(xamzdate_header),
			 "x-amz-date: %s", xamzdate_str);

	buf.alloc = 4096;
	buf.len   = 0;
	buf.data  = palloc(buf.alloc);

	curl = curl_easy_init();
	if (!curl)
		return NULL;

	headers = curl_slist_append(headers, auth_header);
	headers = curl_slist_append(headers, date_header);
	headers = curl_slist_append(headers, xamzdate_header);
	headers = curl_slist_append(headers,
								"x-amz-content-sha256: " /* + hash appended below */);

	/* Append the payload hash to the x-amz-content-sha256 header */
	{
		char hash_header[128];
		snprintf(hash_header, sizeof(hash_header),
				 "x-amz-content-sha256: %s", payload_hash);
		/* Replace last appended (empty) header */
		headers = curl_slist_append(headers, hash_header);
	}

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

	if (strcmp(method, "GET") == 0)
		curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	else if (strcmp(method, "DELETE") == 0)
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
	else if (strcmp(method, "PUT") == 0)
	{
		curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
		curl_easy_setopt(curl, CURLOPT_READDATA, body);
		curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
						 (curl_off_t) body_len);
	}

	res = curl_easy_perform(curl);

	if (res == CURLE_OK)
	{
		long	http_code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

		if (http_code >= 200 && http_code < 300)
		{
			result = (bytea *) palloc(VARHDRSZ + buf.len);
			SET_VARSIZE(result, VARHDRSZ + buf.len);
			memcpy(VARDATA(result), buf.data, buf.len);
		}
		else
			elog(WARNING, "objectstore_s3: HTTP %ld for %s", http_code, url);
	}
	else
		elog(WARNING, "objectstore_s3: curl error: %s", curl_easy_strerror(res));

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	pfree(buf.data);

	return result;
}

/* ----------------------------------------------------------------
 * URI -> bucket + key + host + region helpers
 *
 * Parses  s3://bucket/key  or  s3://bucket.s3.region.amazonaws.com/key
 * ---------------------------------------------------------------- */
static void
parse_s3_uri(const char *uri, const char *config_json,
			 char *out_bucket, size_t bucket_len,
			 char *out_key,    size_t key_len,
			 char *out_host,   size_t host_len,
			 char *out_region, size_t region_len,
			 char *out_access_key, size_t ak_len,
			 char *out_secret_key, size_t sk_len)
{
	const char *p = uri;

	/* Strip scheme */
	if (strncmp(p, "s3://", 5) == 0)
		p += 5;

	/* bucket */
	{
		const char *slash = strchr(p, '/');
		size_t		blen;

		if (slash)
			blen = (size_t)(slash - p);
		else
			blen = strlen(p);
		if (blen >= bucket_len) blen = bucket_len - 1;
		memcpy(out_bucket, p, blen);
		out_bucket[blen] = '\0';
		p = slash ? slash + 1 : p + blen;
	}

	/* key */
	strlcpy(out_key, p, key_len);

	/* region from config, fallback to us-east-1 */
	if (!json_extract_str(config_json, "region", out_region, region_len))
		strlcpy(out_region, "us-east-1", region_len);

	/* host */
	{
		char endpoint[256];
		if (json_extract_str(config_json, "endpoint", endpoint, sizeof(endpoint)))
			strlcpy(out_host, endpoint, host_len);
		else
			snprintf(out_host, host_len, "%s.s3.%s.amazonaws.com",
					 out_bucket, out_region);
	}

	/*
	 * Credentials are required for SigV4 signing.  Never sign with
	 * uninitialized buffers: a credential registered without keys used to
	 * crash the backend (and with it, every other session on the server).
	 */
	out_access_key[0] = '\0';
	out_secret_key[0] = '\0';
	if (!json_extract_str(config_json, "access_key_id", out_access_key, ak_len) ||
		!json_extract_str(config_json, "secret_access_key", out_secret_key, sk_len))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_objectstore: S3 credential is missing \"access_key_id\" or \"secret_access_key\"")));
}

/* ----------------------------------------------------------------
 * Public S3 operations
 * ---------------------------------------------------------------- */

bytea *
s3_object_get(const char *uri, const char *config_json)
{
	char	bucket[256], key[1024], host[512];
	char	region[64], access_key[128], secret_key[256];
	char	url[2048];

	parse_s3_uri(uri, config_json,
				 bucket, sizeof(bucket), key, sizeof(key),
				 host, sizeof(host), region, sizeof(region),
				 access_key, sizeof(access_key),
				 secret_key, sizeof(secret_key));

	snprintf(url, sizeof(url), "https://%s/%s", host, key);

	return s3_do_request("GET", url, host, key[0] ? key : "/",
						 region, access_key, secret_key, NULL, 0);
}

bool
s3_object_put(const char *uri, bytea *data, const char *config_json)
{
	char	bucket[256], key[1024], host[512];
	char	region[64], access_key[128], secret_key[256];
	char	url[2048];
	bytea  *result;

	parse_s3_uri(uri, config_json,
				 bucket, sizeof(bucket), key, sizeof(key),
				 host, sizeof(host), region, sizeof(region),
				 access_key, sizeof(access_key),
				 secret_key, sizeof(secret_key));

	snprintf(url, sizeof(url), "https://%s/%s", host, key);

	result = s3_do_request("PUT", url, host, key[0] ? key : "/",
						   region, access_key, secret_key,
						   VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data));
	return result != NULL;
}

bool
s3_object_delete(const char *uri, const char *config_json)
{
	char	bucket[256], key[1024], host[512];
	char	region[64], access_key[128], secret_key[256];
	char	url[2048];
	bytea  *result;

	parse_s3_uri(uri, config_json,
				 bucket, sizeof(bucket), key, sizeof(key),
				 host, sizeof(host), region, sizeof(region),
				 access_key, sizeof(access_key),
				 secret_key, sizeof(secret_key));

	snprintf(url, sizeof(url), "https://%s/%s", host, key);

	result = s3_do_request("DELETE", url, host, key[0] ? key : "/",
						   region, access_key, secret_key, NULL, 0);
	return result != NULL;
}

/* ----------------------------------------------------------------
 * s3_object_list: parse S3 ListObjectsV2 XML response.
 *
 * S3 returns XML like:
 *   <Contents><Key>...</Key><Size>N</Size>
 *             <LastModified>...</LastModified><ETag>...</ETag></Contents>
 *
 * We use a minimal hand-written XML scanner (no libxml2 needed).
 * ---------------------------------------------------------------- */
ObjstoreListResult *
s3_object_list(const char *prefix, const char *config_json,
			   int max_keys, int *out_nresults)
{
	char	bucket[256], dummy_key[8] = "", host[512];
	char	region[64], access_key[128], secret_key[256];
	char	url[2048];
	char	query[512];
	bytea  *response;
	ObjstoreListResult *results;
	int		cap = 64, n = 0;
	char   *body;
	char   *p;

	parse_s3_uri(prefix, config_json,
				 bucket, sizeof(bucket), dummy_key, sizeof(dummy_key),
				 host, sizeof(host), region, sizeof(region),
				 access_key, sizeof(access_key),
				 secret_key, sizeof(secret_key));

	/* ListObjectsV2 query */
	snprintf(query, sizeof(query),
			 "list-type=2&max-keys=%d&prefix=%s",
			 max_keys, dummy_key);

	snprintf(url, sizeof(url), "https://%s/?%s", host, query);

	response = s3_do_request("GET", url, host, "/",
							 region, access_key, secret_key, NULL, 0);
	if (!response)
	{
		*out_nresults = 0;
		return NULL;
	}

	body = VARDATA_ANY(response);

	results = palloc(sizeof(ObjstoreListResult) * cap);

	p = body;
	while ((p = strstr(p, "<Contents>")) != NULL)
	{
		char	key[1024] = "";
		char	size_str[32] = "0";
		char	lmod[64] = "";
		char	etag[128] = "";
		char   *end;

		end = strstr(p, "</Contents>");
		if (!end) break;

		/* Extract fields from within this <Contents> block */
#define EXTRACT_XML(tag, buf, bufsz) do { \
	char *_s = strstr(p, "<" tag ">"); \
	char *_e = _s ? strstr(_s, "</" tag ">") : NULL; \
	if (_s && _e) { \
		_s += strlen("<" tag ">"); \
		size_t _l = (size_t)(_e - _s); \
		if (_l >= (bufsz)) _l = (bufsz) - 1; \
		memcpy(buf, _s, _l); \
		buf[_l] = '\0'; \
	} \
} while(0)

		EXTRACT_XML("Key",          key,      sizeof(key));
		EXTRACT_XML("Size",         size_str, sizeof(size_str));
		EXTRACT_XML("LastModified", lmod,     sizeof(lmod));
		EXTRACT_XML("ETag",         etag,     sizeof(etag));
#undef EXTRACT_XML

		if (n >= cap)
		{
			cap *= 2;
			results = repalloc(results, sizeof(ObjstoreListResult) * cap);
		}

		strlcpy(results[n].key, key, sizeof(results[n].key));
		results[n].size_bytes   = (int64) strtoll(size_str, NULL, 10);
		results[n].last_modified = 0; /* simplified: skip ISO 8601 parse */
		strlcpy(results[n].etag, etag, sizeof(results[n].etag));
		n++;

		p = end + strlen("</Contents>");
	}

	*out_nresults = n;
	return results;
}
