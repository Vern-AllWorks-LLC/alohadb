/*-------------------------------------------------------------------------
 *
 * objectstore_http.c
 *	  GCS and Azure Blob cloud storage via standard HTTPS REST APIs.
 *
 *	  GCS:   OAuth2 service-account token exchange (RFC 6749) +
 *	         JSON REST API (no proprietary SDK).
 *	  Azure: Shared Access Signature (HMAC-SHA256, RFC 2104) +
 *	         Azure Blob REST API.
 *
 *	  HTTP transport: libcurl (MIT license).
 *	  HMAC-SHA256: OpenSSL (Apache 2.0 / OpenSSL license).
 *
 *	  All algorithms are open standards with prior art predating any
 *	  applicable patent window.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_objectstore/objectstore_http.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "varatt.h"

#include "pg_objectstore.h"

#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/pem.h>
#include <string.h>
#include <stdio.h>

/* ----------------------------------------------------------------
 * base64_encode: encode len bytes of src into a null-terminated
 * base64 string (standard alphabet).
 * Returns a palloc'd string.
 * ---------------------------------------------------------------- */
static char *
base64_encode(const unsigned char *src, size_t len)
{
	BIO    *b64, *bmem;
	BUF_MEM *bptr;
	char   *result;

	b64  = BIO_new(BIO_f_base64());
	bmem = BIO_new(BIO_s_mem());
	b64  = BIO_push(b64, bmem);
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
	BIO_write(b64, src, (int) len);
	BIO_flush(b64);
	BIO_get_mem_ptr(b64, &bptr);

	result = palloc(bptr->length + 1);
	memcpy(result, bptr->data, bptr->length);
	result[bptr->length] = '\0';

	BIO_free_all(b64);
	return result;
}

/* ----------------------------------------------------------------
 * Minimal JSON field extractor (same as objectstore_s3.c)
 * ---------------------------------------------------------------- */
static bool
json_extract_str(const char *json, const char *key,
				 char *out, size_t out_len)
{
	char		search[256];
	const char *p;
	size_t		key_len;

	snprintf(search, sizeof(search), "\"%s\"", key);
	p = strstr(json, search);
	if (!p)
		return false;

	p += strlen(search);
	while (*p == ' ' || *p == ':') p++;
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
 * libcurl response buffer (same pattern as objectstore_s3.c)
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
		size_t new_alloc = buf->alloc * 2 + total + 1024;
		buf->data = repalloc(buf->data, new_alloc);
		buf->alloc = new_alloc;
	}
	memcpy(buf->data + buf->len, ptr, total);
	buf->len += total;
	buf->data[buf->len] = '\0';
	return total;
}

/* ----------------------------------------------------------------
 * http_do_request
 *
 * Generic HTTP request with a pre-built header list.
 * Returns a palloc'd bytea with the response body, or NULL on error.
 * ---------------------------------------------------------------- */
static bytea *
http_do_request(const char *method, const char *url,
				struct curl_slist *headers,
				const char *body, size_t body_len)
{
	CURL		   *curl;
	CURLcode		res;
	CurlBuffer		buf;
	bytea		   *result = NULL;

	buf.alloc = 4096;
	buf.len   = 0;
	buf.data  = palloc(buf.alloc);

	curl = curl_easy_init();
	if (!curl)
	{
		pfree(buf.data);
		return NULL;
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
	else if (strcmp(method, "POST") == 0)
	{
		curl_easy_setopt(curl, CURLOPT_POST, 1L);
		if (body)
		{
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) body_len);
		}
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
			elog(WARNING, "objectstore_http: HTTP %ld for %s", http_code, url);
	}
	else
		elog(WARNING, "objectstore_http: curl error: %s",
			 curl_easy_strerror(res));

	curl_easy_cleanup(curl);
	pfree(buf.data);
	return result;
}

/* ----------------------------------------------------------------
 * GCS: obtain an OAuth2 bearer token via service-account JWT.
 *
 * Uses the service_account_json from the credential config.
 * JWT signing: RS256 (RSA-SHA256), standard JWT structure.
 * Token endpoint: https://oauth2.googleapis.com/token  (RFC 6749).
 *
 * Returns a palloc'd access token string, or NULL on failure.
 * ---------------------------------------------------------------- */
static char *
gcs_get_access_token(const char *config_json)
{
	char	sa_email[256] = "";
	char	private_key[4096] = "";
	char	token_uri[256] = "https://oauth2.googleapis.com/token";
	char   *token = NULL;

	json_extract_str(config_json, "client_email", sa_email, sizeof(sa_email));
	json_extract_str(config_json, "private_key",  private_key, sizeof(private_key));

	if (!sa_email[0] || !private_key[0])
	{
		elog(WARNING, "objectstore_http: GCS config missing client_email or private_key");
		return NULL;
	}

	/*
	 * Build a minimal JWT:
	 *   header: {"alg":"RS256","typ":"JWT"}
	 *   claims: {"iss":"<email>","scope":"https://www.googleapis.com/auth/devstorage.read_write","aud":"<token_uri>","exp":<now+3600>,"iat":<now>}
	 *
	 * Sign with RS256 using the service-account private key.
	 * Then POST to token_uri to exchange for an access token.
	 *
	 * Full RS256 JWT implementation requires PEM key parsing via
	 * OpenSSL EVP_DigestSign.  We include a complete implementation
	 * here using standard OpenSSL APIs only.
	 */
	{
		const char *header_json = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
		char		claims_json[1024];
		char	   *header_b64;
		char	   *claims_b64;
		char		signing_input[2048];
		unsigned char *sig = NULL;
		size_t		sig_len = 0;
		EVP_PKEY   *pkey = NULL;
		EVP_MD_CTX *mdctx = NULL;
		BIO		   *bio;
		char	   *jwt;
		char	   *sig_b64;
		time_t		now_t;
		CurlBuffer	token_buf;
		struct curl_slist *headers = NULL;
		CURL	   *curl;
		CURLcode	res;

		time(&now_t);
		snprintf(claims_json, sizeof(claims_json),
			"{\"iss\":\"%s\","
			"\"scope\":\"https://www.googleapis.com/auth/devstorage.read_write\","
			"\"aud\":\"%s\","
			"\"exp\":%ld,"
			"\"iat\":%ld}",
			sa_email, token_uri,
			(long)(now_t + 3600), (long) now_t);

		header_b64 = base64_encode(
			(const unsigned char *) header_json, strlen(header_json));
		claims_b64 = base64_encode(
			(const unsigned char *) claims_json, strlen(claims_json));

		snprintf(signing_input, sizeof(signing_input),
				 "%s.%s", header_b64, claims_b64);

		/* Load RSA private key from PEM string */
		bio = BIO_new_mem_buf(private_key, (int) strlen(private_key));
		PEM_read_bio_PrivateKey(bio, &pkey, NULL, NULL);
		BIO_free(bio);

		if (!pkey)
		{
			elog(WARNING, "objectstore_http: GCS: failed to load private key");
			goto gcs_cleanup;
		}

		mdctx = EVP_MD_CTX_new();
		EVP_DigestSignInit(mdctx, NULL, EVP_sha256(), NULL, pkey);
		EVP_DigestSignUpdate(mdctx,
							 signing_input, strlen(signing_input));
		EVP_DigestSignFinal(mdctx, NULL, &sig_len);
		sig = palloc(sig_len);
		EVP_DigestSignFinal(mdctx, sig, &sig_len);

		sig_b64 = base64_encode(sig, sig_len);
		pfree(sig);

		/* JWT = header.claims.signature */
		jwt = psprintf("%s.%s.%s", header_b64, claims_b64, sig_b64);

		/* Exchange JWT for access token */
		{
			char post_body[4096];

			snprintf(post_body, sizeof(post_body),
				"grant_type=urn%%3Aietf%%3Aparams%%3Aoauth%%3Agrant-type%%3Ajwt-bearer"
				"&assertion=%s",
				jwt);

			token_buf.alloc = 2048;
			token_buf.len   = 0;
			token_buf.data  = palloc(token_buf.alloc);

			curl = curl_easy_init();
			headers = curl_slist_append(NULL,
				"Content-Type: application/x-www-form-urlencoded");

			curl_easy_setopt(curl, CURLOPT_URL, token_uri);
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
			curl_easy_setopt(curl, CURLOPT_POST, 1L);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body);
			curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
			curl_easy_setopt(curl, CURLOPT_WRITEDATA, &token_buf);
			curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);

			res = curl_easy_perform(curl);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);

			if (res == CURLE_OK)
			{
				char tok[512] = "";
				if (json_extract_str(token_buf.data, "access_token",
									 tok, sizeof(tok)))
					token = pstrdup(tok);
				else
					elog(WARNING, "objectstore_http: GCS token exchange failed: %s",
						 token_buf.data);
			}
			pfree(token_buf.data);
		}

gcs_cleanup:
		if (mdctx) EVP_MD_CTX_free(mdctx);
		if (pkey)  EVP_PKEY_free(pkey);
	}

	return token;
}

/* ----------------------------------------------------------------
 * http_object_get
 * ---------------------------------------------------------------- */
bytea *
http_object_get(const char *uri, const char *config_json,
				const char *provider)
{
	struct curl_slist *headers = NULL;
	bytea		  *result;

	if (strcmp(provider, "gcs") == 0)
	{
		char	bucket[256] = "", obj_key[1024] = "";
		char	url[2048];
		char   *token;
		char	auth_hdr[768];

		/* gcs://bucket/key */
		{
			const char *p = uri;
			if (strncmp(p, "gcs://", 6) == 0) p += 6;
			{
				const char *slash = strchr(p, '/');
				size_t		bl = slash ? (size_t)(slash-p) : strlen(p);
				if (bl >= sizeof(bucket)) bl = sizeof(bucket)-1;
				memcpy(bucket, p, bl); bucket[bl] = '\0';
				if (slash) strlcpy(obj_key, slash+1, sizeof(obj_key));
			}
		}

		token = gcs_get_access_token(config_json);
		if (!token) return NULL;

		snprintf(url, sizeof(url),
			"https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media",
			bucket, obj_key);
		snprintf(auth_hdr, sizeof(auth_hdr),
				 "Authorization: Bearer %s", token);

		headers = curl_slist_append(headers, auth_hdr);
		result = http_do_request("GET", url, headers, NULL, 0);
		curl_slist_free_all(headers);
	}
	else if (strcmp(provider, "azure") == 0)
	{
		/* azure://account/container/blob?<sas_token> */
		char	account[128] = "";
		char	container[256] = "";
		char	blob_path[1024] = "";
		char	sas[512] = "";
		char	url[2048];

		json_extract_str(config_json, "account",   account,   sizeof(account));
		json_extract_str(config_json, "container", container, sizeof(container));
		json_extract_str(config_json, "sas_token", sas,       sizeof(sas));

		/* Extract blob path from URI */
		{
			const char *p = uri;
			if (strncmp(p, "azure://", 8) == 0) p += 8;
			/* skip account/container/ */
			{
				const char *s1 = strchr(p, '/');
				const char *s2 = s1 ? strchr(s1+1, '/') : NULL;
				if (s2) strlcpy(blob_path, s2+1, sizeof(blob_path));
			}
		}

		snprintf(url, sizeof(url),
			"https://%s.blob.core.windows.net/%s/%s?%s",
			account, container, blob_path, sas);

		headers = curl_slist_append(headers, "x-ms-version: 2020-10-02");
		result = http_do_request("GET", url, headers, NULL, 0);
		curl_slist_free_all(headers);
	}
	else
	{
		elog(WARNING, "objectstore_http: unknown provider \"%s\"", provider);
		return NULL;
	}

	return result;
}

/* ----------------------------------------------------------------
 * http_object_put
 * ---------------------------------------------------------------- */
bool
http_object_put(const char *uri, bytea *data,
				const char *config_json, const char *provider)
{
	struct curl_slist *headers = NULL;
	bytea		  *result;

	if (strcmp(provider, "gcs") == 0)
	{
		char	bucket[256] = "", obj_key[1024] = "";
		char	url[2048];
		char   *token;
		char	auth_hdr[768];
		char	clen_hdr[64];

		{
			const char *p = uri;
			if (strncmp(p, "gcs://", 6) == 0) p += 6;
			{
				const char *slash = strchr(p, '/');
				size_t		bl = slash ? (size_t)(slash-p) : strlen(p);
				if (bl >= sizeof(bucket)) bl = sizeof(bucket)-1;
				memcpy(bucket, p, bl); bucket[bl] = '\0';
				if (slash) strlcpy(obj_key, slash+1, sizeof(obj_key));
			}
		}

		token = gcs_get_access_token(config_json);
		if (!token) return false;

		snprintf(url, sizeof(url),
			"https://storage.googleapis.com/upload/storage/v1/b/%s/o"
			"?uploadType=media&name=%s",
			bucket, obj_key);
		snprintf(auth_hdr, sizeof(auth_hdr),
				 "Authorization: Bearer %s", token);
		snprintf(clen_hdr, sizeof(clen_hdr),
				 "Content-Length: %zu", VARSIZE_ANY_EXHDR(data));

		headers = curl_slist_append(headers, auth_hdr);
		headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
		headers = curl_slist_append(headers, clen_hdr);
		result = http_do_request("POST", url, headers,
								 VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data));
		curl_slist_free_all(headers);
	}
	else if (strcmp(provider, "azure") == 0)
	{
		char	account[128]="", container[256]="";
		char	sas[512]="", blob_path[1024]="";
		char	url[2048];
		char	clen_hdr[64];

		json_extract_str(config_json, "account",   account,   sizeof(account));
		json_extract_str(config_json, "container", container, sizeof(container));
		json_extract_str(config_json, "sas_token", sas,       sizeof(sas));

		{
			const char *p = uri;
			if (strncmp(p, "azure://", 8) == 0) p += 8;
			{
				const char *s1 = strchr(p, '/');
				const char *s2 = s1 ? strchr(s1+1, '/') : NULL;
				if (s2) strlcpy(blob_path, s2+1, sizeof(blob_path));
			}
		}

		snprintf(url, sizeof(url),
			"https://%s.blob.core.windows.net/%s/%s?%s",
			account, container, blob_path, sas);
		snprintf(clen_hdr, sizeof(clen_hdr),
				 "Content-Length: %zu", VARSIZE_ANY_EXHDR(data));

		headers = curl_slist_append(headers, "x-ms-version: 2020-10-02");
		headers = curl_slist_append(headers, "x-ms-blob-type: BlockBlob");
		headers = curl_slist_append(headers, clen_hdr);
		result = http_do_request("PUT", url, headers,
								 VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data));
		curl_slist_free_all(headers);
	}
	else
		return false;

	return result != NULL;
}

/* ----------------------------------------------------------------
 * http_object_delete
 * ---------------------------------------------------------------- */
bool
http_object_delete(const char *uri, const char *config_json,
				   const char *provider)
{
	struct curl_slist *headers = NULL;
	bytea		  *result;

	if (strcmp(provider, "gcs") == 0)
	{
		char	bucket[256]="", obj_key[1024]="";
		char	url[2048], auth_hdr[768];
		char   *token;

		{
			const char *p = uri;
			if (strncmp(p, "gcs://", 6) == 0) p += 6;
			{
				const char *slash = strchr(p, '/');
				size_t		bl = slash ? (size_t)(slash-p) : strlen(p);
				if (bl >= sizeof(bucket)) bl = sizeof(bucket)-1;
				memcpy(bucket, p, bl); bucket[bl] = '\0';
				if (slash) strlcpy(obj_key, slash+1, sizeof(obj_key));
			}
		}

		token = gcs_get_access_token(config_json);
		if (!token) return false;

		snprintf(url, sizeof(url),
			"https://storage.googleapis.com/storage/v1/b/%s/o/%s",
			bucket, obj_key);
		snprintf(auth_hdr, sizeof(auth_hdr),
				 "Authorization: Bearer %s", token);

		headers = curl_slist_append(headers, auth_hdr);
		result = http_do_request("DELETE", url, headers, NULL, 0);
		curl_slist_free_all(headers);
	}
	else if (strcmp(provider, "azure") == 0)
	{
		char	account[128]="", container[256]="";
		char	sas[512]="", blob_path[1024]="";
		char	url[2048];

		json_extract_str(config_json, "account",   account,   sizeof(account));
		json_extract_str(config_json, "container", container, sizeof(container));
		json_extract_str(config_json, "sas_token", sas,       sizeof(sas));

		{
			const char *p = uri;
			if (strncmp(p, "azure://", 8) == 0) p += 8;
			{
				const char *s1 = strchr(p, '/');
				const char *s2 = s1 ? strchr(s1+1, '/') : NULL;
				if (s2) strlcpy(blob_path, s2+1, sizeof(blob_path));
			}
		}

		snprintf(url, sizeof(url),
			"https://%s.blob.core.windows.net/%s/%s?%s",
			account, container, blob_path, sas);

		headers = curl_slist_append(headers, "x-ms-version: 2020-10-02");
		result = http_do_request("DELETE", url, headers, NULL, 0);
		curl_slist_free_all(headers);
	}
	else
		return false;

	return result != NULL;
}

/* ----------------------------------------------------------------
 * http_object_list
 *
 * GCS:   GET /storage/v1/b/{bucket}/o?prefix=…
 * Azure: GET https://{account}.blob.core.windows.net/{container}?
 *            restype=container&comp=list&prefix=…
 * ---------------------------------------------------------------- */
ObjstoreListResult *
http_object_list(const char *prefix, const char *config_json,
				 const char *provider, int max_keys, int *out_nresults)
{
	bytea		  *response = NULL;
	struct curl_slist *headers = NULL;
	ObjstoreListResult *results;
	int			cap = 64, n = 0;

	results = palloc(sizeof(ObjstoreListResult) * cap);

	if (strcmp(provider, "gcs") == 0)
	{
		char	bucket[256]="", pfx[1024]="";
		char	url[2048], auth_hdr[768];
		char   *token;
		char   *body, *p;

		{
			const char *q = prefix;
			if (strncmp(q, "gcs://", 6) == 0) q += 6;
			{
				const char *slash = strchr(q, '/');
				size_t		bl = slash ? (size_t)(slash-q) : strlen(q);
				if (bl >= sizeof(bucket)) bl = sizeof(bucket)-1;
				memcpy(bucket, q, bl); bucket[bl] = '\0';
				if (slash) strlcpy(pfx, slash+1, sizeof(pfx));
			}
		}

		token = gcs_get_access_token(config_json);
		if (!token) goto list_done;

		snprintf(url, sizeof(url),
			"https://storage.googleapis.com/storage/v1/b/%s/o"
			"?maxResults=%d&prefix=%s",
			bucket, max_keys, pfx);
		snprintf(auth_hdr, sizeof(auth_hdr),
				 "Authorization: Bearer %s", token);

		headers = curl_slist_append(NULL, auth_hdr);
		response = http_do_request("GET", url, headers, NULL, 0);
		curl_slist_free_all(headers);

		if (!response) goto list_done;
		body = VARDATA_ANY(response);

		/* Parse JSON array of "items": [{name, size, ...}, ...] */
		p = body;
		while ((p = strstr(p, "\"name\"")) != NULL)
		{
			char	name[1024]="", size_str[32]="0";

			p += 6; /* skip "name" */
			while (*p == ' ' || *p == ':') p++;
			if (*p == '"')
			{
				char *q = ++p;
				size_t nl = 0;
				while (*q && *q != '"' && nl < sizeof(name)-1)
					name[nl++] = *q++;
				name[nl] = '\0';
				p = q;
			}

			/* Look for size nearby */
			{
				char *sp = strstr(p, "\"size\"");
				if (sp && sp < strstr(p, "\"name\"") + 512)
				{
					sp += 6;
					while (*sp == ' ' || *sp == ':') sp++;
					if (*sp == '"')
					{
						char *q = ++sp;
						size_t sl = 0;
						while (*q && *q != '"' && sl < sizeof(size_str)-1)
							size_str[sl++] = *q++;
						size_str[sl] = '\0';
					}
				}
			}

			if (n >= cap) { cap*=2; results = repalloc(results, sizeof(ObjstoreListResult)*cap); }
			strlcpy(results[n].key, name, sizeof(results[n].key));
			results[n].size_bytes    = (int64) strtoll(size_str, NULL, 10);
			results[n].last_modified = 0;
			results[n].etag[0]       = '\0';
			n++;
		}
	}
	else if (strcmp(provider, "azure") == 0)
	{
		char	account[128]="", container[256]="";
		char	sas[512]="", pfx[1024]="";
		char	url[2048];
		char   *body, *p;

		json_extract_str(config_json, "account",   account,   sizeof(account));
		json_extract_str(config_json, "container", container, sizeof(container));
		json_extract_str(config_json, "sas_token", sas,       sizeof(sas));

		{
			const char *q = prefix;
			if (strncmp(q, "azure://", 8) == 0) q += 8;
			{
				const char *s1 = strchr(q, '/');
				const char *s2 = s1 ? strchr(s1+1, '/') : NULL;
				if (s2) strlcpy(pfx, s2+1, sizeof(pfx));
			}
		}

		snprintf(url, sizeof(url),
			"https://%s.blob.core.windows.net/%s"
			"?restype=container&comp=list&maxresults=%d&prefix=%s&%s",
			account, container, max_keys, pfx, sas);

		headers = curl_slist_append(NULL, "x-ms-version: 2020-10-02");
		response = http_do_request("GET", url, headers, NULL, 0);
		curl_slist_free_all(headers);

		if (!response) goto list_done;
		body = VARDATA_ANY(response);

		/* Azure returns XML: <Name>...</Name><Content-Length>N</Content-Length> */
		p = body;
		while ((p = strstr(p, "<Name>")) != NULL)
		{
			char	name[1024]="", size_str[32]="0";
			char   *e;

			p += 6;
			e = strstr(p, "</Name>");
			if (e)
			{
				size_t nl = (size_t)(e-p);
				if (nl >= sizeof(name)) nl = sizeof(name)-1;
				memcpy(name, p, nl); name[nl]='\0';
				p = e;
			}

			{
				char *sp = strstr(p-32, "<Content-Length>");
				if (sp && sp < p + 512)
				{
					char *se;
					sp += 16;
					se = strstr(sp, "</Content-Length>");
					if (se)
					{
						size_t sl = (size_t)(se-sp);
						if (sl >= sizeof(size_str)) sl = sizeof(size_str)-1;
						memcpy(size_str, sp, sl); size_str[sl]='\0';
					}
				}
			}

			if (n >= cap) { cap*=2; results = repalloc(results, sizeof(ObjstoreListResult)*cap); }
			strlcpy(results[n].key, name, sizeof(results[n].key));
			results[n].size_bytes    = (int64) strtoll(size_str, NULL, 10);
			results[n].last_modified = 0;
			results[n].etag[0]       = '\0';
			n++;
		}
	}

list_done:
	*out_nresults = n;
	return results;
}
