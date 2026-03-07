/*-------------------------------------------------------------------------
 *
 * flower_client.c
 *	  HTTP REST client for Flower server communication.
 *
 *	  Provides functions to fetch aggregated model weights from and
 *	  send gradient updates to a Flower aggregation server.  Uses
 *	  libcurl for HTTP transport and manual JSON serialization of
 *	  float weight arrays.
 *
 *	  The Flower REST bridge is expected to expose:
 *	    GET  /weights/<model_name>   - returns JSON {"weights": [...]}
 *	    POST /gradients/<model_name> - accepts JSON with gradients
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_federated/flower_client.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <curl/curl.h>
#include <math.h>
#include <string.h>

#include "lib/stringinfo.h"
#include "utils/memutils.h"

#include "alohadb_federated.h"

/* ----------------------------------------------------------------
 * curl write callback: accumulate response into a StringInfo
 * ----------------------------------------------------------------
 */
typedef struct FlCurlResponse
{
	StringInfoData buf;
	bool		overflow;
} FlCurlResponse;

static size_t
fl_curl_write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t			realsize = size * nmemb;
	FlCurlResponse *resp = (FlCurlResponse *) userp;

	if (resp->overflow)
		return realsize;

	if (resp->buf.len + (int) realsize > FL_MAX_RESPONSE_LEN)
	{
		resp->overflow = true;
		return realsize;
	}

	appendBinaryStringInfo(&resp->buf, (const char *) contents, (int) realsize);
	return realsize;
}

/* ----------------------------------------------------------------
 * parse_float_array
 *
 * Extract a JSON array of floats from a string starting at *pos.
 * Expects the input to be positioned at '['.  Advances *pos past
 * the closing ']'.  Returns a palloc'd float array and sets *count.
 * Returns NULL on parse error.
 * ----------------------------------------------------------------
 */
static float *
parse_float_array(const char *json, int *pos, int *count)
{
	float	   *result;
	int			capacity = 256;
	int			n = 0;
	int			p = *pos;

	/* Skip whitespace */
	while (json[p] == ' ' || json[p] == '\t' || json[p] == '\n' || json[p] == '\r')
		p++;

	if (json[p] != '[')
		return NULL;
	p++;							/* skip '[' */

	result = (float *) palloc(sizeof(float) * capacity);

	while (json[p] != '\0')
	{
		char	   *endptr;
		double		val;

		/* Skip whitespace */
		while (json[p] == ' ' || json[p] == '\t' || json[p] == '\n' || json[p] == '\r')
			p++;

		if (json[p] == ']')
		{
			p++;				/* skip ']' */
			break;
		}

		/* Skip comma between elements */
		if (json[p] == ',')
		{
			p++;
			continue;
		}

		/* Parse a float value */
		val = strtod(&json[p], &endptr);
		if (endptr == &json[p])
		{
			pfree(result);
			return NULL;		/* parse error */
		}
		p = (int) (endptr - json);

		/* Grow array if needed */
		if (n >= capacity)
		{
			capacity *= 2;
			if (capacity > FL_MAX_WEIGHTS)
			{
				pfree(result);
				return NULL;
			}
			result = (float *) repalloc(result, sizeof(float) * capacity);
		}

		result[n++] = (float) val;
	}

	*pos = p;
	*count = n;
	return result;
}

/* ----------------------------------------------------------------
 * find_weights_array
 *
 * Locate the "weights" key in a JSON object and parse its array.
 * Returns a palloc'd float array, sets *num_weights.
 * Returns NULL on failure.
 * ----------------------------------------------------------------
 */
static float *
find_weights_array(const char *json, int *num_weights)
{
	const char *key;
	int			pos;

	/* Find "weights" key */
	key = strstr(json, "\"weights\"");
	if (key == NULL)
		return NULL;

	/* Advance past "weights" and the colon */
	pos = (int) (key - json) + 9;	/* length of "\"weights\"" */
	while (json[pos] == ' ' || json[pos] == ':' || json[pos] == '\t')
		pos++;

	return parse_float_array(json, &pos, num_weights);
}

/* ----------------------------------------------------------------
 * serialize_float_array
 *
 * Build a JSON array string from a float array.
 * Returns a palloc'd string.
 * ----------------------------------------------------------------
 */
static char *
serialize_float_array(const float *arr, int count)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');

	for (i = 0; i < count; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');

		if (isnan(arr[i]))
			appendStringInfoString(&buf, "0.0");
		else if (isinf(arr[i]))
			appendStringInfo(&buf, "%s", arr[i] > 0 ? "1e38" : "-1e38");
		else
			appendStringInfo(&buf, "%.8g", (double) arr[i]);
	}

	appendStringInfoChar(&buf, ']');
	return buf.data;
}

/* ================================================================
 * fl_client_get_weights
 *
 * Fetch aggregated model weights from the Flower server.
 * ================================================================
 */
float *
fl_client_get_weights(const char *server_url, const char *model_name,
					  int *num_weights)
{
	CURL		   *curl;
	CURLcode		res;
	FlCurlResponse	response;
	StringInfoData	url;
	long			http_code;
	float		   *weights;

	*num_weights = 0;

	/* Build the URL: GET /weights/<model_name> */
	initStringInfo(&url);
	appendStringInfo(&url, "%s/weights/%s", server_url, model_name);

	curl = curl_easy_init();
	if (curl == NULL)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: could not initialize libcurl")));
		pfree(url.data);
		return NULL;
	}

	initStringInfo(&response.buf);
	response.overflow = false;

	curl_easy_setopt(curl, CURLOPT_URL, url.data);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, fl_curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long) FL_HTTP_TIMEOUT_SECS);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

	res = curl_easy_perform(curl);

	if (res != CURLE_OK)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: GET %s failed: %s",
						url.data, curl_easy_strerror(res))));
		curl_easy_cleanup(curl);
		pfree(url.data);
		if (response.buf.data)
			pfree(response.buf.data);
		return NULL;
	}

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	curl_easy_cleanup(curl);
	pfree(url.data);

	if (response.overflow)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: weight response exceeded %d bytes",
						FL_MAX_RESPONSE_LEN)));
		pfree(response.buf.data);
		return NULL;
	}

	if (http_code < 200 || http_code >= 300)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: Flower server returned HTTP %ld",
						http_code),
				 errdetail("Response: %.512s", response.buf.data)));
		pfree(response.buf.data);
		return NULL;
	}

	/* Parse the JSON response to extract the weights array */
	weights = find_weights_array(response.buf.data, num_weights);
	pfree(response.buf.data);

	if (weights == NULL)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: could not parse weights from "
						"Flower server response")));
		return NULL;
	}

	elog(DEBUG1, "alohadb_federated: received %d weights from Flower server",
		 *num_weights);

	return weights;
}

/* ================================================================
 * fl_client_send_gradients
 *
 * POST gradient updates to the Flower server.
 * ================================================================
 */
bool
fl_client_send_gradients(const char *server_url, const char *model_name,
						 const float *gradients, int num_weights,
						 int64 num_samples, double loss)
{
	CURL		   *curl;
	CURLcode		res;
	struct curl_slist *headers = NULL;
	FlCurlResponse	response;
	StringInfoData	url;
	StringInfoData	body;
	char		   *weights_json;
	long			http_code;

	/* Build the URL: POST /gradients/<model_name> */
	initStringInfo(&url);
	appendStringInfo(&url, "%s/gradients/%s", server_url, model_name);

	/* Serialize the gradients to JSON */
	weights_json = serialize_float_array(gradients, num_weights);

	/* Build the JSON request body */
	initStringInfo(&body);
	appendStringInfo(&body,
					 "{\"gradients\":%s,"
					 "\"num_samples\":" INT64_FORMAT ","
					 "\"loss\":%.8g}",
					 weights_json, num_samples, loss);
	pfree(weights_json);

	curl = curl_easy_init();
	if (curl == NULL)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: could not initialize libcurl")));
		pfree(url.data);
		pfree(body.data);
		return false;
	}

	initStringInfo(&response.buf);
	response.overflow = false;

	headers = curl_slist_append(headers, "Content-Type: application/json");

	curl_easy_setopt(curl, CURLOPT_URL, url.data);
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, fl_curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long) FL_HTTP_TIMEOUT_SECS);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

	res = curl_easy_perform(curl);

	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: POST %s failed: %s",
						url.data, curl_easy_strerror(res))));
		curl_easy_cleanup(curl);
		pfree(url.data);
		pfree(body.data);
		if (response.buf.data)
			pfree(response.buf.data);
		return false;
	}

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	curl_easy_cleanup(curl);
	pfree(url.data);
	pfree(body.data);

	if (response.buf.data)
		pfree(response.buf.data);

	if (http_code < 200 || http_code >= 300)
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: Flower server returned HTTP %ld "
						"for gradient POST", http_code)));
		return false;
	}

	elog(DEBUG1, "alohadb_federated: sent %d gradient updates to Flower server "
		 "(samples=" INT64_FORMAT ", loss=%.6f)",
		 num_weights, num_samples, loss);

	return true;
}
