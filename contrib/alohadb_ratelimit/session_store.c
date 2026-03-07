#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/snapmgr.h"
#include "alohadb_ratelimit.h"

PG_FUNCTION_INFO_V1(session_set);
PG_FUNCTION_INFO_V1(session_get);
PG_FUNCTION_INFO_V1(session_get_all);
PG_FUNCTION_INFO_V1(session_delete);
PG_FUNCTION_INFO_V1(session_touch);

/*
 * session_set(session_id text, key text, value jsonb, ttl interval DEFAULT '24 hours')
 */
Datum
session_set(PG_FUNCTION_ARGS)
{
    char *session_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *key = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *value_str;
    char *ttl_str;
    StringInfoData sql;
    Datum jsonb_datum = PG_GETARG_DATUM(2);

    value_str = DatumGetCString(DirectFunctionCall1(jsonb_out, jsonb_datum));
    ttl_str = PG_ARGISNULL(3) ? "24 hours" : text_to_cstring(PG_GETARG_TEXT_PP(3));

    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "INSERT INTO %s (session_id, key, value, expires_at) "
        "VALUES (%s, %s, %s::jsonb, now() + %s::interval) "
        "ON CONFLICT (session_id, key) DO UPDATE SET "
        "value = EXCLUDED.value, expires_at = EXCLUDED.expires_at",
        RATELIMIT_SESSION_TABLE,
        quote_literal_cstr(session_id),
        quote_literal_cstr(key),
        quote_literal_cstr(value_str),
        quote_literal_cstr(ttl_str));

    SPI_execute(sql.data, false, 0);

    PopActiveSnapshot();
    SPI_finish();

    PG_RETURN_VOID();
}

/*
 * session_get(session_id text, key text) -> jsonb
 */
Datum
session_get(PG_FUNCTION_ARGS)
{
    char *session_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *key = text_to_cstring(PG_GETARG_TEXT_PP(1));
    StringInfoData sql;
    int ret;
    Datum result;
    char *json_copy = NULL;

    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT value::text FROM %s "
        "WHERE session_id = %s AND key = %s AND expires_at > now()",
        RATELIMIT_SESSION_TABLE,
        quote_literal_cstr(session_id),
        quote_literal_cstr(key));

    ret = SPI_execute(sql.data, true, 1);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        if (val)
        {
            MemoryContext oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
            json_copy = pstrdup(val);
            MemoryContextSwitchTo(oldcxt);
        }
    }

    PopActiveSnapshot();
    SPI_finish();

    if (json_copy)
    {
        result = DirectFunctionCall1(jsonb_in, CStringGetDatum(json_copy));
        PG_RETURN_DATUM(result);
    }

    PG_RETURN_NULL();
}

/*
 * session_get_all(session_id text) -> jsonb
 * Returns all key-value pairs for a session as a single jsonb object.
 */
Datum
session_get_all(PG_FUNCTION_ARGS)
{
    char *session_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
    StringInfoData sql;
    StringInfoData json_buf;
    int ret;
    uint64 nrows, i;
    Datum result;
    char *json_copy = NULL;

    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT key, value::text FROM %s "
        "WHERE session_id = %s AND expires_at > now()",
        RATELIMIT_SESSION_TABLE,
        quote_literal_cstr(session_id));

    ret = SPI_execute(sql.data, true, 0);
    nrows = SPI_processed;

    initStringInfo(&json_buf);
    appendStringInfoChar(&json_buf, '{');

    if (ret == SPI_OK_SELECT && nrows > 0)
    {
        for (i = 0; i < nrows; i++)
        {
            char *k = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            char *v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);

            if (i > 0) appendStringInfoString(&json_buf, ", ");
            appendStringInfo(&json_buf, "\"%s\": %s", k, v ? v : "null");
        }
    }

    appendStringInfoChar(&json_buf, '}');

    {
        MemoryContext oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        json_copy = pstrdup(json_buf.data);
        MemoryContextSwitchTo(oldcxt);
    }

    PopActiveSnapshot();
    SPI_finish();

    result = DirectFunctionCall1(jsonb_in, CStringGetDatum(json_copy));
    PG_RETURN_DATUM(result);
}

Datum
session_delete(PG_FUNCTION_ARGS)
{
    char *session_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
    StringInfoData sql;

    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "DELETE FROM %s WHERE session_id = %s",
        RATELIMIT_SESSION_TABLE,
        quote_literal_cstr(session_id));

    SPI_execute(sql.data, false, 0);

    PopActiveSnapshot();
    SPI_finish();

    PG_RETURN_VOID();
}

/*
 * session_touch(session_id text, ttl interval DEFAULT '24 hours')
 * Extend the TTL on all keys for a session.
 */
Datum
session_touch(PG_FUNCTION_ARGS)
{
    char *session_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *ttl_str = PG_ARGISNULL(1) ? "24 hours" : text_to_cstring(PG_GETARG_TEXT_PP(1));
    StringInfoData sql;

    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "UPDATE %s SET expires_at = now() + %s::interval "
        "WHERE session_id = %s",
        RATELIMIT_SESSION_TABLE,
        quote_literal_cstr(ttl_str),
        quote_literal_cstr(session_id));

    SPI_execute(sql.data, false, 0);

    PopActiveSnapshot();
    SPI_finish();

    PG_RETURN_VOID();
}
