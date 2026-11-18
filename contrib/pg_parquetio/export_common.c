/*-------------------------------------------------------------------------
 *
 * export_common.c
 *	  Shared infrastructure for the pg_parquetio writers.
 *
 *	  - classification of query result columns into export kinds
 *	  - conversion of PostgreSQL values (epoch shifts, UTF-8 conversion,
 *	    detoasting of vectors/arrays)
 *	  - privilege and path checks for server-side output files (same
 *	    rule as COPY ... TO 'file': superuser or pg_write_server_files)
 *	  - an output file that is written under a temporary name and renamed
 *	    into place only when the export completes
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_parquetio/export_common.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "datatype/timestamp.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/vector.h"

#include "pg_parquetio.h"
#include "export_common.h"

/* microseconds / days between 1970-01-01 and 2000-01-01 */
#define PG_TO_UNIX_DAYS		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define PG_TO_UNIX_USECS	((int64) PG_TO_UNIX_DAYS * USECS_PER_DAY)

#define EXPORT_FILE_BUFSIZE	(1024 * 1024)

/* ----------------------------------------------------------------
 * Column classification
 * ----------------------------------------------------------------
 */
ExportColumn *
export_describe(TupleDesc tupdesc, int *ncols)
{
	ExportColumn *cols;
	int			n = tupdesc->natts;
	int			i,
				j;

	if (n == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("query must return at least one column")));

	cols = palloc0(sizeof(ExportColumn) * n);

	for (i = 0; i < n; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		ExportColumn *c = &cols[i];
		const char *nm = NameStr(att->attname);
		int32		basetypmod = att->atttypmod;

		c->name = pg_server_to_any(nm, strlen(nm), PG_UTF8);
		c->namelen = strlen(c->name);
		c->typid = att->atttypid;
		c->typmod = att->atttypmod;
		c->basetype = getBaseTypeAndTypmod(att->atttypid, &basetypmod);
		c->fixed_dim = -1;

		switch (c->basetype)
		{
			case BOOLOID:
				c->kind = EK_BOOL;
				break;
			case INT2OID:
				c->kind = EK_INT16;
				break;
			case INT4OID:
				c->kind = EK_INT32;
				break;
			case INT8OID:
				c->kind = EK_INT64;
				break;
			case FLOAT4OID:
				c->kind = EK_FLOAT4;
				break;
			case FLOAT8OID:
				c->kind = EK_FLOAT8;
				break;
			case TEXTOID:
			case VARCHAROID:
			case BPCHAROID:
				c->kind = EK_TEXT;
				break;
			case BYTEAOID:
				c->kind = EK_BYTEA;
				break;
			case DATEOID:
				c->kind = EK_DATE;
				break;
			case TIMESTAMPOID:
				c->kind = EK_TIMESTAMP;
				break;
			case TIMESTAMPTZOID:
				c->kind = EK_TIMESTAMPTZ;
				break;
			case UUIDOID:
				c->kind = EK_UUID;
				break;
			case JSONOID:
				c->kind = EK_JSON;
				break;
			case JSONBOID:
				c->kind = EK_JSON;
				c->use_outfunc = true;
				break;
			case VECTOROID:
				c->kind = EK_VECTOR;
				if (basetypmod > 0)
					c->fixed_dim = basetypmod;
				break;
			case FLOAT4ARRAYOID:
				c->kind = EK_FLOAT4_ARRAY;
				break;
			case FLOAT8ARRAYOID:
				c->kind = EK_FLOAT8_ARRAY;
				break;
			default:
				/* numeric and everything else: text via the output function */
				c->kind = EK_OUTPUT_TEXT;
				c->use_outfunc = true;
				break;
		}

		if (c->use_outfunc)
		{
			Oid			outfn;
			bool		isvarlena;

			getTypeOutputInfo(att->atttypid, &outfn, &isvarlena);
			fmgr_info(outfn, &c->outfunc);
		}

		for (j = 0; j < i; j++)
		{
			if (strcmp(cols[j].name, c->name) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("query result contains duplicate column name \"%s\"",
								nm),
						 errhint("Use column aliases to give every output column a unique name.")));
		}
	}

	*ncols = n;
	return cols;
}

/*
 * Prepare the query and open a no-scroll cursor on it, so the writers can
 * fetch the result in batches instead of materializing it.
 * Caller must be SPI-connected.
 */
Portal
export_open_cursor(const char *query)
{
	SPIPlanPtr	plan;
	Portal		portal;

	plan = SPI_prepare(query, 0, NULL);
	if (plan == NULL)
		elog(ERROR, "SPI_prepare failed: %s", SPI_result_code_string(SPI_result));

	if (!SPI_is_cursor_plan(plan))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("query must be a single statement that returns rows")));

	portal = SPI_cursor_open(NULL, plan, NULL, NULL, false);
	if (portal == NULL)
		elog(ERROR, "SPI_cursor_open failed: %s",
			 SPI_result_code_string(SPI_result));
	return portal;
}

/* ----------------------------------------------------------------
 * Value conversion
 * ----------------------------------------------------------------
 */

/*
 * Text bytes (UTF-8) for EK_TEXT / EK_JSON / EK_OUTPUT_TEXT / EK_BYTEA.
 * The result may point into a detoasted copy that lives in the current
 * memory context.
 */
const char *
export_text_value(ExportColumn *col, Datum d, int *len)
{
	const char *s;
	int			slen;
	char	   *conv;

	if (col->use_outfunc)
	{
		s = OutputFunctionCall(&col->outfunc, d);
		slen = strlen(s);
	}
	else
	{
		struct varlena *v = PG_DETOAST_DATUM_PACKED(d);

		s = VARDATA_ANY(v);
		slen = VARSIZE_ANY_EXHDR(v);
	}

	if (col->kind == EK_BYTEA)
	{
		*len = slen;
		return s;
	}

	conv = pg_server_to_any(s, slen, PG_UTF8);
	if (conv != s)
		slen = strlen(conv);
	*len = slen;
	return conv;
}

/* date -> days since 1970-01-01 */
int32
export_date_value(Datum d)
{
	DateADT		v = DatumGetDateADT(d);

	if (DATE_NOT_FINITE(v))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("cannot export infinite date value")));
	return (int32) v + PG_TO_UNIX_DAYS;
}

/* timestamp[tz] -> microseconds since 1970-01-01 00:00:00 (UTC for tz) */
int64
export_timestamp_value(Datum d)
{
	Timestamp	v = DatumGetTimestamp(d);
	int64		r;

	if (TIMESTAMP_NOT_FINITE(v))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("cannot export infinite timestamp value")));
	if (pg_add_s64_overflow(v, PG_TO_UNIX_USECS, &r))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));
	return r;
}

const float *
export_vector_value(ExportColumn *col, Datum d, int *dim)
{
	Vector	   *v = DatumGetVector(d);

	if (col->fixed_dim > 0 && v->dim != col->fixed_dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector in column \"%s\" has %d dimensions, expected %d",
						col->name, v->dim, col->fixed_dim)));
	*dim = v->dim;
	return v->x;
}

/* Deconstruct a one-dimensional float4[]/float8[]; returns element count */
int
export_float_array_value(ExportColumn *col, Datum d, Datum **elems, bool **nulls)
{
	ArrayType  *arr = DatumGetArrayTypeP(d);
	int			n;

	if (ARR_NDIM(arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot export multidimensional array in column \"%s\"",
						col->name),
				 errhint("Only one-dimensional float4[] and float8[] arrays are exported as lists.")));

	if (col->kind == EK_FLOAT4_ARRAY)
		deconstruct_array(arr, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT,
						  elems, nulls, &n);
	else
		deconstruct_array(arr, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL,
						  TYPALIGN_DOUBLE, elems, nulls, &n);
	return n;
}

/* ----------------------------------------------------------------
 * Little-endian appenders
 * ----------------------------------------------------------------
 */
void
append_le16(StringInfo s, uint16 v)
{
	char		b[2];

	b[0] = (char) (v & 0xff);
	b[1] = (char) (v >> 8);
	appendBinaryStringInfo(s, b, 2);
}

void
append_le32(StringInfo s, uint32 v)
{
	char		b[4];
	int			i;

	for (i = 0; i < 4; i++)
		b[i] = (char) ((v >> (8 * i)) & 0xff);
	appendBinaryStringInfo(s, b, 4);
}

void
append_le64(StringInfo s, uint64 v)
{
	char		b[8];
	int			i;

	for (i = 0; i < 8; i++)
		b[i] = (char) ((v >> (8 * i)) & 0xff);
	appendBinaryStringInfo(s, b, 8);
}

void
append_f32(StringInfo s, float v)
{
	uint32		u;

	memcpy(&u, &v, 4);
	append_le32(s, u);
}

void
append_f64(StringInfo s, double v)
{
	uint64		u;

	memcpy(&u, &v, 8);
	append_le64(s, u);
}

void
append_f32_array(StringInfo s, const float *v, int n)
{
#ifdef WORDS_BIGENDIAN
	int			i;

	for (i = 0; i < n; i++)
		append_f32(s, v[i]);
#else
	appendBinaryStringInfo(s, (const char *) v, (int) (sizeof(float) * n));
#endif
}

/* ----------------------------------------------------------------
 * Output path checks
 * ----------------------------------------------------------------
 */


/*
 * Resolve a path to its canonical absolute form (forward slashes).  On POSIX
 * this is realpath() (the path must exist); on Windows _fullpath(), plus an
 * explicit existence check, which _fullpath() does not do.
 */
static char *
export_realpath(const char *path, char *resolved)
{
#ifdef WIN32
	struct stat st;

	if (_fullpath(resolved, path, PARQUET_MAX_PATH_LEN) == NULL)
		return NULL;
	if (stat(resolved, &st) != 0)
		return NULL;
	canonicalize_path(resolved);
	return resolved;
#else
	return realpath(path, resolved);
#endif
}

/* Is resolved absolute path "full" inside one of the allowed directories? */
static bool
export_path_allowed(const char *full)
{
	char	   *copy;
	char	   *tok;
	char	   *saveptr;
	bool		ok = false;

	if (pg_parquetio_allowed_paths == NULL ||
		pg_parquetio_allowed_paths[0] == '\0')
		return false;

	copy = pstrdup(pg_parquetio_allowed_paths);
	for (tok = strtok_r(copy, ",", &saveptr); tok != NULL && !ok;
		 tok = strtok_r(NULL, ",", &saveptr))
	{
		char		resolved[PARQUET_MAX_PATH_LEN];
		char	   *dir;
		size_t		dl;

		while (*tok == ' ')
			tok++;
		dl = strlen(tok);
		while (dl > 0 && tok[dl - 1] == ' ')
			tok[--dl] = '\0';
		if (dl == 0 || !is_absolute_path(tok))
			continue;

		canonicalize_path(tok);
		dir = export_realpath(tok, resolved) ? resolved : tok;
		dl = strlen(dir);
		while (dl > 1 && dir[dl - 1] == '/')
			dir[--dl] = '\0';

		if (strcmp(dir, "/") == 0)
			ok = true;
		else if (strncmp(full, dir, dl) == 0 && full[dl] == '/')
			ok = true;
	}
	pfree(copy);
	return ok;
}

/*
 * Check that the current user may write server files, and resolve the
 * output path: it must be absolute, its directory must exist, and the
 * resolved location must lie inside pg_parquetio.allowed_paths.
 * Returns the resolved path (palloc'd).
 */
char *
export_check_output_path(const char *path)
{
	char	   *copy;
	char	   *sep;
	const char *base;
	const char *dir;
	char		resolved[PARQUET_MAX_PATH_LEN];
	char	   *full;

	if (!has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to write server file"),
				 errdetail("Only roles with privileges of the \"%s\" role may write files on the server.",
						   "pg_write_server_files")));

	if (path[0] == '\0' || !is_absolute_path(path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("output path must be absolute: \"%s\"", path)));

	if (strlen(path) >= PARQUET_MAX_PATH_LEN - 64)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("output path is too long")));

	/* A trailing separator names a directory (canonicalize_path drops it) */
	if (path[strlen(path) - 1] == '/' || path[strlen(path) - 1] == '\\')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("output path \"%s\" does not name a file", path)));

	copy = pstrdup(path);
	canonicalize_path(copy);	/* also turns '\\' into '/' on Windows */
	sep = strrchr(copy, '/');
	if (sep == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("output path must be absolute: \"%s\"", path)));
	*sep = '\0';
	base = sep + 1;
	dir = (copy[0] == '\0') ? "/" : copy;

	if (base[0] == '\0' || strcmp(base, ".") == 0 || strcmp(base, "..") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("output path \"%s\" does not name a file", path)));

	if (export_realpath(dir, resolved) == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not access output directory \"%s\": %m", dir)));

	if (strcmp(resolved, "/") == 0)
		full = psprintf("/%s", base);
	else
		full = psprintf("%s/%s", resolved, base);

	if (!export_path_allowed(full))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("output path \"%s\" is not in an allowed directory", path),
				 errhint("Add the directory to \"pg_parquetio.allowed_paths\".")));

	pfree(copy);
	return full;
}

/* ----------------------------------------------------------------
 * Output file
 * ----------------------------------------------------------------
 */
ExportFile *
export_file_open(const char *path)
{
	ExportFile *f = palloc0(sizeof(ExportFile));

	f->path = pstrdup(path);
	f->tmppath = psprintf("%s.tmp.%d", path, MyProcPid);
	f->fd = -1;

	/* a stale temp file can only be ours from an earlier crash */
	(void) unlink(f->tmppath);
	f->fd = OpenTransientFile(f->tmppath, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
	if (f->fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", f->tmppath)));
	initStringInfo(&f->buf);
	return f;
}

static void
export_file_flush(ExportFile *f)
{
	const char *p = f->buf.data;
	Size		left = f->buf.len;

	while (left > 0)
	{
		ssize_t		w;

		errno = 0;
		w = write(f->fd, p, left);
		if (w <= 0)
		{
			if (errno == EINTR)
				continue;
			if (errno == 0)
				errno = ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m", f->tmppath)));
		}
		p += w;
		left -= w;
	}
	resetStringInfo(&f->buf);
}

void
export_file_write(ExportFile *f, const void *data, Size len)
{
	f->offset += len;
	if (len >= EXPORT_FILE_BUFSIZE)
	{
		const char *p = data;

		export_file_flush(f);
		/* write large pieces in chunks through the buffer */
		while (len > 0)
		{
			Size		n = Min(len, (Size) EXPORT_FILE_BUFSIZE);

			appendBinaryStringInfo(&f->buf, p, n);
			export_file_flush(f);
			p += n;
			len -= n;
		}
		return;
	}
	appendBinaryStringInfo(&f->buf, data, len);
	if (f->buf.len >= EXPORT_FILE_BUFSIZE)
		export_file_flush(f);
}

void
export_file_commit(ExportFile *f)
{
	export_file_flush(f);
	if (CloseTransientFile(f->fd) != 0)
	{
		f->fd = -1;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", f->tmppath)));
	}
	f->fd = -1;
	if (rename(f->tmppath, f->path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						f->tmppath, f->path)));
}

/* Error cleanup: close and remove the temporary file.  Never throws. */
void
export_file_abort(ExportFile *f)
{
	if (f == NULL)
		return;
	if (f->fd >= 0)
	{
		(void) CloseTransientFile(f->fd);
		f->fd = -1;
	}
	(void) unlink(f->tmppath);
}
