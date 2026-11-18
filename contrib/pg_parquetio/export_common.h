/*-------------------------------------------------------------------------
 *
 * export_common.h
 *	  Shared infrastructure for the pg_parquetio writers
 *	  (write_parquet, write_arrow, arrow_ipc_stream): result-column type
 *	  classification, value conversion, output-path checks, and a
 *	  buffered, atomically-renamed output file.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_parquetio/export_common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPORT_COMMON_H
#define EXPORT_COMMON_H

#include "access/tupdesc.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/portal.h"

/* How a result column is exported */
typedef enum ExportKind
{
	EK_BOOL,
	EK_INT16,
	EK_INT32,
	EK_INT64,
	EK_FLOAT4,
	EK_FLOAT8,
	EK_TEXT,					/* text/varchar/bpchar: varlena used directly */
	EK_BYTEA,
	EK_DATE,
	EK_TIMESTAMP,
	EK_TIMESTAMPTZ,
	EK_UUID,
	EK_JSON,					/* json (direct) or jsonb (output function) */
	EK_VECTOR,					/* core vector type: list of float32 */
	EK_FLOAT4_ARRAY,			/* 1-D float4[]: list of float32 */
	EK_FLOAT8_ARRAY,			/* 1-D float8[]: list of float64 */
	EK_OUTPUT_TEXT				/* anything else: type output function */
} ExportKind;

typedef struct ExportColumn
{
	char	   *name;			/* UTF-8 column name */
	int			namelen;
	Oid			typid;			/* declared type */
	Oid			basetype;		/* after stripping domains */
	int32		typmod;
	ExportKind	kind;
	int			fixed_dim;		/* vector(n): n, else -1 */
	bool		use_outfunc;	/* text produced by the output function */
	FmgrInfo	outfunc;
} ExportColumn;

/* Batch size used when fetching from the SPI cursor */
#define EXPORT_FETCH_ROWS	1000

/* Close a row group / record batch early once this much data is buffered */
#define EXPORT_MAX_BUFFERED	((Size) 128 * 1024 * 1024)

extern ExportColumn *export_describe(TupleDesc tupdesc, int *ncols);
extern Portal export_open_cursor(const char *query);

/* value conversion */
extern const char *export_text_value(ExportColumn *col, Datum d, int *len);
extern int32 export_date_value(Datum d);
extern int64 export_timestamp_value(Datum d);
extern const float *export_vector_value(ExportColumn *col, Datum d, int *dim);
extern int	export_float_array_value(ExportColumn *col, Datum d,
									 Datum **elems, bool **nulls);

/* little-endian appenders */
extern void append_le16(StringInfo s, uint16 v);
extern void append_le32(StringInfo s, uint32 v);
extern void append_le64(StringInfo s, uint64 v);
extern void append_f32(StringInfo s, float v);
extern void append_f64(StringInfo s, double v);
extern void append_f32_array(StringInfo s, const float *v, int n);

/* output files */
typedef struct ExportFile
{
	char	   *path;			/* final, resolved path */
	char	   *tmppath;		/* written first, renamed on success */
	int			fd;
	uint64		offset;			/* logical bytes written so far */
	StringInfoData buf;
} ExportFile;

extern char *export_check_output_path(const char *path);
extern ExportFile *export_file_open(const char *path);
extern void export_file_write(ExportFile *f, const void *data, Size len);
extern void export_file_commit(ExportFile *f);
extern void export_file_abort(ExportFile *f);

#endif							/* EXPORT_COMMON_H */
