/*-------------------------------------------------------------------------
 *
 * alohadb_inference.c
 *	  In-database ML inference extension using ONNX Runtime.
 *
 *	  Provides UDFs for loading ONNX models into an in-memory cache and
 *	  running inference directly inside PostgreSQL.  When ONNX Runtime is
 *	  not available at compile time, every function raises a clear ERROR.
 *
 *	  UDFs:
 *	    alohadb_load_model(name text, model_data bytea) RETURNS void
 *	    alohadb_unload_model(name text) RETURNS void
 *	    alohadb_infer(model_name text, input_data vector) RETURNS vector
 *	    alohadb_batch_infer(model_name text, input_data vector[])
 *	        RETURNS vector[]
 *	    alohadb_list_models() RETURNS TABLE(...)
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_inference/alohadb_inference.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/spin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/vector.h"

#include "alohadb_inference.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_inference",
					.version = "1.0"
);

/* UDF declarations */
PG_FUNCTION_INFO_V1(alohadb_load_model);
PG_FUNCTION_INFO_V1(alohadb_unload_model);
PG_FUNCTION_INFO_V1(alohadb_infer);
PG_FUNCTION_INFO_V1(alohadb_batch_infer);
PG_FUNCTION_INFO_V1(alohadb_list_models);

/* GUC variables */
int			alohadb_inference_device = INFERENCE_DEVICE_CPU;
int			alohadb_inference_max_models = DEFAULT_MAX_MODELS;

/* Enum options for the inference device GUC */
static const struct config_enum_entry inference_device_options[] =
{
	{"cpu", INFERENCE_DEVICE_CPU, false},
	{"cuda", INFERENCE_DEVICE_CUDA, false},
	{NULL, 0, false}
};

#ifdef HAVE_ONNXRUNTIME
/* Global ONNX Runtime environment, created once in _PG_init() */
static OrtEnv *ort_env = NULL;
#endif

/*
 * _PG_init
 *	  Module load callback.
 *
 *	  Registers GUCs and initializes the model cache.  When ONNX Runtime
 *	  is available, also creates the global ORT environment.
 */
void
_PG_init(void)
{
	/* Define GUCs under the "alohadb" prefix */
	DefineCustomEnumVariable("alohadb.inference_device",
							 "Inference device to use (cpu or cuda).",
							 NULL,
							 &alohadb_inference_device,
							 INFERENCE_DEVICE_CPU,
							 inference_device_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("alohadb.inference_max_models",
							"Maximum number of ONNX models to cache.",
							NULL,
							&alohadb_inference_max_models,
							DEFAULT_MAX_MODELS,
							1,
							1024,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("alohadb");

	/* Initialize the model cache */
	model_cache_init(alohadb_inference_max_models);

#ifdef HAVE_ONNXRUNTIME
	/* Create the global ORT environment */
	{
		const OrtApi  *api = OrtGetApiBase()->GetApi(ORT_API_VERSION);
		OrtStatus	  *status;

		status = api->CreateEnv(ORT_LOGGING_LEVEL_WARNING,
								"alohadb_inference",
								&ort_env);
		if (status != NULL)
		{
			const char *msg = api->GetErrorMessage(status);

			api->ReleaseStatus(status);
			elog(WARNING,
				 "alohadb_inference: failed to create ORT environment: %s",
				 msg);
			ort_env = NULL;
		}
	}
#endif
}

/*
 * _PG_fini
 *	  Module unload callback.
 *
 *	  Releases the global ORT environment.  Note: PostgreSQL does not
 *	  normally call _PG_fini for shared libraries, but we define it for
 *	  completeness.
 */
void
_PG_fini(void)
{
#ifdef HAVE_ONNXRUNTIME
	if (ort_env != NULL)
	{
		const OrtApi *api = OrtGetApiBase()->GetApi(ORT_API_VERSION);

		api->ReleaseEnv(ort_env);
		ort_env = NULL;
	}
#endif
}

/* ----------------------------------------------------------------
 * Helper: raise an error when ONNX Runtime is not available.
 * ----------------------------------------------------------------
 */
#ifndef HAVE_ONNXRUNTIME
static void
ort_not_available(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ONNX Runtime not available"),
			 errdetail("This PostgreSQL build was compiled without "
					   "ONNX Runtime support."),
			 errhint("Rebuild PostgreSQL with ONNX Runtime to enable "
					 "ML inference.")));
}
#endif

/* ----------------------------------------------------------------
 * Helper: check an ORT status and raise ERROR on failure.
 * ----------------------------------------------------------------
 */
#ifdef HAVE_ONNXRUNTIME
static void
check_ort_status(const OrtApi *api, OrtStatus *status, const char *context)
{
	if (status != NULL)
	{
		const char *msg = api->GetErrorMessage(status);
		char	   *errmsg_copy = pstrdup(msg);

		api->ReleaseStatus(status);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("ONNX Runtime error during %s: %s",
						context, errmsg_copy)));
	}
}

/*
 * format_shape
 *	  Build a human-readable shape string like "[1, 3, 224, 224]".
 *	  Allocated in the current memory context.
 */
static char *
format_shape(int ndims, const int64_t *dims)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');
	for (i = 0; i < ndims; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ", ");
		if (dims[i] < 0)
			appendStringInfoString(&buf, "?");
		else
			appendStringInfo(&buf, INT64_FORMAT, dims[i]);
	}
	appendStringInfoChar(&buf, ']');

	return buf.data;
}
#endif

/* ================================================================
 * alohadb_load_model(name text, model_data bytea) RETURNS void
 * ================================================================
 */
Datum
alohadb_load_model(PG_FUNCTION_ARGS)
{
#ifndef HAVE_ONNXRUNTIME
	ort_not_available();
	PG_RETURN_VOID();				/* unreachable */
#else
	text	   *name_text;
	bytea	   *model_data;
	char	   *name;
	const char *data_ptr;
	Size		data_len;
	ModelCacheEntry *entry;
	const OrtApi   *api;
	OrtStatus	   *status;
	OrtSessionOptions *opts;
	OrtSession	   *session;
	OrtAllocator   *allocator;
	MemoryContext	oldctx;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("model name and data must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	model_data = PG_GETARG_BYTEA_PP(1);
	name = text_to_cstring(name_text);
	data_ptr = VARDATA_ANY(model_data);
	data_len = VARSIZE_ANY_EXHDR(model_data);

	if (ort_env == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("ONNX Runtime environment not initialized")));

	api = OrtGetApiBase()->GetApi(ORT_API_VERSION);

	/* Create session options */
	status = api->CreateSessionOptions(&opts);
	check_ort_status(api, status, "CreateSessionOptions");

	/* Configure execution provider based on GUC */
	if (alohadb_inference_device == INFERENCE_DEVICE_CUDA)
	{
		OrtCUDAProviderOptions cuda_opts;

		memset(&cuda_opts, 0, sizeof(cuda_opts));
		cuda_opts.device_id = 0;
		status = api->SessionOptionsAppendExecutionProvider_CUDA(opts,
																 &cuda_opts);
		if (status != NULL)
		{
			const char *msg = api->GetErrorMessage(status);

			api->ReleaseStatus(status);
			api->ReleaseSessionOptions(opts);
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("failed to enable CUDA execution provider: %s",
							msg),
					 errhint("Ensure CUDA and cuDNN are installed, or set "
							 "alohadb.inference_device = 'cpu'.")));
		}
	}

	/* Set thread count to 1 for resource isolation */
	status = api->SetIntraOpNumThreads(opts, 1);
	check_ort_status(api, status, "SetIntraOpNumThreads");

	status = api->SetInterOpNumThreads(opts, 1);
	check_ort_status(api, status, "SetInterOpNumThreads");

	/* Create session from in-memory model data */
	status = api->CreateSessionFromArray(ort_env,
										 data_ptr, data_len,
										 opts, &session);
	if (status != NULL)
	{
		const char *msg = api->GetErrorMessage(status);

		api->ReleaseStatus(status);
		api->ReleaseSessionOptions(opts);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("failed to load ONNX model \"%s\": %s",
						name, msg)));
	}

	/* Allocate a cache entry (evicts LRU if necessary) */
	entry = model_cache_allocate(name);

	/* Switch to TopMemoryContext for persistent allocations */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	entry->session = session;
	entry->session_options = opts;

	/* Retrieve input metadata */
	status = api->GetAllocatorWithDefaultOptions(&allocator);
	check_ort_status(api, status, "GetAllocatorWithDefaultOptions");

	{
		size_t		num_inputs;
		size_t		num_outputs;
		OrtTypeInfo *type_info;
		const OrtTensorTypeAndShapeInfo *shape_info;
		size_t		dim_count;
		char	   *ort_name;

		/* Input info */
		status = api->SessionGetInputCount(session, &num_inputs);
		check_ort_status(api, status, "SessionGetInputCount");

		if (num_inputs > 0)
		{
			status = api->SessionGetInputName(session, 0, allocator,
											  &ort_name);
			check_ort_status(api, status, "SessionGetInputName");
			entry->input_name = pstrdup(ort_name);
			status = api->AllocatorFree(allocator, ort_name);
			check_ort_status(api, status, "AllocatorFree");

			status = api->SessionGetInputTypeInfo(session, 0, &type_info);
			check_ort_status(api, status, "SessionGetInputTypeInfo");

			status = api->CastTypeInfoToTensorInfo(type_info, &shape_info);
			check_ort_status(api, status, "CastTypeInfoToTensorInfo");

			status = api->GetDimensionsCount(shape_info, &dim_count);
			check_ort_status(api, status, "GetDimensionsCount");

			entry->input_ndims = (int) dim_count;
			entry->input_dims = (int64_t *) palloc(sizeof(int64_t) * dim_count);
			status = api->GetDimensions(shape_info, entry->input_dims,
										dim_count);
			check_ort_status(api, status, "GetDimensions");

			entry->input_shape = format_shape(entry->input_ndims,
											  entry->input_dims);

			api->ReleaseTypeInfo(type_info);
		}
		else
		{
			entry->input_shape = pstrdup("[]");
		}

		/* Output info */
		status = api->SessionGetOutputCount(session, &num_outputs);
		check_ort_status(api, status, "SessionGetOutputCount");

		if (num_outputs > 0)
		{
			status = api->SessionGetOutputName(session, 0, allocator,
											   &ort_name);
			check_ort_status(api, status, "SessionGetOutputName");
			entry->output_name = pstrdup(ort_name);
			status = api->AllocatorFree(allocator, ort_name);
			check_ort_status(api, status, "AllocatorFree");

			status = api->SessionGetOutputTypeInfo(session, 0, &type_info);
			check_ort_status(api, status, "SessionGetOutputTypeInfo");

			status = api->CastTypeInfoToTensorInfo(type_info, &shape_info);
			check_ort_status(api, status, "CastTypeInfoToTensorInfo");

			status = api->GetDimensionsCount(shape_info, &dim_count);
			check_ort_status(api, status, "GetDimensionsCount");

			entry->output_ndims = (int) dim_count;
			entry->output_dims = (int64_t *) palloc(sizeof(int64_t) * dim_count);
			status = api->GetDimensions(shape_info, entry->output_dims,
										dim_count);
			check_ort_status(api, status, "GetDimensions");

			entry->output_shape = format_shape(entry->output_ndims,
											   entry->output_dims);

			api->ReleaseTypeInfo(type_info);
		}
		else
		{
			entry->output_shape = pstrdup("[]");
		}
	}

	MemoryContextSwitchTo(oldctx);

	elog(INFO, "alohadb_inference: loaded model \"%s\" "
		 "(input: %s, output: %s)",
		 name, entry->input_shape, entry->output_shape);

	pfree(name);
	PG_RETURN_VOID();
#endif							/* HAVE_ONNXRUNTIME */
}

/* ================================================================
 * alohadb_unload_model(name text) RETURNS void
 * ================================================================
 */
Datum
alohadb_unload_model(PG_FUNCTION_ARGS)
{
#ifndef HAVE_ONNXRUNTIME
	ort_not_available();
	PG_RETURN_VOID();
#else
	text   *name_text;
	char   *name;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("model name must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	name = text_to_cstring(name_text);

	model_cache_remove(name);

	elog(INFO, "alohadb_inference: unloaded model \"%s\"", name);
	pfree(name);

	PG_RETURN_VOID();
#endif
}

/* ================================================================
 * alohadb_infer(model_name text, input_data vector) RETURNS vector
 * ================================================================
 */
Datum
alohadb_infer(PG_FUNCTION_ARGS)
{
#ifndef HAVE_ONNXRUNTIME
	ort_not_available();
	PG_RETURN_NULL();
#else
	text	   *name_text;
	char	   *name;
	Vector	   *input_vec;
	ModelCacheEntry *entry;
	const OrtApi   *api;
	OrtStatus	   *status;
	OrtMemoryInfo  *mem_info;
	OrtValue	   *input_tensor = NULL;
	OrtValue	   *output_tensor = NULL;
	const char	   *input_names[1];
	const char	   *output_names[1];
	int64_t		   *input_shape;
	int				input_ndims;
	float		   *output_data;
	size_t			output_count;
	Vector		   *result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("model name and input data must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	name = text_to_cstring(name_text);
	input_vec = PG_GETARG_VECTOR_P(1);

	/* Look up the model */
	entry = model_cache_find(name);
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("model \"%s\" is not loaded", name),
				 errhint("Call alohadb_load_model() first.")));

	model_cache_touch(entry);

	api = OrtGetApiBase()->GetApi(ORT_API_VERSION);

	/* Build input shape: use model dims but override batch to 1 */
	if (entry->input_ndims > 0)
	{
		int		i;

		input_ndims = entry->input_ndims;
		input_shape = (int64_t *) palloc(sizeof(int64_t) * input_ndims);
		for (i = 0; i < input_ndims; i++)
			input_shape[i] = entry->input_dims[i];

		/* Set batch dimension (first dim) to 1 if dynamic */
		if (input_shape[0] < 0)
			input_shape[0] = 1;

		/*
		 * Set the last dimension to the vector's actual dim if the model
		 * expects a dynamic size there.
		 */
		if (input_ndims >= 2 && input_shape[input_ndims - 1] < 0)
			input_shape[input_ndims - 1] = input_vec->dim;
	}
	else
	{
		/* Fallback: treat as a 1-D tensor */
		input_ndims = 1;
		input_shape = (int64_t *) palloc(sizeof(int64_t));
		input_shape[0] = input_vec->dim;
	}

	/* Create memory info for CPU */
	status = api->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault,
									  &mem_info);
	check_ort_status(api, status, "CreateCpuMemoryInfo");

	/* Create input tensor from vector data */
	status = api->CreateTensorWithDataAsOrtValue(
		mem_info,
		input_vec->x, sizeof(float) * input_vec->dim,
		input_shape, input_ndims,
		ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT,
		&input_tensor);
	check_ort_status(api, status, "CreateTensorWithDataAsOrtValue");

	api->ReleaseMemoryInfo(mem_info);

	/* Run inference */
	input_names[0] = entry->input_name;
	output_names[0] = entry->output_name;

	status = api->Run(entry->session, NULL,
					  input_names, (const OrtValue *const *) &input_tensor, 1,
					  output_names, 1, &output_tensor);
	if (status != NULL)
	{
		const char *msg = api->GetErrorMessage(status);
		char	   *err_copy = pstrdup(msg);

		api->ReleaseStatus(status);
		api->ReleaseValue(input_tensor);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("ONNX Runtime inference failed for model \"%s\": %s",
						name, err_copy)));
	}

	/* Extract output data */
	status = api->GetTensorMutableData(output_tensor, (void **) &output_data);
	check_ort_status(api, status, "GetTensorMutableData");

	{
		OrtTensorTypeAndShapeInfo *out_info;
		size_t		total;

		status = api->GetTensorTypeAndShape(output_tensor, &out_info);
		check_ort_status(api, status, "GetTensorTypeAndShape");

		status = api->GetTensorShapeElementCount(out_info, &total);
		check_ort_status(api, status, "GetTensorShapeElementCount");

		output_count = total;
		api->ReleaseTensorTypeAndShapeInfo(out_info);
	}

	/* Clamp to VECTOR_MAX_DIM */
	if (output_count > VECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("model output has %zu elements, exceeding maximum "
						"vector dimension %d",
						output_count, VECTOR_MAX_DIM)));

	/* Build result vector */
	result = InitVector((int) output_count);
	memcpy(result->x, output_data, sizeof(float) * output_count);

	/* Clean up ORT values */
	api->ReleaseValue(input_tensor);
	api->ReleaseValue(output_tensor);
	pfree(name);
	pfree(input_shape);

	PG_RETURN_VECTOR_P(result);
#endif							/* HAVE_ONNXRUNTIME */
}

/* ================================================================
 * alohadb_batch_infer(model_name text, input_data vector[])
 *     RETURNS vector[]
 * ================================================================
 */
Datum
alohadb_batch_infer(PG_FUNCTION_ARGS)
{
#ifndef HAVE_ONNXRUNTIME
	ort_not_available();
	PG_RETURN_NULL();
#else
	text	   *name_text;
	char	   *name;
	ArrayType  *input_array;
	ModelCacheEntry *entry;
	const OrtApi   *api;
	OrtStatus	   *status;
	OrtMemoryInfo  *mem_info;
	OrtValue	   *input_tensor = NULL;
	OrtValue	   *output_tensor = NULL;
	const char	   *input_names[1];
	const char	   *output_names[1];
	Datum	   *elem_datums;
	bool	   *elem_nulls;
	int			n_inputs;
	int			vec_dim;
	float	   *batch_data;
	int			i;
	int64_t		input_shape[2];
	float	   *output_data;
	size_t		output_total;
	int			output_dim;
	Datum	   *result_datums;
	ArrayType  *result_array;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("model name and input data must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	name = text_to_cstring(name_text);
	input_array = PG_GETARG_ARRAYTYPE_P(1);

	/* Deconstruct the input array of vectors */
	deconstruct_array(input_array,
					  6000, -1, false, TYPALIGN_INT,
					  &elem_datums, &elem_nulls, &n_inputs);

	if (n_inputs == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input array must not be empty")));

	/* Determine vector dimension from the first element */
	{
		Vector *first_vec = DatumGetVector(elem_datums[0]);

		vec_dim = first_vec->dim;
	}

	/* Look up the model */
	entry = model_cache_find(name);
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("model \"%s\" is not loaded", name),
				 errhint("Call alohadb_load_model() first.")));

	model_cache_touch(entry);

	api = OrtGetApiBase()->GetApi(ORT_API_VERSION);

	/* Build a contiguous batch buffer: [n_inputs, vec_dim] */
	batch_data = (float *) palloc(sizeof(float) * n_inputs * vec_dim);
	for (i = 0; i < n_inputs; i++)
	{
		Vector *v;

		if (elem_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("input array element %d must not be NULL",
							i + 1)));

		v = DatumGetVector(elem_datums[i]);
		if (v->dim != vec_dim)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("all input vectors must have the same dimension"),
					 errdetail("Vector %d has dimension %d, expected %d.",
							   i + 1, v->dim, vec_dim)));

		memcpy(batch_data + (i * vec_dim), v->x, sizeof(float) * vec_dim);
	}

	/* Create input tensor */
	input_shape[0] = n_inputs;
	input_shape[1] = vec_dim;

	status = api->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault,
									  &mem_info);
	check_ort_status(api, status, "CreateCpuMemoryInfo");

	status = api->CreateTensorWithDataAsOrtValue(
		mem_info,
		batch_data, sizeof(float) * n_inputs * vec_dim,
		input_shape, 2,
		ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT,
		&input_tensor);
	check_ort_status(api, status, "CreateTensorWithDataAsOrtValue");

	api->ReleaseMemoryInfo(mem_info);

	/* Run inference */
	input_names[0] = entry->input_name;
	output_names[0] = entry->output_name;

	status = api->Run(entry->session, NULL,
					  input_names, (const OrtValue *const *) &input_tensor, 1,
					  output_names, 1, &output_tensor);
	if (status != NULL)
	{
		const char *msg = api->GetErrorMessage(status);
		char	   *err_copy = pstrdup(msg);

		api->ReleaseStatus(status);
		api->ReleaseValue(input_tensor);
		pfree(batch_data);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("ONNX Runtime batch inference failed for model "
						"\"%s\": %s", name, err_copy)));
	}

	/* Extract output data */
	status = api->GetTensorMutableData(output_tensor, (void **) &output_data);
	check_ort_status(api, status, "GetTensorMutableData");

	{
		OrtTensorTypeAndShapeInfo *out_info;

		status = api->GetTensorTypeAndShape(output_tensor, &out_info);
		check_ort_status(api, status, "GetTensorTypeAndShape");

		status = api->GetTensorShapeElementCount(out_info, &output_total);
		check_ort_status(api, status, "GetTensorShapeElementCount");

		api->ReleaseTensorTypeAndShapeInfo(out_info);
	}

	/* Compute per-sample output dimension */
	if (n_inputs > 0)
		output_dim = (int) (output_total / n_inputs);
	else
		output_dim = 0;

	if (output_dim > VECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("model output dimension %d exceeds maximum "
						"vector dimension %d",
						output_dim, VECTOR_MAX_DIM)));

	/* Build result array of vectors */
	result_datums = (Datum *) palloc(sizeof(Datum) * n_inputs);
	for (i = 0; i < n_inputs; i++)
	{
		Vector *v = InitVector(output_dim);

		memcpy(v->x, output_data + (i * output_dim),
			   sizeof(float) * output_dim);
		result_datums[i] = PointerGetDatum(v);
	}

	/* Construct a 1-D PostgreSQL array of vector (OID 6000) */
	result_array = construct_array(result_datums, n_inputs,
								   6000, -1, false, TYPALIGN_INT);

	/* Clean up */
	api->ReleaseValue(input_tensor);
	api->ReleaseValue(output_tensor);
	pfree(batch_data);
	pfree(name);

	PG_RETURN_ARRAYTYPE_P(result_array);
#endif							/* HAVE_ONNXRUNTIME */
}

/* ================================================================
 * alohadb_list_models() RETURNS TABLE(name text, loaded_at timestamptz,
 *                                     input_shape text, output_shape text)
 * ================================================================
 */

/* Context for the multi-call SRF */
typedef struct ListModelsSRFContext
{
	ModelCacheEntry *entries;
	int				max_entries;
	int				current;
} ListModelsSRFContext;

Datum
alohadb_list_models(PG_FUNCTION_ARGS)
{
	FuncCallContext		   *funcctx;
	ListModelsSRFContext   *srfctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldctx;
		TupleDesc		tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor: (name text, loaded_at timestamptz,
		 *                          input_shape text, output_shape text) */
		tupdesc = CreateTemplateTupleDesc(4);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "loaded_at",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "input_shape",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "output_shape",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		srfctx = (ListModelsSRFContext *)
			palloc0(sizeof(ListModelsSRFContext));
		srfctx->entries = model_cache_get_entries(&srfctx->max_entries);
		srfctx->current = 0;

		funcctx->user_fctx = srfctx;
		MemoryContextSwitchTo(oldctx);
	}

	funcctx = SRF_PERCALL_SETUP();
	srfctx = (ListModelsSRFContext *) funcctx->user_fctx;

	/* Scan for the next in-use entry */
	while (srfctx->current < srfctx->max_entries)
	{
		ModelCacheEntry *entry = &srfctx->entries[srfctx->current];

		srfctx->current++;

		if (entry->in_use)
		{
			Datum		values[4];
			bool		nulls[4];
			HeapTuple	tuple;
			Datum		result;

			memset(nulls, 0, sizeof(nulls));

			values[0] = CStringGetTextDatum(entry->name);
			values[1] = TimestampTzGetDatum(entry->loaded_at);
			values[2] = (entry->input_shape != NULL)
				? CStringGetTextDatum(entry->input_shape)
				: CStringGetTextDatum("unknown");
			values[3] = (entry->output_shape != NULL)
				? CStringGetTextDatum(entry->output_shape)
				: CStringGetTextDatum("unknown");

			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);

			SRF_RETURN_NEXT(funcctx, result);
		}
	}

	SRF_RETURN_DONE(funcctx);
}
