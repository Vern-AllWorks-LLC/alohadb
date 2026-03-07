/*-------------------------------------------------------------------------
 *
 * alohadb_inference.h
 *	  Shared declarations for the alohadb_inference extension.
 *
 *	  Provides in-database ML inference using ONNX Runtime.
 *	  When ONNX Runtime is not available at compile time, all UDFs
 *	  raise an informative ERROR.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_inference/alohadb_inference.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_INFERENCE_H
#define ALOHADB_INFERENCE_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

#ifdef HAVE_ONNXRUNTIME
#include <onnxruntime_c_api.h>
#endif

/*
 * Maximum length of a model name.
 */
#define MODEL_NAME_MAXLEN		128

/*
 * Default maximum number of cached models.
 */
#define DEFAULT_MAX_MODELS		10

/*
 * Inference device types.
 */
typedef enum InferenceDevice
{
	INFERENCE_DEVICE_CPU = 0,
	INFERENCE_DEVICE_CUDA
} InferenceDevice;

/*
 * ModelCacheEntry - a single cached ONNX model.
 *
 * Each entry stores the model name, the time it was loaded, shape
 * metadata strings, and (when ONNX Runtime is available) the ORT
 * session and related objects.
 */
typedef struct ModelCacheEntry
{
	char		name[MODEL_NAME_MAXLEN];	/* model identifier */
	TimestampTz loaded_at;				/* when the model was loaded */
	char	   *input_shape;			/* human-readable input shape */
	char	   *output_shape;			/* human-readable output shape */
	bool		in_use;					/* true if this slot is occupied */
	int			lru_counter;			/* for LRU eviction, higher = more recent */

#ifdef HAVE_ONNXRUNTIME
	OrtSession *session;				/* ONNX Runtime session handle */
	OrtSessionOptions *session_options;	/* session options */
	char	   *input_name;				/* first input tensor name */
	char	   *output_name;			/* first output tensor name */
	int			input_ndims;			/* number of input dimensions */
	int64_t	   *input_dims;				/* input dimension sizes */
	int			output_ndims;			/* number of output dimensions */
	int64_t	   *output_dims;			/* output dimension sizes */
#endif
} ModelCacheEntry;

/*
 * ModelCache - the global model cache.
 *
 * Allocated in TopMemoryContext so it persists across transactions.
 * Protected by a spinlock for thread safety.
 */
typedef struct ModelCache
{
	int			max_entries;		/* configurable maximum */
	int			num_entries;		/* current count of loaded models */
	int			lru_clock;			/* monotonic counter for LRU */
	slock_t		lock;				/* spinlock for concurrent access */
	ModelCacheEntry *entries;		/* array of max_entries slots */
} ModelCache;

/* GUC variables (defined in alohadb_inference.c) */
extern int	alohadb_inference_device;
extern int	alohadb_inference_max_models;

/* Model cache functions (defined in model_cache.c) */
extern void model_cache_init(int max_entries);
extern ModelCacheEntry *model_cache_find(const char *name);
extern ModelCacheEntry *model_cache_allocate(const char *name);
extern void model_cache_remove(const char *name);
extern void model_cache_touch(ModelCacheEntry *entry);
extern int	model_cache_count(void);
extern ModelCacheEntry *model_cache_get_entries(int *count);

#endif							/* ALOHADB_INFERENCE_H */
