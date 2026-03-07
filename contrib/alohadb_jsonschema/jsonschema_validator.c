/*-------------------------------------------------------------------------
 *
 * jsonschema_validator.c
 *	  Core JSON Schema Draft-07 validation engine for alohadb_jsonschema.
 *
 *	  Recursively walks the schema and document, checking each keyword.
 *	  Supported keywords: type, properties, required, additionalProperties,
 *	  items, minLength, maxLength, minimum, maximum, exclusiveMinimum,
 *	  exclusiveMaximum, minItems, maxItems, minProperties, maxProperties,
 *	  pattern, enum, const, allOf, anyOf, oneOf, not, $ref.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_jsonschema/jsonschema_validator.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_collation_d.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

#include "alohadb_jsonschema.h"

/* ----------------------------------------------------------------
 * Forward declarations of helper functions
 * ---------------------------------------------------------------- */

static void validate_recursive(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
							   StringInfo path, List **errors,
							   MemoryContext result_mcxt);

static Jsonb *get_jsonb_key(Jsonb *obj, const char *key);
static const char *get_jsonb_string(Jsonb *obj, const char *key);
static double get_jsonb_number(Jsonb *obj, const char *key, bool *found);
static bool jsonb_value_is_type(Jsonb *doc, const char *type_name);
static int	jschema_array_len(Jsonb *arr);
static int	jschema_object_len(Jsonb *obj);
static Jsonb *resolve_ref(Jsonb *root_schema, const char *ref);
static Jsonb *jsonb_value_to_jsonb(JsonbValue *val);
static void add_error(List **errors, MemoryContext result_mcxt,
					  const char *path, const char *fmt,...) pg_attribute_printf(4, 5);
static int	numeric_cmp_double(Numeric num, double val);
static bool numeric_is_integer(Numeric num);
static char *get_doc_type_name(Jsonb *doc);

/* ----------------------------------------------------------------
 * add_error
 *
 * Allocate an error in result_mcxt and append to the errors list.
 * ---------------------------------------------------------------- */
static void
add_error(List **errors, MemoryContext result_mcxt,
		  const char *path, const char *fmt,...)
{
	MemoryContext oldcxt;
	JsonSchemaError *err;
	va_list		args;
	char		buf[1024];

	va_start(args, fmt);
	vsnprintf(buf, sizeof(buf), fmt, args);
	va_end(args);

	oldcxt = MemoryContextSwitchTo(result_mcxt);

	err = palloc(sizeof(JsonSchemaError));
	err->valid = false;
	err->error_path = pstrdup(path);
	err->error_message = pstrdup(buf);

	*errors = lappend(*errors, err);

	MemoryContextSwitchTo(oldcxt);
}

/* ----------------------------------------------------------------
 * jsonb_value_to_jsonb
 *
 * Convert a JsonbValue to a palloc'd Jsonb.  Handles both scalar
 * and binary (container) values.
 * ---------------------------------------------------------------- */
static Jsonb *
jsonb_value_to_jsonb(JsonbValue *val)
{
	if (val == NULL)
		return NULL;

	if (val->type == jbvBinary)
	{
		/*
		 * Construct a proper Jsonb with varlena header from the raw
		 * container data.
		 */
		Jsonb	   *result;
		int			len = val->val.binary.len;

		result = (Jsonb *) palloc(VARHDRSZ + len);
		SET_VARSIZE(result, VARHDRSZ + len);
		memcpy(&result->root, val->val.binary.data, len);
		return result;
	}

	/* Scalar or deep value -- use the standard conversion */
	return JsonbValueToJsonb(val);
}

/* ----------------------------------------------------------------
 * get_jsonb_key
 *
 * Look up a key in a JSONB object and return the value as a new Jsonb.
 * Returns NULL if not found or if input is not an object.
 * ---------------------------------------------------------------- */
static Jsonb *
get_jsonb_key(Jsonb *obj, const char *key)
{
	JsonbValue	kval;
	JsonbValue *result;

	if (obj == NULL || !JB_ROOT_IS_OBJECT(obj))
		return NULL;

	kval.type = jbvString;
	kval.val.string.val = unconstify(char *, key);
	kval.val.string.len = strlen(key);

	result = findJsonbValueFromContainer(&obj->root, JB_FOBJECT, &kval);
	if (result == NULL)
		return NULL;

	return jsonb_value_to_jsonb(result);
}

/* ----------------------------------------------------------------
 * get_jsonb_string
 *
 * Extract a string value for a given key from a JSONB object.
 * Returns NULL if the key is missing or the value is not a string.
 * The returned pointer is palloc'd.
 * ---------------------------------------------------------------- */
static const char *
get_jsonb_string(Jsonb *obj, const char *key)
{
	JsonbValue	kval;
	JsonbValue *result;
	char	   *str;

	if (obj == NULL || !JB_ROOT_IS_OBJECT(obj))
		return NULL;

	kval.type = jbvString;
	kval.val.string.val = unconstify(char *, key);
	kval.val.string.len = strlen(key);

	result = findJsonbValueFromContainer(&obj->root, JB_FOBJECT, &kval);
	if (result == NULL || result->type != jbvString)
		return NULL;

	str = palloc(result->val.string.len + 1);
	memcpy(str, result->val.string.val, result->val.string.len);
	str[result->val.string.len] = '\0';
	return str;
}

/* ----------------------------------------------------------------
 * get_jsonb_number
 *
 * Extract a numeric value for a given key, converting to double.
 * Sets *found to true if a numeric value was found.
 * ---------------------------------------------------------------- */
static double
get_jsonb_number(Jsonb *obj, const char *key, bool *found)
{
	JsonbValue	kval;
	JsonbValue *result;
	Datum		d;

	*found = false;

	if (obj == NULL || !JB_ROOT_IS_OBJECT(obj))
		return 0.0;

	kval.type = jbvString;
	kval.val.string.val = unconstify(char *, key);
	kval.val.string.len = strlen(key);

	result = findJsonbValueFromContainer(&obj->root, JB_FOBJECT, &kval);
	if (result == NULL || result->type != jbvNumeric)
		return 0.0;

	*found = true;
	d = DirectFunctionCall1(numeric_float8_no_overflow,
							NumericGetDatum(result->val.numeric));
	return DatumGetFloat8(d);
}

/* ----------------------------------------------------------------
 * numeric_cmp_double
 *
 * Compare a Numeric to a double.  Returns <0, 0, or >0.
 * ---------------------------------------------------------------- */
static int
numeric_cmp_double(Numeric num, double val)
{
	Datum		d;
	double		numval;

	d = DirectFunctionCall1(numeric_float8_no_overflow,
							NumericGetDatum(num));
	numval = DatumGetFloat8(d);

	if (numval < val)
		return -1;
	if (numval > val)
		return 1;
	return 0;
}

/* ----------------------------------------------------------------
 * numeric_is_integer
 *
 * Check whether a Numeric value has no fractional part by
 * truncating to zero decimal places and comparing.
 * ---------------------------------------------------------------- */
static bool
numeric_is_integer(Numeric num)
{
	Numeric		truncated;
	Datum		d;

	if (numeric_is_nan(num) || numeric_is_inf(num))
		return false;

	d = DirectFunctionCall2(numeric_trunc,
							NumericGetDatum(num),
							Int32GetDatum(0));
	truncated = DatumGetNumeric(d);

	/* Compare original with truncated */
	d = DirectFunctionCall2(numeric_cmp,
							NumericGetDatum(num),
							NumericGetDatum(truncated));
	return (DatumGetInt32(d) == 0);
}

/* ----------------------------------------------------------------
 * get_doc_type_name
 *
 * Return the JSON Schema type name for a JSONB document value.
 * ---------------------------------------------------------------- */
static char *
get_doc_type_name(Jsonb *doc)
{
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;

	if (doc == NULL)
		return "null";

	/* Check for scalar wrapper first */
	if (JB_ROOT_IS_SCALAR(doc))
	{
		it = JsonbIteratorInit(&doc->root);
		tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */
		Assert(tok == WJB_BEGIN_ARRAY);
		tok = JsonbIteratorNext(&it, &val, true);	/* WJB_ELEM */
		Assert(tok == WJB_ELEM);

		switch (val.type)
		{
			case jbvNull:
				return "null";
			case jbvString:
				return "string";
			case jbvNumeric:
				return "number";
			case jbvBool:
				return "boolean";
			default:
				return "unknown";
		}
	}

	if (JB_ROOT_IS_OBJECT(doc))
		return "object";
	if (JB_ROOT_IS_ARRAY(doc))
		return "array";

	return "unknown";
}

/* ----------------------------------------------------------------
 * jsonb_value_is_type
 *
 * Check whether a JSONB document matches a JSON Schema type name.
 * ---------------------------------------------------------------- */
static bool
jsonb_value_is_type(Jsonb *doc, const char *type_name)
{
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;

	if (doc == NULL)
		return (strcmp(type_name, "null") == 0);

	/* Handle scalar values wrapped in an array */
	if (JB_ROOT_IS_SCALAR(doc))
	{
		it = JsonbIteratorInit(&doc->root);
		tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */
		Assert(tok == WJB_BEGIN_ARRAY);
		tok = JsonbIteratorNext(&it, &val, true);	/* WJB_ELEM */
		Assert(tok == WJB_ELEM);

		if (strcmp(type_name, "null") == 0)
			return (val.type == jbvNull);
		if (strcmp(type_name, "string") == 0)
			return (val.type == jbvString);
		if (strcmp(type_name, "boolean") == 0)
			return (val.type == jbvBool);
		if (strcmp(type_name, "number") == 0)
			return (val.type == jbvNumeric);
		if (strcmp(type_name, "integer") == 0)
			return (val.type == jbvNumeric &&
					numeric_is_integer(val.val.numeric));

		/* scalar can't be object or array */
		return false;
	}

	if (strcmp(type_name, "object") == 0)
		return JB_ROOT_IS_OBJECT(doc);
	if (strcmp(type_name, "array") == 0)
		return JB_ROOT_IS_ARRAY(doc);

	return false;
}

/* ----------------------------------------------------------------
 * jschema_array_len
 *
 * Count the number of elements in a JSONB array.  Returns 0 if
 * the input is not an array.
 * ---------------------------------------------------------------- */
static int
jschema_array_len(Jsonb *arr)
{
	if (arr == NULL)
		return 0;

	if (JB_ROOT_IS_ARRAY(arr) && !JB_ROOT_IS_SCALAR(arr))
		return JB_ROOT_COUNT(arr);

	return 0;
}

/* ----------------------------------------------------------------
 * jschema_object_len
 *
 * Count the number of key/value pairs in a JSONB object.
 * Returns 0 if the input is not an object.
 * ---------------------------------------------------------------- */
static int
jschema_object_len(Jsonb *obj)
{
	if (obj == NULL)
		return 0;

	if (JB_ROOT_IS_OBJECT(obj))
		return JB_ROOT_COUNT(obj);

	return 0;
}

/* ----------------------------------------------------------------
 * get_scalar_numeric
 *
 * If doc is a scalar numeric, extract and return its Numeric value.
 * Returns NULL otherwise.
 * ---------------------------------------------------------------- */
static Numeric
get_scalar_numeric(Jsonb *doc)
{
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;

	if (doc == NULL || !JB_ROOT_IS_SCALAR(doc))
		return NULL;

	it = JsonbIteratorInit(&doc->root);
	tok = JsonbIteratorNext(&it, &val, true);
	Assert(tok == WJB_BEGIN_ARRAY);
	tok = JsonbIteratorNext(&it, &val, true);
	Assert(tok == WJB_ELEM);

	if (val.type == jbvNumeric)
		return val.val.numeric;

	return NULL;
}

/* ----------------------------------------------------------------
 * get_scalar_string
 *
 * If doc is a scalar string, extract the string value.
 * Writes the length to *len.  Returns NULL otherwise.
 * ---------------------------------------------------------------- */
static const char *
get_scalar_string(Jsonb *doc, int *len)
{
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;

	if (doc == NULL || !JB_ROOT_IS_SCALAR(doc))
		return NULL;

	it = JsonbIteratorInit(&doc->root);
	tok = JsonbIteratorNext(&it, &val, true);
	Assert(tok == WJB_BEGIN_ARRAY);
	tok = JsonbIteratorNext(&it, &val, true);
	Assert(tok == WJB_ELEM);

	if (val.type == jbvString)
	{
		*len = val.val.string.len;
		return val.val.string.val;
	}

	return NULL;
}

/* ----------------------------------------------------------------
 * resolve_ref
 *
 * Resolve a $ref JSON Pointer.  Only supports internal references
 * of the form "#/path/to/definition".  Returns a Jsonb sub-schema
 * or NULL if the reference cannot be resolved.
 * ---------------------------------------------------------------- */
static Jsonb *
resolve_ref(Jsonb *root_schema, const char *ref)
{
	Jsonb	   *current;
	char	   *refcopy;
	char	   *token;
	char	   *saveptr;

	if (root_schema == NULL || ref == NULL)
		return NULL;

	/* Only support internal references starting with # */
	if (ref[0] != '#')
		return NULL;

	/* "#" alone means the root schema itself */
	if (ref[1] == '\0')
		return root_schema;

	/* Must be "#/" to be a valid JSON Pointer */
	if (ref[1] != '/')
		return NULL;

	current = root_schema;
	refcopy = pstrdup(ref + 2);		/* skip "#/" */

	for (token = strtok_r(refcopy, "/", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, "/", &saveptr))
	{
		/*
		 * Decode JSON Pointer escapes: ~1 -> /, ~0 -> ~
		 * We handle these in order: first ~1 then ~0.
		 */
		StringInfoData decoded;
		char	   *p;

		initStringInfo(&decoded);
		for (p = token; *p != '\0'; p++)
		{
			if (*p == '~' && *(p + 1) == '1')
			{
				appendStringInfoChar(&decoded, '/');
				p++;
			}
			else if (*p == '~' && *(p + 1) == '0')
			{
				appendStringInfoChar(&decoded, '~');
				p++;
			}
			else
			{
				appendStringInfoChar(&decoded, *p);
			}
		}

		current = get_jsonb_key(current, decoded.data);
		pfree(decoded.data);

		if (current == NULL)
		{
			pfree(refcopy);
			return NULL;
		}
	}

	pfree(refcopy);
	return current;
}

/* ----------------------------------------------------------------
 * validate_type
 *
 * Check the "type" keyword.  Supports both a single type string
 * and an array of type strings.
 * ---------------------------------------------------------------- */
static void
validate_type(Jsonb *doc, Jsonb *schema, StringInfo path,
			  List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *type_val;

	type_val = get_jsonb_key(schema, "type");
	if (type_val == NULL)
		return;

	if (JB_ROOT_IS_SCALAR(type_val))
	{
		/* Single type string */
		const char *type_str = get_jsonb_string(schema, "type");

		if (type_str != NULL && !jsonb_value_is_type(doc, type_str))
		{
			add_error(errors, result_mcxt, path->data,
					  "type mismatch: expected %s, got %s",
					  type_str, get_doc_type_name(doc));
		}
	}
	else if (JB_ROOT_IS_ARRAY(type_val))
	{
		/* Array of type strings -- doc must match at least one */
		JsonbIterator *it;
		JsonbIteratorToken tok;
		JsonbValue	val;
		bool		matched = false;

		it = JsonbIteratorInit(&type_val->root);
		tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

		while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
		{
			if (tok == WJB_ELEM && val.type == jbvString)
			{
				char	   *ts = palloc(val.val.string.len + 1);

				memcpy(ts, val.val.string.val, val.val.string.len);
				ts[val.val.string.len] = '\0';

				if (jsonb_value_is_type(doc, ts))
				{
					matched = true;
					pfree(ts);
					break;
				}
				pfree(ts);
			}
		}

		if (!matched)
		{
			add_error(errors, result_mcxt, path->data,
					  "type mismatch: value does not match any of the allowed types");
		}
	}
}

/* ----------------------------------------------------------------
 * validate_required
 *
 * Check the "required" keyword -- an array of property names
 * that must be present in the document object.
 * ---------------------------------------------------------------- */
static void
validate_required(Jsonb *doc, Jsonb *schema, StringInfo path,
				  List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *required;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;

	required = get_jsonb_key(schema, "required");
	if (required == NULL || !JB_ROOT_IS_ARRAY(required))
		return;

	if (!JB_ROOT_IS_OBJECT(doc))
		return;

	it = JsonbIteratorInit(&required->root);
	tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

	while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
	{
		if (tok == WJB_ELEM && val.type == jbvString)
		{
			JsonbValue *found;

			found = findJsonbValueFromContainer(&doc->root, JB_FOBJECT, &val);
			if (found == NULL)
			{
				char	   *name = palloc(val.val.string.len + 1);

				memcpy(name, val.val.string.val, val.val.string.len);
				name[val.val.string.len] = '\0';
				add_error(errors, result_mcxt, path->data,
						  "required property '%s' is missing", name);
				pfree(name);
			}
		}
	}
}

/* ----------------------------------------------------------------
 * validate_properties
 *
 * Check "properties" keyword -- for each property defined in the
 * schema, if it exists in the document, recursively validate it.
 * ---------------------------------------------------------------- */
static void
validate_properties(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
					StringInfo path, List **errors,
					MemoryContext result_mcxt)
{
	Jsonb	   *properties;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	key;
	JsonbValue	val;

	properties = get_jsonb_key(schema, "properties");
	if (properties == NULL || !JB_ROOT_IS_OBJECT(properties))
		return;

	if (!JB_ROOT_IS_OBJECT(doc))
		return;

	it = JsonbIteratorInit(&properties->root);
	tok = JsonbIteratorNext(&it, &key, true);	/* WJB_BEGIN_OBJECT */

	while ((tok = JsonbIteratorNext(&it, &key, true)) != WJB_END_OBJECT)
	{
		if (tok == WJB_KEY)
		{
			char	   *prop_name;
			Jsonb	   *prop_schema;
			Jsonb	   *prop_value;
			int			saved_len;

			prop_name = palloc(key.val.string.len + 1);
			memcpy(prop_name, key.val.string.val, key.val.string.len);
			prop_name[key.val.string.len] = '\0';

			/* Advance to the value */
			tok = JsonbIteratorNext(&it, &val, true);
			Assert(tok == WJB_VALUE || tok == WJB_ELEM);

			prop_schema = jsonb_value_to_jsonb(&val);
			prop_value = get_jsonb_key(doc, prop_name);

			if (prop_value != NULL && prop_schema != NULL)
			{
				saved_len = path->len;
				appendStringInfo(path, ".%s", prop_name);

				validate_recursive(prop_value, prop_schema, root_schema,
								   path, errors, result_mcxt);

				/* Restore path */
				path->data[saved_len] = '\0';
				path->len = saved_len;
			}

			pfree(prop_name);
		}
	}
}

/* ----------------------------------------------------------------
 * validate_additional_properties
 *
 * Check "additionalProperties" keyword.  If false, no extra keys
 * are allowed beyond what is listed in "properties".  If a schema
 * object, extra keys must validate against it.
 * ---------------------------------------------------------------- */
static void
validate_additional_properties(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
							   StringInfo path, List **errors,
							   MemoryContext result_mcxt)
{
	Jsonb	   *additional;
	Jsonb	   *properties;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	key;
	JsonbValue	val;
	bool		additional_is_false = false;
	Jsonb	   *additional_schema = NULL;

	additional = get_jsonb_key(schema, "additionalProperties");
	if (additional == NULL)
		return;

	if (!JB_ROOT_IS_OBJECT(doc))
		return;

	/* Determine if additionalProperties is false or a schema */
	if (JB_ROOT_IS_SCALAR(additional))
	{
		JsonbIterator *ait;
		JsonbValue	aval;

		ait = JsonbIteratorInit(&additional->root);
		(void) JsonbIteratorNext(&ait, &aval, true);	/* WJB_BEGIN_ARRAY */
		(void) JsonbIteratorNext(&ait, &aval, true);	/* WJB_ELEM */

		if (aval.type == jbvBool && !aval.val.boolean)
			additional_is_false = true;
		else if (aval.type == jbvBool && aval.val.boolean)
			return;					/* true means anything is allowed */
	}
	else if (JB_ROOT_IS_OBJECT(additional))
	{
		additional_schema = additional;
	}
	else
	{
		return;
	}

	properties = get_jsonb_key(schema, "properties");

	/* Iterate document keys, check each one */
	it = JsonbIteratorInit(&doc->root);
	tok = JsonbIteratorNext(&it, &key, true);	/* WJB_BEGIN_OBJECT */

	while ((tok = JsonbIteratorNext(&it, &key, true)) != WJB_END_OBJECT)
	{
		if (tok == WJB_KEY)
		{
			bool		in_properties = false;
			char	   *kname;
			int			saved_len;

			/* Skip the value */
			tok = JsonbIteratorNext(&it, &val, true);

			/* Check if this key is defined in "properties" */
			if (properties != NULL && JB_ROOT_IS_OBJECT(properties))
			{
				JsonbValue *found;

				found = findJsonbValueFromContainer(&properties->root,
													JB_FOBJECT, &key);
				if (found != NULL)
					in_properties = true;
			}

			if (in_properties)
				continue;

			kname = palloc(key.val.string.len + 1);
			memcpy(kname, key.val.string.val, key.val.string.len);
			kname[key.val.string.len] = '\0';

			if (additional_is_false)
			{
				add_error(errors, result_mcxt, path->data,
						  "additional property '%s' is not allowed",
						  kname);
			}
			else if (additional_schema != NULL)
			{
				Jsonb	   *prop_value;

				prop_value = get_jsonb_key(doc, kname);
				if (prop_value != NULL)
				{
					saved_len = path->len;
					appendStringInfo(path, ".%s", kname);

					validate_recursive(prop_value, additional_schema,
									   root_schema, path, errors,
									   result_mcxt);

					path->data[saved_len] = '\0';
					path->len = saved_len;
				}
			}

			pfree(kname);
		}
	}
}

/* ----------------------------------------------------------------
 * validate_items
 *
 * Check "items" keyword -- validate each element in a doc array
 * against the items sub-schema.
 * ---------------------------------------------------------------- */
static void
validate_items(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
			   StringInfo path, List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *items_schema;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;
	int			idx = 0;

	items_schema = get_jsonb_key(schema, "items");
	if (items_schema == NULL)
		return;

	if (!JB_ROOT_IS_ARRAY(doc) || JB_ROOT_IS_SCALAR(doc))
		return;

	it = JsonbIteratorInit(&doc->root);
	tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

	while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
	{
		if (tok == WJB_ELEM)
		{
			Jsonb	   *elem;
			int			saved_len;

			elem = jsonb_value_to_jsonb(&val);

			saved_len = path->len;
			appendStringInfo(path, "[%d]", idx);

			validate_recursive(elem, items_schema, root_schema,
							   path, errors, result_mcxt);

			path->data[saved_len] = '\0';
			path->len = saved_len;

			idx++;
		}
	}
}

/* ----------------------------------------------------------------
 * validate_string_constraints
 *
 * Check minLength, maxLength, and pattern for string values.
 * ---------------------------------------------------------------- */
static void
validate_string_constraints(Jsonb *doc, Jsonb *schema, StringInfo path,
							List **errors, MemoryContext result_mcxt)
{
	int			slen;
	const char *str;
	bool		found;
	double		constraint_val;
	const char *pattern_str;
	int			char_len;

	str = get_scalar_string(doc, &slen);
	if (str == NULL)
		return;

	/* Compute character length (not byte length) for multibyte encodings */
	char_len = pg_mbstrlen_with_len(str, slen);

	/* minLength */
	constraint_val = get_jsonb_number(schema, "minLength", &found);
	if (found && char_len < (int) constraint_val)
	{
		add_error(errors, result_mcxt, path->data,
				  "string length %d is less than minLength %d",
				  char_len, (int) constraint_val);
	}

	/* maxLength */
	constraint_val = get_jsonb_number(schema, "maxLength", &found);
	if (found && char_len > (int) constraint_val)
	{
		add_error(errors, result_mcxt, path->data,
				  "string length %d exceeds maxLength %d",
				  char_len, (int) constraint_val);
	}

	/* pattern */
	pattern_str = get_jsonb_string(schema, "pattern");
	if (pattern_str != NULL)
	{
		text	   *pattern_text;
		bool		match;

		pattern_text = cstring_to_text(pattern_str);

		match = RE_compile_and_execute(pattern_text,
									   unconstify(char *, str), slen,
									   REG_ADVANCED, C_COLLATION_OID,
									   0, NULL);
		if (!match)
		{
			add_error(errors, result_mcxt, path->data,
					  "string does not match pattern '%s'", pattern_str);
		}

		pfree(pattern_text);
	}
}

/* ----------------------------------------------------------------
 * validate_numeric_constraints
 *
 * Check minimum, maximum, exclusiveMinimum, exclusiveMaximum.
 * ---------------------------------------------------------------- */
static void
validate_numeric_constraints(Jsonb *doc, Jsonb *schema, StringInfo path,
							 List **errors, MemoryContext result_mcxt)
{
	Numeric		num;
	bool		found;
	double		constraint_val;
	int			cmp;

	num = get_scalar_numeric(doc);
	if (num == NULL)
		return;

	/* minimum */
	constraint_val = get_jsonb_number(schema, "minimum", &found);
	if (found)
	{
		cmp = numeric_cmp_double(num, constraint_val);
		if (cmp < 0)
		{
			add_error(errors, result_mcxt, path->data,
					  "value is less than minimum %g", constraint_val);
		}
	}

	/* maximum */
	constraint_val = get_jsonb_number(schema, "maximum", &found);
	if (found)
	{
		cmp = numeric_cmp_double(num, constraint_val);
		if (cmp > 0)
		{
			add_error(errors, result_mcxt, path->data,
					  "value exceeds maximum %g", constraint_val);
		}
	}

	/* exclusiveMinimum */
	constraint_val = get_jsonb_number(schema, "exclusiveMinimum", &found);
	if (found)
	{
		cmp = numeric_cmp_double(num, constraint_val);
		if (cmp <= 0)
		{
			add_error(errors, result_mcxt, path->data,
					  "value is not greater than exclusiveMinimum %g",
					  constraint_val);
		}
	}

	/* exclusiveMaximum */
	constraint_val = get_jsonb_number(schema, "exclusiveMaximum", &found);
	if (found)
	{
		cmp = numeric_cmp_double(num, constraint_val);
		if (cmp >= 0)
		{
			add_error(errors, result_mcxt, path->data,
					  "value is not less than exclusiveMaximum %g",
					  constraint_val);
		}
	}
}

/* ----------------------------------------------------------------
 * validate_array_constraints
 *
 * Check minItems and maxItems.
 * ---------------------------------------------------------------- */
static void
validate_array_constraints(Jsonb *doc, Jsonb *schema, StringInfo path,
						   List **errors, MemoryContext result_mcxt)
{
	int			arr_len;
	bool		found;
	double		constraint_val;

	if (!JB_ROOT_IS_ARRAY(doc) || JB_ROOT_IS_SCALAR(doc))
		return;

	arr_len = jschema_array_len(doc);

	/* minItems */
	constraint_val = get_jsonb_number(schema, "minItems", &found);
	if (found && arr_len < (int) constraint_val)
	{
		add_error(errors, result_mcxt, path->data,
				  "array has %d items, less than minItems %d",
				  arr_len, (int) constraint_val);
	}

	/* maxItems */
	constraint_val = get_jsonb_number(schema, "maxItems", &found);
	if (found && arr_len > (int) constraint_val)
	{
		add_error(errors, result_mcxt, path->data,
				  "array has %d items, exceeds maxItems %d",
				  arr_len, (int) constraint_val);
	}
}

/* ----------------------------------------------------------------
 * validate_object_constraints
 *
 * Check minProperties and maxProperties.
 * ---------------------------------------------------------------- */
static void
validate_object_constraints(Jsonb *doc, Jsonb *schema, StringInfo path,
							List **errors, MemoryContext result_mcxt)
{
	int			obj_len;
	bool		found;
	double		constraint_val;

	if (!JB_ROOT_IS_OBJECT(doc))
		return;

	obj_len = jschema_object_len(doc);

	/* minProperties */
	constraint_val = get_jsonb_number(schema, "minProperties", &found);
	if (found && obj_len < (int) constraint_val)
	{
		add_error(errors, result_mcxt, path->data,
				  "object has %d properties, less than minProperties %d",
				  obj_len, (int) constraint_val);
	}

	/* maxProperties */
	constraint_val = get_jsonb_number(schema, "maxProperties", &found);
	if (found && obj_len > (int) constraint_val)
	{
		add_error(errors, result_mcxt, path->data,
				  "object has %d properties, exceeds maxProperties %d",
				  obj_len, (int) constraint_val);
	}
}

/* ----------------------------------------------------------------
 * validate_enum
 *
 * Check "enum" keyword -- an array of allowed values.
 * The document must match at least one value.
 * ---------------------------------------------------------------- */
static void
validate_enum(Jsonb *doc, Jsonb *schema, StringInfo path,
			  List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *enum_arr;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;
	bool		matched = false;

	enum_arr = get_jsonb_key(schema, "enum");
	if (enum_arr == NULL || !JB_ROOT_IS_ARRAY(enum_arr))
		return;

	it = JsonbIteratorInit(&enum_arr->root);
	tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

	while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
	{
		if (tok == WJB_ELEM)
		{
			Jsonb	   *enum_val;

			enum_val = jsonb_value_to_jsonb(&val);

			if (compareJsonbContainers(&doc->root, &enum_val->root) == 0)
			{
				matched = true;
				break;
			}
		}
	}

	if (!matched)
	{
		add_error(errors, result_mcxt, path->data,
				  "value does not match any value in enum");
	}
}

/* ----------------------------------------------------------------
 * validate_const
 *
 * Check "const" keyword -- the document must match the constant value.
 * ---------------------------------------------------------------- */
static void
validate_const(Jsonb *doc, Jsonb *schema, StringInfo path,
			   List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *const_val;

	const_val = get_jsonb_key(schema, "const");
	if (const_val == NULL)
		return;

	if (compareJsonbContainers(&doc->root, &const_val->root) != 0)
	{
		add_error(errors, result_mcxt, path->data,
				  "value does not match the required constant");
	}
}

/* ----------------------------------------------------------------
 * validate_allof
 *
 * Check "allOf" keyword -- all sub-schemas must match.
 * ---------------------------------------------------------------- */
static void
validate_allof(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
			   StringInfo path, List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *allof;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;
	int			idx = 0;

	allof = get_jsonb_key(schema, "allOf");
	if (allof == NULL || !JB_ROOT_IS_ARRAY(allof))
		return;

	it = JsonbIteratorInit(&allof->root);
	tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

	while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
	{
		if (tok == WJB_ELEM)
		{
			Jsonb	   *sub_schema;

			sub_schema = jsonb_value_to_jsonb(&val);
			if (sub_schema != NULL)
			{
				validate_recursive(doc, sub_schema, root_schema,
								   path, errors, result_mcxt);
			}
			idx++;
		}
	}
}

/* ----------------------------------------------------------------
 * validate_anyof
 *
 * Check "anyOf" keyword -- at least one sub-schema must match.
 * ---------------------------------------------------------------- */
static void
validate_anyof(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
			   StringInfo path, List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *anyof;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;
	bool		any_matched = false;
	int			idx = 0;

	anyof = get_jsonb_key(schema, "anyOf");
	if (anyof == NULL || !JB_ROOT_IS_ARRAY(anyof))
		return;

	it = JsonbIteratorInit(&anyof->root);
	tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

	while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
	{
		if (tok == WJB_ELEM)
		{
			Jsonb	   *sub_schema;
			List	   *sub_errors = NIL;

			sub_schema = jsonb_value_to_jsonb(&val);
			if (sub_schema != NULL)
			{
				validate_recursive(doc, sub_schema, root_schema,
								   path, &sub_errors, result_mcxt);

				if (sub_errors == NIL)
				{
					any_matched = true;
					break;
				}
				/* Discard errors from non-matching sub-schemas */
				list_free_deep(sub_errors);
			}
			idx++;
		}
	}

	if (!any_matched)
	{
		add_error(errors, result_mcxt, path->data,
				  "value does not match any schema in anyOf");
	}
}

/* ----------------------------------------------------------------
 * validate_oneof
 *
 * Check "oneOf" keyword -- exactly one sub-schema must match.
 * ---------------------------------------------------------------- */
static void
validate_oneof(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
			   StringInfo path, List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *oneof;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;
	int			match_count = 0;
	int			idx = 0;

	oneof = get_jsonb_key(schema, "oneOf");
	if (oneof == NULL || !JB_ROOT_IS_ARRAY(oneof))
		return;

	it = JsonbIteratorInit(&oneof->root);
	tok = JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */

	while ((tok = JsonbIteratorNext(&it, &val, true)) != WJB_END_ARRAY)
	{
		if (tok == WJB_ELEM)
		{
			Jsonb	   *sub_schema;
			List	   *sub_errors = NIL;

			sub_schema = jsonb_value_to_jsonb(&val);
			if (sub_schema != NULL)
			{
				validate_recursive(doc, sub_schema, root_schema,
								   path, &sub_errors, result_mcxt);

				if (sub_errors == NIL)
					match_count++;

				/* Discard sub-errors */
				list_free_deep(sub_errors);
			}
			idx++;
		}
	}

	if (match_count == 0)
	{
		add_error(errors, result_mcxt, path->data,
				  "value does not match any schema in oneOf");
	}
	else if (match_count > 1)
	{
		add_error(errors, result_mcxt, path->data,
				  "value matches %d schemas in oneOf, but must match exactly one",
				  match_count);
	}
}

/* ----------------------------------------------------------------
 * validate_not
 *
 * Check "not" keyword -- the sub-schema must NOT match.
 * ---------------------------------------------------------------- */
static void
validate_not(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
			 StringInfo path, List **errors, MemoryContext result_mcxt)
{
	Jsonb	   *not_schema;
	List	   *sub_errors = NIL;

	not_schema = get_jsonb_key(schema, "not");
	if (not_schema == NULL)
		return;

	validate_recursive(doc, not_schema, root_schema,
					   path, &sub_errors, result_mcxt);

	if (sub_errors == NIL)
	{
		/* The sub-schema matched, which means "not" fails */
		add_error(errors, result_mcxt, path->data,
				  "value must not match the schema in 'not'");
	}
	else
	{
		/* Sub-schema did not match, which is what we want */
		list_free_deep(sub_errors);
	}
}

/* ----------------------------------------------------------------
 * validate_ref
 *
 * Check "$ref" keyword -- resolve the reference and validate
 * against the referenced sub-schema.
 * ---------------------------------------------------------------- */
static void
validate_ref(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
			 StringInfo path, List **errors, MemoryContext result_mcxt)
{
	const char *ref_str;
	Jsonb	   *ref_schema;

	ref_str = get_jsonb_string(schema, "$ref");
	if (ref_str == NULL)
		return;

	ref_schema = resolve_ref(root_schema, ref_str);
	if (ref_schema == NULL)
	{
		add_error(errors, result_mcxt, path->data,
				  "cannot resolve $ref '%s'", ref_str);
		return;
	}

	validate_recursive(doc, ref_schema, root_schema,
					   path, errors, result_mcxt);
}

/* ----------------------------------------------------------------
 * validate_recursive
 *
 * Main recursive validation dispatcher.  Checks all applicable
 * keywords in the schema against the document.
 * ---------------------------------------------------------------- */
static void
validate_recursive(Jsonb *doc, Jsonb *schema, Jsonb *root_schema,
				   StringInfo path, List **errors,
				   MemoryContext result_mcxt)
{
	/* A boolean true schema accepts anything; false rejects everything */
	if (JB_ROOT_IS_SCALAR(schema))
	{
		JsonbIterator *it;
		JsonbValue	val;

		it = JsonbIteratorInit(&schema->root);
		(void) JsonbIteratorNext(&it, &val, true);	/* WJB_BEGIN_ARRAY */
		(void) JsonbIteratorNext(&it, &val, true);	/* WJB_ELEM */

		if (val.type == jbvBool)
		{
			if (!val.val.boolean)
			{
				add_error(errors, result_mcxt, path->data,
						  "value is rejected by a false schema");
			}
			return;
		}

		/* A scalar schema that is not a boolean is invalid, skip */
		return;
	}

	if (!JB_ROOT_IS_OBJECT(schema))
		return;

	/* Handle $ref first -- it takes precedence in Draft-07 */
	if (get_jsonb_key(schema, "$ref") != NULL)
	{
		validate_ref(doc, schema, root_schema, path, errors, result_mcxt);
		return;
	}

	/* type */
	validate_type(doc, schema, path, errors, result_mcxt);

	/* required */
	validate_required(doc, schema, path, errors, result_mcxt);

	/* properties */
	validate_properties(doc, schema, root_schema, path, errors, result_mcxt);

	/* additionalProperties */
	validate_additional_properties(doc, schema, root_schema, path, errors,
								  result_mcxt);

	/* items */
	validate_items(doc, schema, root_schema, path, errors, result_mcxt);

	/* String constraints */
	validate_string_constraints(doc, schema, path, errors, result_mcxt);

	/* Numeric constraints */
	validate_numeric_constraints(doc, schema, path, errors, result_mcxt);

	/* Array constraints */
	validate_array_constraints(doc, schema, path, errors, result_mcxt);

	/* Object constraints */
	validate_object_constraints(doc, schema, path, errors, result_mcxt);

	/* enum */
	validate_enum(doc, schema, path, errors, result_mcxt);

	/* const */
	validate_const(doc, schema, path, errors, result_mcxt);

	/* allOf */
	validate_allof(doc, schema, root_schema, path, errors, result_mcxt);

	/* anyOf */
	validate_anyof(doc, schema, root_schema, path, errors, result_mcxt);

	/* oneOf */
	validate_oneof(doc, schema, root_schema, path, errors, result_mcxt);

	/* not */
	validate_not(doc, schema, root_schema, path, errors, result_mcxt);
}

/* ----------------------------------------------------------------
 * jsonschema_validate_internal
 *
 * Public entry point.  Creates a temporary memory context for the
 * validation work, runs the recursive validator, and returns the
 * result with errors allocated in result_mcxt.
 * ---------------------------------------------------------------- */
JsonSchemaResult *
jsonschema_validate_internal(Jsonb *doc, Jsonb *schema,
							 MemoryContext result_mcxt)
{
	MemoryContext work_mcxt;
	MemoryContext oldcxt;
	JsonSchemaResult *result;
	List	   *errors = NIL;
	StringInfoData path;

	/* Create a temporary context for all intermediate allocations */
	work_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									  "jsonschema validation",
									  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(work_mcxt);

	initStringInfo(&path);
	appendStringInfoChar(&path, '$');

	/* Run the recursive validation */
	validate_recursive(doc, schema, schema, &path, &errors, result_mcxt);

	MemoryContextSwitchTo(oldcxt);

	/* Build the result in the result memory context */
	oldcxt = MemoryContextSwitchTo(result_mcxt);

	result = palloc(sizeof(JsonSchemaResult));
	result->errors = errors;
	result->valid = (errors == NIL);

	MemoryContextSwitchTo(oldcxt);

	/* Clean up the working context */
	MemoryContextDelete(work_mcxt);

	return result;
}
