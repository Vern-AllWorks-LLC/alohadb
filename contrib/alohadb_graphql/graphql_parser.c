/*-------------------------------------------------------------------------
 *
 * graphql_parser.c
 *	  Recursive-descent parser for a simplified subset of GraphQL.
 *
 *	  Supported constructs:
 *	  - Anonymous queries:   { table { field1 field2 } }
 *	  - Named queries:       query MyQuery { table { ... } }
 *	  - Field arguments:     table(where: {col: "val"}, limit: 10) { ... }
 *	  - Nested fields:       table { col related { col2 } }
 *	  - Mutations:           mutation { insert_table(objects: [...]) { f } }
 *	                         mutation { delete_table(where: {...}) { f } }
 *
 *	  The parser produces a tree of GqlNode structs allocated in
 *	  CurrentMemoryContext.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_graphql/graphql_parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "alohadb_graphql.h"

/* ----------------------------------------------------------------
 * Lexer token types
 * ---------------------------------------------------------------- */

typedef enum GqlTokenType
{
	TOK_LBRACE,			/* { */
	TOK_RBRACE,			/* } */
	TOK_LPAREN,			/* ( */
	TOK_RPAREN,			/* ) */
	TOK_LBRACKET,			/* [ */
	TOK_RBRACKET,			/* ] */
	TOK_COLON,				/* : */
	TOK_COMMA,				/* , */
	TOK_NAME,				/* identifier */
	TOK_STRING,			/* "..." */
	TOK_INT,				/* integer literal */
	TOK_FLOAT,				/* float literal */
	TOK_TRUE,				/* true */
	TOK_FALSE,				/* false */
	TOK_NULL,				/* null */
	TOK_EOF				/* end of input */
} GqlTokenType;

typedef struct GqlToken
{
	GqlTokenType type;
	char		value[GQL_MAX_NAME_LEN];
} GqlToken;

/* ----------------------------------------------------------------
 * Parser state
 * ---------------------------------------------------------------- */

typedef struct GqlParser
{
	const char *input;		/* original query string */
	const char *pos;		/* current position */
	GqlToken	current;	/* current lookahead token */
} GqlParser;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static void parser_init(GqlParser *p, const char *query);
static void parser_advance(GqlParser *p);
static void parser_expect(GqlParser *p, GqlTokenType expected);
static bool parser_match(GqlParser *p, GqlTokenType type);

static GqlNode *parse_document(GqlParser *p);
static GqlNode *parse_selection_set(GqlParser *p);
static GqlNode *parse_field(GqlParser *p);
static GqlNode *parse_arguments(GqlParser *p);
static char *parse_value_as_string(GqlParser *p);
static char *parse_object_value(GqlParser *p);
static char *parse_array_value(GqlParser *p);

static GqlNode *make_node(GqlNodeType type, const char *name);

/* ----------------------------------------------------------------
 * Lexer implementation
 * ---------------------------------------------------------------- */

/*
 * Skip whitespace and comments (lines starting with #).
 */
static void
skip_whitespace(GqlParser *p)
{
	while (*p->pos != '\0')
	{
		if (*p->pos == ' ' || *p->pos == '\t' ||
			*p->pos == '\n' || *p->pos == '\r')
		{
			p->pos++;
		}
		else if (*p->pos == '#')
		{
			/* Skip line comment */
			while (*p->pos != '\0' && *p->pos != '\n')
				p->pos++;
		}
		else if (*p->pos == ',' )
		{
			/*
			 * In GraphQL, commas are optional insignificant separators.
			 * We consume them in the lexer for convenience but also
			 * produce TOK_COMMA when the parser explicitly asks for it.
			 * Here we skip them as whitespace to simplify selection-set
			 * parsing.  The parser_advance function handles commas
			 * properly by returning TOK_COMMA when needed.
			 */
			break;
		}
		else
			break;
	}
}

/*
 * Read the next token into p->current.
 */
static void
parser_advance(GqlParser *p)
{
	skip_whitespace(p);

	if (*p->pos == '\0')
	{
		p->current.type = TOK_EOF;
		p->current.value[0] = '\0';
		return;
	}

	switch (*p->pos)
	{
		case '{':
			p->current.type = TOK_LBRACE;
			p->current.value[0] = '{';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case '}':
			p->current.type = TOK_RBRACE;
			p->current.value[0] = '}';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case '(':
			p->current.type = TOK_LPAREN;
			p->current.value[0] = '(';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case ')':
			p->current.type = TOK_RPAREN;
			p->current.value[0] = ')';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case '[':
			p->current.type = TOK_LBRACKET;
			p->current.value[0] = '[';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case ']':
			p->current.type = TOK_RBRACKET;
			p->current.value[0] = ']';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case ':':
			p->current.type = TOK_COLON;
			p->current.value[0] = ':';
			p->current.value[1] = '\0';
			p->pos++;
			return;
		case ',':
			p->current.type = TOK_COMMA;
			p->current.value[0] = ',';
			p->current.value[1] = '\0';
			p->pos++;
			return;

		case '"':
		{
			/* String literal */
			StringInfoData buf;
			int		len;

			initStringInfo(&buf);
			p->pos++;	/* skip opening quote */

			while (*p->pos != '\0' && *p->pos != '"')
			{
				if (*p->pos == '\\')
				{
					p->pos++;
					if (*p->pos == '\0')
						break;
					switch (*p->pos)
					{
						case '"':
							appendStringInfoChar(&buf, '"');
							break;
						case '\\':
							appendStringInfoChar(&buf, '\\');
							break;
						case 'n':
							appendStringInfoChar(&buf, '\n');
							break;
						case 't':
							appendStringInfoChar(&buf, '\t');
							break;
						case 'r':
							appendStringInfoChar(&buf, '\r');
							break;
						default:
							appendStringInfoChar(&buf, *p->pos);
							break;
					}
				}
				else
				{
					appendStringInfoChar(&buf, *p->pos);
				}
				p->pos++;
			}

			if (*p->pos == '"')
				p->pos++;	/* skip closing quote */
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("unterminated string in GraphQL query")));

			p->current.type = TOK_STRING;
			len = buf.len;
			if (len >= GQL_MAX_NAME_LEN)
				len = GQL_MAX_NAME_LEN - 1;
			memcpy(p->current.value, buf.data, len);
			p->current.value[len] = '\0';
			pfree(buf.data);
			return;
		}

		default:
			break;
	}

	/* Number literal (int or float) */
	if (*p->pos == '-' || (*p->pos >= '0' && *p->pos <= '9'))
	{
		const char *start = p->pos;
		bool		is_float = false;
		int			len;

		if (*p->pos == '-')
			p->pos++;

		while (*p->pos >= '0' && *p->pos <= '9')
			p->pos++;

		if (*p->pos == '.')
		{
			is_float = true;
			p->pos++;
			while (*p->pos >= '0' && *p->pos <= '9')
				p->pos++;
		}

		len = p->pos - start;
		if (len >= GQL_MAX_NAME_LEN)
			len = GQL_MAX_NAME_LEN - 1;

		memcpy(p->current.value, start, len);
		p->current.value[len] = '\0';
		p->current.type = is_float ? TOK_FLOAT : TOK_INT;
		return;
	}

	/* Identifier or keyword */
	if ((*p->pos >= 'a' && *p->pos <= 'z') ||
		(*p->pos >= 'A' && *p->pos <= 'Z') ||
		*p->pos == '_')
	{
		const char *start = p->pos;
		int			len;

		while ((*p->pos >= 'a' && *p->pos <= 'z') ||
			   (*p->pos >= 'A' && *p->pos <= 'Z') ||
			   (*p->pos >= '0' && *p->pos <= '9') ||
			   *p->pos == '_')
			p->pos++;

		len = p->pos - start;
		if (len >= GQL_MAX_NAME_LEN)
			len = GQL_MAX_NAME_LEN - 1;

		memcpy(p->current.value, start, len);
		p->current.value[len] = '\0';

		/* Check for keywords */
		if (strcmp(p->current.value, "true") == 0)
			p->current.type = TOK_TRUE;
		else if (strcmp(p->current.value, "false") == 0)
			p->current.type = TOK_FALSE;
		else if (strcmp(p->current.value, "null") == 0)
			p->current.type = TOK_NULL;
		else
			p->current.type = TOK_NAME;

		return;
	}

	/* Unknown character */
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unexpected character '%c' in GraphQL query", *p->pos)));
}

/* ----------------------------------------------------------------
 * Parser helpers
 * ---------------------------------------------------------------- */

static void
parser_init(GqlParser *p, const char *query)
{
	p->input = query;
	p->pos = query;
	parser_advance(p);		/* prime the first token */
}

static void
parser_expect(GqlParser *p, GqlTokenType expected)
{
	if (p->current.type != expected)
	{
		const char *expected_name;

		switch (expected)
		{
			case TOK_LBRACE:	expected_name = "'{'";		break;
			case TOK_RBRACE:	expected_name = "'}'";		break;
			case TOK_LPAREN:	expected_name = "'('";		break;
			case TOK_RPAREN:	expected_name = "')'";		break;
			case TOK_COLON:		expected_name = "':'";		break;
			case TOK_NAME:		expected_name = "identifier"; break;
			default:			expected_name = "token";	break;
		}

		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("GraphQL syntax error: expected %s, got \"%s\"",
						expected_name, p->current.value)));
	}
	parser_advance(p);
}

static bool
parser_match(GqlParser *p, GqlTokenType type)
{
	if (p->current.type == type)
	{
		parser_advance(p);
		return true;
	}
	return false;
}

static GqlNode *
make_node(GqlNodeType type, const char *name)
{
	GqlNode *node = palloc0(sizeof(GqlNode));

	node->type = type;
	if (name)
		node->name = pstrdup(name);
	return node;
}

/* ----------------------------------------------------------------
 * parse_value_as_string
 *
 * Parse any GraphQL value and return it as a C string.
 * For objects and arrays, returns the JSON representation.
 * ---------------------------------------------------------------- */
static char *
parse_object_value(GqlParser *p)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');

	parser_expect(p, TOK_LBRACE);	/* consume '{' */

	while (p->current.type != TOK_RBRACE && p->current.type != TOK_EOF)
	{
		char   *key;
		char   *val;

		if (buf.len > 1)
			appendStringInfoString(&buf, ", ");

		/* key */
		if (p->current.type != TOK_NAME && p->current.type != TOK_STRING)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("GraphQL: expected field name in object, got \"%s\"",
							p->current.value)));

		key = pstrdup(p->current.value);
		parser_advance(p);
		parser_expect(p, TOK_COLON);

		/* value */
		val = parse_value_as_string(p);

		appendStringInfo(&buf, "\"%s\": %s", key, val);
		pfree(key);
		pfree(val);

		/* optional comma */
		parser_match(p, TOK_COMMA);
	}

	parser_expect(p, TOK_RBRACE);
	appendStringInfoChar(&buf, '}');

	return buf.data;
}

static char *
parse_array_value(GqlParser *p)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');

	parser_expect(p, TOK_LBRACKET);	/* consume '[' */

	while (p->current.type != TOK_RBRACKET && p->current.type != TOK_EOF)
	{
		char   *val;

		if (buf.len > 1)
			appendStringInfoString(&buf, ", ");

		val = parse_value_as_string(p);
		appendStringInfoString(&buf, val);
		pfree(val);

		/* optional comma */
		parser_match(p, TOK_COMMA);
	}

	parser_expect(p, TOK_RBRACKET);
	appendStringInfoChar(&buf, ']');

	return buf.data;
}

static char *
parse_value_as_string(GqlParser *p)
{
	StringInfoData buf;

	switch (p->current.type)
	{
		case TOK_STRING:
		{
			/* Return as JSON string with quotes */
			char   *result;

			initStringInfo(&buf);
			appendStringInfo(&buf, "\"%s\"", p->current.value);
			result = buf.data;
			parser_advance(p);
			return result;
		}

		case TOK_INT:
		case TOK_FLOAT:
		{
			char   *result = pstrdup(p->current.value);

			parser_advance(p);
			return result;
		}

		case TOK_TRUE:
		{
			parser_advance(p);
			return pstrdup("true");
		}

		case TOK_FALSE:
		{
			parser_advance(p);
			return pstrdup("false");
		}

		case TOK_NULL:
		{
			parser_advance(p);
			return pstrdup("null");
		}

		case TOK_NAME:
		{
			/*
			 * Enum-like value (unquoted name in value position).
			 * We treat it as a quoted string for JSON output.
			 */
			char   *result;

			initStringInfo(&buf);
			appendStringInfo(&buf, "\"%s\"", p->current.value);
			result = buf.data;
			parser_advance(p);
			return result;
		}

		case TOK_LBRACE:
			return parse_object_value(p);

		case TOK_LBRACKET:
			return parse_array_value(p);

		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("GraphQL: unexpected token \"%s\" in value position",
							p->current.value)));
			return NULL;	/* keep compiler happy */
	}
}

/* ----------------------------------------------------------------
 * parse_arguments
 *
 * Parse: ( name: value, name: value, ... )
 * Returns a linked list of GQL_ARGUMENT nodes.
 * ---------------------------------------------------------------- */
static GqlNode *
parse_arguments(GqlParser *p)
{
	GqlNode *head = NULL;
	GqlNode *tail = NULL;

	parser_expect(p, TOK_LPAREN);		/* consume '(' */

	while (p->current.type != TOK_RPAREN && p->current.type != TOK_EOF)
	{
		GqlNode *arg;
		char	argname[GQL_MAX_NAME_LEN];

		/* argument name */
		if (p->current.type != TOK_NAME)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("GraphQL: expected argument name, got \"%s\"",
							p->current.value)));

		strlcpy(argname, p->current.value, GQL_MAX_NAME_LEN);
		parser_advance(p);
		parser_expect(p, TOK_COLON);	/* consume ':' */

		/* argument value */
		arg = make_node(GQL_ARGUMENT, argname);
		arg->value = parse_value_as_string(p);

		/* Append to linked list */
		if (tail)
			tail->next = arg;
		else
			head = arg;
		tail = arg;

		/* optional comma between arguments */
		parser_match(p, TOK_COMMA);
	}

	parser_expect(p, TOK_RPAREN);		/* consume ')' */

	return head;
}

/* ----------------------------------------------------------------
 * parse_field
 *
 * Parse a single field: name(args) { children }
 * Name and selection set are required; args are optional.
 * ---------------------------------------------------------------- */
static GqlNode *
parse_field(GqlParser *p)
{
	GqlNode *field;
	char	fieldname[GQL_MAX_NAME_LEN];

	if (p->current.type != TOK_NAME)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("GraphQL: expected field name, got \"%s\"",
						p->current.value)));

	strlcpy(fieldname, p->current.value, GQL_MAX_NAME_LEN);
	parser_advance(p);

	field = make_node(GQL_FIELD, fieldname);

	/* Optional arguments: (arg: val, ...) */
	if (p->current.type == TOK_LPAREN)
		field->args = parse_arguments(p);

	/* Optional selection set: { child1 child2 } */
	if (p->current.type == TOK_LBRACE)
		field->children = parse_selection_set(p);

	return field;
}

/* ----------------------------------------------------------------
 * parse_selection_set
 *
 * Parse: { field1 field2 ... }
 * Returns a linked list of GQL_FIELD nodes.
 * ---------------------------------------------------------------- */
static GqlNode *
parse_selection_set(GqlParser *p)
{
	GqlNode *head = NULL;
	GqlNode *tail = NULL;

	parser_expect(p, TOK_LBRACE);		/* consume '{' */

	while (p->current.type != TOK_RBRACE && p->current.type != TOK_EOF)
	{
		GqlNode *field = parse_field(p);

		if (tail)
			tail->next = field;
		else
			head = field;
		tail = field;

		/* optional comma between fields (GraphQL allows commas as separators) */
		parser_match(p, TOK_COMMA);
	}

	parser_expect(p, TOK_RBRACE);		/* consume '}' */

	return head;
}

/* ----------------------------------------------------------------
 * parse_document
 *
 * Top-level grammar:
 *   document := operation
 *   operation := "query" [name] selectionSet
 *              | "mutation" [name] selectionSet
 *              | selectionSet     (anonymous query)
 * ---------------------------------------------------------------- */
static GqlNode *
parse_document(GqlParser *p)
{
	GqlNode *root;

	if (p->current.type == TOK_NAME &&
		strcmp(p->current.value, "query") == 0)
	{
		parser_advance(p);		/* consume "query" */

		root = make_node(GQL_QUERY, NULL);

		/* Optional operation name */
		if (p->current.type == TOK_NAME)
		{
			root->name = pstrdup(p->current.value);
			parser_advance(p);
		}

		root->children = parse_selection_set(p);
	}
	else if (p->current.type == TOK_NAME &&
			 strcmp(p->current.value, "mutation") == 0)
	{
		parser_advance(p);		/* consume "mutation" */

		root = make_node(GQL_MUTATION, NULL);

		/* Optional operation name */
		if (p->current.type == TOK_NAME)
		{
			root->name = pstrdup(p->current.value);
			parser_advance(p);
		}

		root->children = parse_selection_set(p);
	}
	else if (p->current.type == TOK_LBRACE)
	{
		/* Anonymous query */
		root = make_node(GQL_QUERY, NULL);
		root->children = parse_selection_set(p);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("GraphQL: expected 'query', 'mutation', or '{', got \"%s\"",
						p->current.value)));
		return NULL;	/* keep compiler happy */
	}

	return root;
}

/* ----------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------- */

GqlNode *
gql_parse(const char *query)
{
	GqlParser p;

	if (query == NULL || query[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("GraphQL query must not be empty")));

	if (strlen(query) > GQL_MAX_QUERY_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("GraphQL query exceeds maximum length of %d bytes",
						GQL_MAX_QUERY_LEN)));

	parser_init(&p, query);
	return parse_document(&p);
}

void
gql_free(GqlNode *node)
{
	if (node == NULL)
		return;

	/* Recurse into children, args, and next */
	gql_free(node->args);
	gql_free(node->children);
	gql_free(node->next);

	if (node->name)
		pfree(node->name);
	if (node->value)
		pfree(node->value);

	pfree(node);
}
