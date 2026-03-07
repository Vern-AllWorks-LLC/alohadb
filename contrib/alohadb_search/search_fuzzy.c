/*-------------------------------------------------------------------------
 *
 * search_fuzzy.c
 *	  Fuzzy string matching functions: Levenshtein edit distance and
 *	  Double Metaphone phonetic encoding.
 *
 *	  search_edit_distance: Standard Wagner-Fischer DP algorithm for
 *	  computing the Levenshtein distance between two strings.
 *
 *	  search_fuzzy_match: Convenience wrapper that returns true when
 *	  edit distance <= max_distance.
 *
 *	  search_phonetic: Double Metaphone algorithm producing an
 *	  uppercase code of up to 4 characters.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_search/search_fuzzy.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <string.h>

#include "fmgr.h"
#include "utils/builtins.h"
#include "varatt.h"

#include "alohadb_search.h"

PG_FUNCTION_INFO_V1(search_edit_distance);
PG_FUNCTION_INFO_V1(search_fuzzy_match);
PG_FUNCTION_INFO_V1(search_phonetic);

/* ----------------------------------------------------------------
 * Helper: compute minimum of three integers
 * ---------------------------------------------------------------- */
static inline int
min3(int a, int b, int c)
{
	int		m = a;

	if (b < m)
		m = b;
	if (c < m)
		m = c;
	return m;
}

/* ----------------------------------------------------------------
 * search_edit_distance(a text, b text) RETURNS int
 *
 * Wagner-Fischer dynamic programming Levenshtein distance.
 * Uses O(min(m,n)) space by keeping only two rows.
 * ---------------------------------------------------------------- */
Datum
search_edit_distance(PG_FUNCTION_ARGS)
{
	text	   *a_text = PG_GETARG_TEXT_PP(0);
	text	   *b_text = PG_GETARG_TEXT_PP(1);
	char	   *a = VARDATA_ANY(a_text);
	char	   *b = VARDATA_ANY(b_text);
	int			a_len = VARSIZE_ANY_EXHDR(a_text);
	int			b_len = VARSIZE_ANY_EXHDR(b_text);
	int		   *prev;
	int		   *curr;
	int		   *tmp;
	int			i,
				j;
	int			result;

	/* Quick exit cases */
	if (a_len == 0)
	{
		PG_FREE_IF_COPY(a_text, 0);
		PG_FREE_IF_COPY(b_text, 1);
		PG_RETURN_INT32(b_len);
	}
	if (b_len == 0)
	{
		PG_FREE_IF_COPY(a_text, 0);
		PG_FREE_IF_COPY(b_text, 1);
		PG_RETURN_INT32(a_len);
	}

	/*
	 * Ensure a is the shorter string so we use less memory.
	 */
	if (a_len > b_len)
	{
		char	   *tmp_s;
		int			tmp_l;

		tmp_s = a; a = b; b = tmp_s;
		tmp_l = a_len; a_len = b_len; b_len = tmp_l;
	}

	/* Allocate two rows of (a_len + 1) integers */
	prev = (int *) palloc(sizeof(int) * (a_len + 1));
	curr = (int *) palloc(sizeof(int) * (a_len + 1));

	/* Initialize the base row */
	for (j = 0; j <= a_len; j++)
		prev[j] = j;

	for (i = 1; i <= b_len; i++)
	{
		curr[0] = i;

		for (j = 1; j <= a_len; j++)
		{
			int		cost = (a[j - 1] == b[i - 1]) ? 0 : 1;

			curr[j] = min3(prev[j] + 1,		/* deletion */
						   curr[j - 1] + 1,	/* insertion */
						   prev[j - 1] + cost);	/* substitution */
		}

		/* Swap rows */
		tmp = prev;
		prev = curr;
		curr = tmp;
	}

	result = prev[a_len];

	pfree(prev);
	pfree(curr);

	PG_FREE_IF_COPY(a_text, 0);
	PG_FREE_IF_COPY(b_text, 1);

	PG_RETURN_INT32(result);
}

/* ----------------------------------------------------------------
 * search_fuzzy_match(input text, target text, max_distance int DEFAULT 2)
 * RETURNS boolean
 *
 * Returns true if edit_distance(input, target) <= max_distance.
 * ---------------------------------------------------------------- */
Datum
search_fuzzy_match(PG_FUNCTION_ARGS)
{
	text	   *input = PG_GETARG_TEXT_PP(0);
	text	   *target = PG_GETARG_TEXT_PP(1);
	int			max_distance = PG_GETARG_INT32(2);
	Datum		dist_datum;
	int			dist;

	dist_datum = DirectFunctionCall2(search_edit_distance,
									PointerGetDatum(input),
									PointerGetDatum(target));
	dist = DatumGetInt32(dist_datum);

	PG_FREE_IF_COPY(input, 0);
	PG_FREE_IF_COPY(target, 1);

	PG_RETURN_BOOL(dist <= max_distance);
}


/* ================================================================
 * Double Metaphone Implementation
 *
 * Produces a primary metaphone code (uppercase, up to 4 chars).
 * Handles common English consonant transformations.
 * ================================================================ */

/*
 * Helper: is character a vowel?
 */
static inline bool
is_vowel(char c)
{
	c = toupper((unsigned char) c);
	return (c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U' || c == 'Y');
}

/*
 * Helper: safe character access with bounds checking
 */
static inline char
char_at(const char *s, int len, int pos)
{
	if (pos < 0 || pos >= len)
		return '\0';
	return toupper((unsigned char) s[pos]);
}

/*
 * Helper: check if a substring matches at a given position
 */
static bool
string_at(const char *s, int len, int pos, const char *match)
{
	int		mlen = strlen(match);
	int		i;

	if (pos < 0 || pos + mlen > len)
		return false;

	for (i = 0; i < mlen; i++)
	{
		if (toupper((unsigned char) s[pos + i]) != toupper((unsigned char) match[i]))
			return false;
	}
	return true;
}

/*
 * double_metaphone
 *
 * Compute Double Metaphone primary code for the given input string.
 * The result is written to 'code' which must have room for at least
 * METAPHONE_CODE_LEN + 1 bytes.
 */
static void
double_metaphone(const char *input, int input_len, char *code)
{
	int		pos = 0;		/* current position in input */
	int		code_pos = 0;	/* current position in output code */
	char	c;

	/* Initialize output */
	memset(code, 0, METAPHONE_CODE_LEN + 1);

	if (input_len == 0)
		return;

	/*
	 * Skip leading silent letter combinations:
	 *   KN, GN, PN, AE, WR
	 */
	if (string_at(input, input_len, 0, "KN") ||
		string_at(input, input_len, 0, "GN") ||
		string_at(input, input_len, 0, "PN") ||
		string_at(input, input_len, 0, "AE") ||
		string_at(input, input_len, 0, "WR"))
	{
		pos = 1;
	}

	/* Handle initial X -> S */
	if (char_at(input, input_len, 0) == 'X')
	{
		code[code_pos++] = 'S';
		pos = 1;
	}

	while (pos < input_len && code_pos < METAPHONE_CODE_LEN)
	{
		c = char_at(input, input_len, pos);

		/* Skip duplicate adjacent letters except C */
		if (c != 'C' && pos > 0 && char_at(input, input_len, pos - 1) == c)
		{
			pos++;
			continue;
		}

		switch (c)
		{
			case 'A':
			case 'E':
			case 'I':
			case 'O':
			case 'U':
				/* Vowels are only encoded at the start of the word */
				if (pos == 0)
					code[code_pos++] = 'A';
				pos++;
				break;

			case 'B':
				/* Silent B after M at end of word (e.g., "dumb") */
				if (pos > 0 && char_at(input, input_len, pos - 1) == 'M' &&
					(pos + 1 >= input_len))
				{
					pos++;
					break;
				}
				code[code_pos++] = 'P';
				pos++;
				break;

			case 'C':
				/* PH-like: SCH -> SK */
				if (string_at(input, input_len, pos, "CH"))
				{
					code[code_pos++] = 'X';
					pos += 2;
				}
				else if (string_at(input, input_len, pos, "CI") ||
						 string_at(input, input_len, pos, "CE") ||
						 string_at(input, input_len, pos, "CY"))
				{
					/* C -> S before E, I, Y */
					code[code_pos++] = 'S';
					pos += 1;
				}
				else
				{
					code[code_pos++] = 'K';
					pos++;
				}
				break;

			case 'D':
				if (string_at(input, input_len, pos, "DGI") ||
					string_at(input, input_len, pos, "DGE") ||
					string_at(input, input_len, pos, "DGY"))
				{
					code[code_pos++] = 'J';
					pos += 2;
				}
				else
				{
					code[code_pos++] = 'T';
					pos++;
				}
				break;

			case 'F':
				code[code_pos++] = 'F';
				pos++;
				break;

			case 'G':
				/* GH handling */
				if (string_at(input, input_len, pos, "GH"))
				{
					/*
					 * GH is silent if preceded by a non-vowel and not at
					 * start, or at end. Otherwise GH -> F.
					 */
					if (pos + 2 >= input_len)
					{
						/* GH at end -- silent */
						pos += 2;
					}
					else if (pos > 0 && !is_vowel(char_at(input, input_len, pos - 1)))
					{
						/* GH after consonant -- silent */
						pos += 2;
					}
					else if (pos == 0)
					{
						/* GH at start -> K */
						code[code_pos++] = 'K';
						pos += 2;
					}
					else
					{
						code[code_pos++] = 'F';
						pos += 2;
					}
				}
				else if (string_at(input, input_len, pos, "GN"))
				{
					/* GN -> N (silent G) */
					pos++;
				}
				else if (pos > 0 && char_at(input, input_len, pos - 1) == 'G')
				{
					/* Double G -- skip */
					pos++;
				}
				else if (string_at(input, input_len, pos, "GI") ||
						 string_at(input, input_len, pos, "GE") ||
						 string_at(input, input_len, pos, "GY"))
				{
					code[code_pos++] = 'J';
					pos++;
				}
				else
				{
					code[code_pos++] = 'K';
					pos++;
				}
				break;

			case 'H':
				/*
				 * H is only coded if before a vowel and not after a vowel
				 * (or after a consonant cluster).
				 */
				if (pos + 1 < input_len && is_vowel(char_at(input, input_len, pos + 1)))
				{
					if (pos == 0 || !is_vowel(char_at(input, input_len, pos - 1)))
					{
						code[code_pos++] = 'H';
					}
				}
				pos++;
				break;

			case 'J':
				code[code_pos++] = 'J';
				pos++;
				break;

			case 'K':
				/* K after K is silent */
				if (pos > 0 && char_at(input, input_len, pos - 1) == 'K')
					pos++;
				else
				{
					code[code_pos++] = 'K';
					pos++;
				}
				break;

			case 'L':
				code[code_pos++] = 'L';
				pos++;
				break;

			case 'M':
				code[code_pos++] = 'M';
				pos++;
				break;

			case 'N':
				code[code_pos++] = 'N';
				pos++;
				break;

			case 'P':
				if (string_at(input, input_len, pos, "PH"))
				{
					code[code_pos++] = 'F';
					pos += 2;
				}
				else
				{
					code[code_pos++] = 'P';
					pos++;
				}
				break;

			case 'Q':
				code[code_pos++] = 'K';
				pos++;
				break;

			case 'R':
				code[code_pos++] = 'R';
				pos++;
				break;

			case 'S':
				if (string_at(input, input_len, pos, "SH"))
				{
					code[code_pos++] = 'X';
					pos += 2;
				}
				else if (string_at(input, input_len, pos, "SIO") ||
						 string_at(input, input_len, pos, "SIA"))
				{
					code[code_pos++] = 'X';
					pos += 3;
				}
				else
				{
					code[code_pos++] = 'S';
					pos++;
				}
				break;

			case 'T':
				if (string_at(input, input_len, pos, "TH"))
				{
					code[code_pos++] = '0';	/* theta sound */
					pos += 2;
				}
				else if (string_at(input, input_len, pos, "TIO") ||
						 string_at(input, input_len, pos, "TIA"))
				{
					code[code_pos++] = 'X';
					pos += 3;
				}
				else
				{
					code[code_pos++] = 'T';
					pos++;
				}
				break;

			case 'V':
				code[code_pos++] = 'F';
				pos++;
				break;

			case 'W':
				/* W before a vowel */
				if (pos + 1 < input_len && is_vowel(char_at(input, input_len, pos + 1)))
				{
					code[code_pos++] = 'A';
					pos++;
				}
				else
				{
					pos++;	/* silent W */
				}
				break;

			case 'X':
				code[code_pos++] = 'K';
				if (code_pos < METAPHONE_CODE_LEN)
					code[code_pos++] = 'S';
				pos++;
				break;

			case 'Y':
				/* Y before a vowel */
				if (pos + 1 < input_len && is_vowel(char_at(input, input_len, pos + 1)))
				{
					code[code_pos++] = 'A';
				}
				pos++;
				break;

			case 'Z':
				code[code_pos++] = 'S';
				pos++;
				break;

			default:
				/* Skip non-alphabetic characters */
				pos++;
				break;
		}
	}

	code[code_pos] = '\0';
}

/* ----------------------------------------------------------------
 * search_phonetic(input text, algorithm text DEFAULT 'double_metaphone')
 * RETURNS text
 *
 * Returns the Double Metaphone primary code for the input string.
 * ---------------------------------------------------------------- */
Datum
search_phonetic(PG_FUNCTION_ARGS)
{
	text	   *input_text = PG_GETARG_TEXT_PP(0);
	char	   *input = VARDATA_ANY(input_text);
	int			input_len = VARSIZE_ANY_EXHDR(input_text);
	char		code[METAPHONE_CODE_LEN + 1];

	/* We ignore the algorithm parameter for now; only double_metaphone */

	double_metaphone(input, input_len, code);

	PG_FREE_IF_COPY(input_text, 0);

	PG_RETURN_TEXT_P(cstring_to_text(code));
}
