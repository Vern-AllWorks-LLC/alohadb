/*-------------------------------------------------------------------------
 *
 * alohadb_measure.c
 *	  The "measurement" data type: a physical quantity carrying a value,
 *	  a standard uncertainty and an SI unit, so that scientific results
 *	  stay qualified when stored, combined and aggregated.
 *
 *	  Internally a measurement stores its value and standard uncertainty
 *	  in coherent SI units (temperatures as absolute kelvin), the SI
 *	  dimension exponent vector (m, kg, s, A, K, mol, cd), and a small
 *	  "display unit" made of factors (unit id, SI prefix id, exponent)
 *	  taken from a fixed built-in unit table.  The display unit's scale
 *	  factor to SI and (for affine temperature units) its offset are
 *	  cached alongside.  Text output renders the value in the display
 *	  unit.
 *
 *	  Affine units (degC, degF) are accepted on input and for conversion
 *	  and comparison.  Any arithmetic on them (+, -, *, /, unary minus,
 *	  powers, scaling) is performed on the absolute kelvin value and the
 *	  result is displayed in kelvin.
 *
 *	  Prior art / patent notes: only methods from public standards are
 *	  used.  Uncertainty propagation is the first-order (linear) law of
 *	  propagation of uncertainty for uncorrelated input quantities from
 *	  the GUM, JCGM 100:2008 ("Evaluation of measurement data - Guide to
 *	  the expression of uncertainty in measurement"), clause 5.1; the
 *	  concise "12.5(3)" notation follows GUM 7.2.2; the inverse-variance
 *	  weighted mean is the textbook maximum-likelihood combination of
 *	  independent normal observations (Gauss/Aitken, 19th-20th century).
 *	  Units, prefixes and dimensions follow the BIPM SI Brochure (9th
 *	  edition, 2019, incl. the 2022 prefixes); conversion factors are the
 *	  exact legal definitions (international yard and pound, 1959) or
 *	  CODATA values.  The provenance model in the SQL script is a small
 *	  subset of the W3C PROV-DM Recommendation (2013).  No Monte-Carlo
 *	  propagation (GUM Supplement 1) or proprietary interval arithmetic
 *	  is used.
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_measure/alohadb_measure.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <math.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "nodes/miscnodes.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_measure",
					.version = "1.0"
);

#define NDIM				7
#define MEAS_MAX_FACTORS	8
#define MAX_SYMBOL_LEN		32
#define MAX_NUMBER_LEN		100

/* One factor of a display unit, e.g. km^2 = {UNIT_M, PREFIX_k, 2} */
typedef struct MeasFactor
{
	int16		unit;			/* index into unit_table */
	int8		prefix;			/* index into prefix_table, 0 = none */
	int8		exp;			/* nonzero exponent */
} MeasFactor;

typedef struct MeasUnit
{
	float8		scale;			/* display value * scale + offset = SI */
	float8		offset;			/* nonzero only for degC / degF */
	int8		dim[NDIM];		/* m kg s A K mol cd exponents */
	uint8		nfactors;
	MeasFactor	f[MEAS_MAX_FACTORS];
} MeasUnit;

typedef struct Measurement
{
	float8		value;			/* SI value (absolute K for temperature) */
	float8		unc;			/* standard uncertainty in SI, >= 0 */
	MeasUnit	unit;			/* display unit */
} Measurement;

StaticAssertDecl(sizeof(Measurement) == 72,
				 "measurement must match INTERNALLENGTH in the SQL script");

#define PG_GETARG_MEAS(n)	((Measurement *) PG_GETARG_POINTER(n))
#define PG_RETURN_MEAS(x)	PG_RETURN_POINTER(x)

/* ----------------------------------------------------------------
 * Built-in unit and prefix tables.
 *
 * The array positions are part of the binary send/recv format: only
 * ever append new entries.
 * ----------------------------------------------------------------
 */
typedef struct UnitDef
{
	const char *name;			/* canonical ASCII symbol */
	const char *pretty;			/* UTF-8 display symbol or NULL */
	double		scale;			/* to SI, multiplied by 10^p10 */
	int			p10;
	double		offset;			/* affine offset (kelvin) */
	int8		dim[NDIM];
	bool		prefixable;
} UnitDef;

#define UTF8_DEG	"\xC2\xB0"

static const UnitDef unit_table[] = {
	/* SI base units (dims: m kg s A K mol cd) */
	{"m", NULL, 1, 0, 0, {1, 0, 0, 0, 0, 0, 0}, true},	/* 0 */
	{"g", NULL, 1, -3, 0, {0, 1, 0, 0, 0, 0, 0}, true},	/* 1 */
	{"s", NULL, 1, 0, 0, {0, 0, 1, 0, 0, 0, 0}, true},	/* 2 */
	{"A", NULL, 1, 0, 0, {0, 0, 0, 1, 0, 0, 0}, true},	/* 3 */
	{"K", NULL, 1, 0, 0, {0, 0, 0, 0, 1, 0, 0}, true},	/* 4 */
	{"mol", NULL, 1, 0, 0, {0, 0, 0, 0, 0, 1, 0}, true},	/* 5 */
	{"cd", NULL, 1, 0, 0, {0, 0, 0, 0, 0, 0, 1}, true},	/* 6 */
	/* SI derived units */
	{"N", NULL, 1, 0, 0, {1, 1, -2, 0, 0, 0, 0}, true},	/* 7 */
	{"J", NULL, 1, 0, 0, {2, 1, -2, 0, 0, 0, 0}, true},	/* 8 */
	{"W", NULL, 1, 0, 0, {2, 1, -3, 0, 0, 0, 0}, true},	/* 9 */
	{"Pa", NULL, 1, 0, 0, {-1, 1, -2, 0, 0, 0, 0}, true},	/* 10 */
	{"Hz", NULL, 1, 0, 0, {0, 0, -1, 0, 0, 0, 0}, true},	/* 11 */
	{"Bq", NULL, 1, 0, 0, {0, 0, -1, 0, 0, 0, 0}, true},	/* 12 */
	{"C", NULL, 1, 0, 0, {0, 0, 1, 1, 0, 0, 0}, true},	/* 13 */
	{"V", NULL, 1, 0, 0, {2, 1, -3, -1, 0, 0, 0}, true},	/* 14 */
	{"ohm", "\xCE\xA9", 1, 0, 0, {2, 1, -3, -2, 0, 0, 0}, true},	/* 15 */
	{"S", NULL, 1, 0, 0, {-2, -1, 3, 2, 0, 0, 0}, true},	/* 16 */
	{"F", NULL, 1, 0, 0, {-2, -1, 4, 2, 0, 0, 0}, true},	/* 17 */
	{"T", NULL, 1, 0, 0, {0, 1, -2, -1, 0, 0, 0}, true},	/* 18 */
	{"Wb", NULL, 1, 0, 0, {2, 1, -2, -1, 0, 0, 0}, true},	/* 19 */
	{"H", NULL, 1, 0, 0, {2, 1, -2, -2, 0, 0, 0}, true},	/* 20 */
	{"lm", NULL, 1, 0, 0, {0, 0, 0, 0, 0, 0, 1}, true},	/* 21 */
	{"lx", NULL, 1, 0, 0, {-2, 0, 0, 0, 0, 0, 1}, true},	/* 22 */
	{"Gy", NULL, 1, 0, 0, {2, 0, -2, 0, 0, 0, 0}, true},	/* 23 */
	{"Sv", NULL, 1, 0, 0, {2, 0, -2, 0, 0, 0, 0}, true},	/* 24 */
	{"kat", NULL, 1, 0, 0, {0, 0, -1, 0, 0, 1, 0}, true},	/* 25 */
	/* common non-SI units */
	{"min", NULL, 60, 0, 0, {0, 0, 1, 0, 0, 0, 0}, false},	/* 26 */
	{"h", NULL, 3600, 0, 0, {0, 0, 1, 0, 0, 0, 0}, false},	/* 27 */
	{"d", NULL, 86400, 0, 0, {0, 0, 1, 0, 0, 0, 0}, false},	/* 28 */
	{"L", NULL, 1, -3, 0, {3, 0, 0, 0, 0, 0, 0}, true},	/* 29 */
	{"eV", NULL, 1.602176634e-19, 0, 0, {2, 1, -2, 0, 0, 0, 0}, true},	/* 30 */
	{"cal", NULL, 4.184, 0, 0, {2, 1, -2, 0, 0, 0, 0}, true},	/* 31 */
	{"bar", NULL, 1, 5, 0, {-1, 1, -2, 0, 0, 0, 0}, true},	/* 32 */
	{"atm", NULL, 101325, 0, 0, {-1, 1, -2, 0, 0, 0, 0}, false},	/* 33 */
	{"mmHg", NULL, 133.322387415, 0, 0, {-1, 1, -2, 0, 0, 0, 0}, false},	/* 34 */
	{"degC", UTF8_DEG "C", 1, 0, 273.15, {0, 0, 0, 0, 1, 0, 0}, false},	/* 35 */
	{"degF", UTF8_DEG "F", 5.0 / 9.0, 0, 273.15 - 32.0 * 5.0 / 9.0,
	{0, 0, 0, 0, 1, 0, 0}, false},	/* 36 */
	{"in", NULL, 0.0254, 0, 0, {1, 0, 0, 0, 0, 0, 0}, false},	/* 37 */
	{"ft", NULL, 0.3048, 0, 0, {1, 0, 0, 0, 0, 0, 0}, false},	/* 38 */
	{"mi", NULL, 1609.344, 0, 0, {1, 0, 0, 0, 0, 0, 0}, false},	/* 39 */
	{"lb", NULL, 0.45359237, 0, 0, {0, 1, 0, 0, 0, 0, 0}, false},	/* 40 */
	{"mph", NULL, 0.44704, 0, 0, {1, 0, -1, 0, 0, 0, 0}, false},	/* 41 */
	{"Wh", NULL, 3600, 0, 0, {2, 1, -2, 0, 0, 0, 0}, true},	/* 42 */
	{"Da", NULL, 1.66053906892e-27, 0, 0, {0, 1, 0, 0, 0, 0, 0}, true},	/* 43 */
	{"angstrom", "\xC3\x85", 1, -10, 0, {1, 0, 0, 0, 0, 0, 0}, false},	/* 44 */
};

#define N_UNITS		((int) lengthof(unit_table))
#define UNIT_M		0
#define UNIT_G		1
#define UNIT_S		2
#define UNIT_A		3
#define UNIT_K		4
#define UNIT_MOL	5
#define UNIT_CD		6
#define UNIT_OHM	15
#define UNIT_L		29
#define UNIT_DEGC	35
#define UNIT_DEGF	36
#define UNIT_ANG	44

typedef struct UnitAlias
{
	const char *spelling;
	int			unit;
	bool		utf8only;
} UnitAlias;

static const UnitAlias unit_aliases[] = {
	{"l", UNIT_L, false},
	{"Ohm", UNIT_OHM, false},
	{"Angstrom", UNIT_ANG, false},
	{"\xE2\x84\xA6", UNIT_OHM, true},	/* U+2126 OHM SIGN */
	{"\xE2\x84\x83", UNIT_DEGC, true},	/* U+2103 DEGREE CELSIUS */
	{"\xE2\x84\x89", UNIT_DEGF, true},	/* U+2109 DEGREE FAHRENHEIT */
	{"\xE2\x84\xAB", UNIT_ANG, true},	/* U+212B ANGSTROM SIGN */
};

typedef struct PrefixDef
{
	const char *name;			/* ASCII spelling */
	const char *pretty;			/* UTF-8 display spelling or NULL */
	int			p10;
} PrefixDef;

static const PrefixDef prefix_table[] = {
	{"", NULL, 0},				/* 0: no prefix */
	{"q", NULL, -30},
	{"r", NULL, -27},
	{"y", NULL, -24},
	{"z", NULL, -21},
	{"a", NULL, -18},
	{"f", NULL, -15},
	{"p", NULL, -12},
	{"n", NULL, -9},
	{"u", "\xC2\xB5", -6},		/* 9: micro, displayed as U+00B5 */
	{"m", NULL, -3},
	{"c", NULL, -2},
	{"d", NULL, -1},
	{"da", NULL, 1},
	{"h", NULL, 2},
	{"k", NULL, 3},				/* 15 */
	{"M", NULL, 6},
	{"G", NULL, 9},
	{"T", NULL, 12},
	{"P", NULL, 15},
	{"E", NULL, 18},
	{"Z", NULL, 21},
	{"Y", NULL, 24},
	{"R", NULL, 27},
	{"Q", NULL, 30},
};

#define N_PREFIXES		((int) lengthof(prefix_table))
#define PREFIX_MICRO	9
#define PREFIX_KILO		15

/* UTF-8 spellings of micro accepted on input */
static const char *const micro_utf8[] = {
	"\xC2\xB5",					/* U+00B5 MICRO SIGN */
	"\xCE\xBC",					/* U+03BC GREEK SMALL LETTER MU */
};

static const char *const dim_ascii[NDIM] = {"L", "M", "T", "I", "Theta", "N", "J"};
static const char *const dim_utf8[NDIM] = {"L", "M", "T", "I", "\xCE\x98", "N", "J"};

/* exact powers of ten representable in a double */
static const double exact_pow10[] = {
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
	1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22
};

/* ----------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------
 */
static inline bool
db_is_utf8(void)
{
	return GetDatabaseEncoding() == PG_UTF8;
}

static double
apply_pow10(double x, int p10)
{
	int			a = abs(p10);
	double		f;

	if (a < (int) lengthof(exact_pow10))
		f = exact_pow10[a];
	else
		f = pow(10.0, (double) a);
	return (p10 >= 0) ? x * f : x / f;
}

static int
lookup_unit_exact(const char *sym, bool utf8)
{
	for (int i = 0; i < N_UNITS; i++)
	{
		if (strcmp(sym, unit_table[i].name) == 0)
			return i;
		if (utf8 && unit_table[i].pretty &&
			strcmp(sym, unit_table[i].pretty) == 0)
			return i;
	}
	for (int i = 0; i < (int) lengthof(unit_aliases); i++)
	{
		if (unit_aliases[i].utf8only && !utf8)
			continue;
		if (strcmp(sym, unit_aliases[i].spelling) == 0)
			return unit_aliases[i].unit;
	}
	return -1;
}

/*
 * Resolve a unit symbol, possibly carrying an SI prefix.  An exact match
 * always wins ("min", "Pa", "cd", "mmHg"); otherwise the longest prefix
 * whose remainder is a prefixable unit is taken.
 */
static bool
lookup_symbol(const char *sym, bool utf8, int *unit, int *prefix)
{
	int			u = lookup_unit_exact(sym, utf8);
	int			best_len = 0;

	if (u >= 0)
	{
		*unit = u;
		*prefix = 0;
		return true;
	}

	for (int i = 1; i < N_PREFIXES; i++)
	{
		const char *spell[1 + lengthof(micro_utf8)];
		int			nspell = 0;

		spell[nspell++] = prefix_table[i].name;
		if (utf8 && i == PREFIX_MICRO)
		{
			for (int j = 0; j < (int) lengthof(micro_utf8); j++)
				spell[nspell++] = micro_utf8[j];
		}

		for (int j = 0; j < nspell; j++)
		{
			int			plen = strlen(spell[j]);

			if (plen <= best_len || strncmp(sym, spell[j], plen) != 0 ||
				sym[plen] == '\0')
				continue;
			u = lookup_unit_exact(sym + plen, utf8);
			if (u >= 0 && unit_table[u].prefixable)
			{
				*unit = u;
				*prefix = i;
				best_len = plen;
			}
		}
	}
	return best_len > 0;
}

/*
 * Multiply a factor into a unit, merging with an existing identical
 * (unit, prefix) factor.  Returns false if the exponent would overflow or
 * there is no room left.
 */
static bool
unit_add_factor(MeasUnit *u, int unit, int prefix, int exp)
{
	for (int i = 0; i < u->nfactors; i++)
	{
		if (u->f[i].unit == unit && u->f[i].prefix == prefix)
		{
			int			ne = u->f[i].exp + exp;

			if (ne < PG_INT8_MIN || ne > PG_INT8_MAX)
				return false;
			if (ne == 0)
			{
				for (int j = i + 1; j < u->nfactors; j++)
					u->f[j - 1] = u->f[j];
				u->nfactors--;
				memset(&u->f[u->nfactors], 0, sizeof(MeasFactor));
			}
			else
				u->f[i].exp = (int8) ne;
			return true;
		}
	}
	if (exp == 0)
		return true;
	if (u->nfactors >= MEAS_MAX_FACTORS || exp < PG_INT8_MIN || exp > PG_INT8_MAX)
		return false;
	u->f[u->nfactors].unit = (int16) unit;
	u->f[u->nfactors].prefix = (int8) prefix;
	u->f[u->nfactors].exp = (int8) exp;
	u->nfactors++;
	return true;
}

/*
 * Compute dimension vector, scale and offset from the factor list.
 */
static bool
unit_finalize(MeasUnit *u, Node *escontext)
{
	int			dims[NDIM] = {0};
	double		mant = 1.0;
	int			p10 = 0;
	double		offset = 0.0;

	for (int i = 0; i < u->nfactors; i++)
	{
		const MeasFactor *f = &u->f[i];
		const UnitDef *ud = &unit_table[f->unit];
		int			e = f->exp;

		for (int d = 0; d < NDIM; d++)
			dims[d] += ud->dim[d] * e;
		p10 += (prefix_table[f->prefix].p10 + ud->p10) * e;
		for (int k = 0; k < abs(e); k++)
		{
			if (e > 0)
				mant *= ud->scale;
			else
				mant /= ud->scale;
		}
		if (ud->offset != 0.0)
		{
			if (u->nfactors != 1 || e != 1 || f->prefix != 0)
				ereturn(escontext, false,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("affine temperature unit \"%s\" cannot be combined with other units or raised to a power",
							   ud->name),
						errhint("Use K for temperature differences and compound units."));
			offset = ud->offset;
		}
	}

	for (int d = 0; d < NDIM; d++)
	{
		if (dims[d] < PG_INT8_MIN || dims[d] > PG_INT8_MAX)
			ereturn(escontext, false,
					errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					errmsg("measurement dimension exponent out of range"));
		u->dim[d] = (int8) dims[d];
	}

	u->scale = apply_pow10(mant, p10);
	u->offset = offset;
	if (!isfinite(u->scale) || u->scale <= 0.0)
		ereturn(escontext, false,
				errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("unit scale factor out of range"));
	return true;
}

/* Set u to the coherent SI unit for the given dimensions (kg*m^2/s^2 ...) */
static void
unit_set_si(MeasUnit *u, const int8 *dim)
{
	static const int order_dim[NDIM] = {1, 0, 2, 3, 4, 5, 6};
	static const int order_unit[NDIM] = {UNIT_G, UNIT_M, UNIT_S, UNIT_A,
	UNIT_K, UNIT_MOL, UNIT_CD};
	int8		d[NDIM];

	memcpy(d, dim, sizeof(d));
	memset(u, 0, sizeof(MeasUnit));
	for (int i = 0; i < NDIM; i++)
	{
		int			e = d[order_dim[i]];

		if (e != 0)
			(void) unit_add_factor(u, order_unit[i],
								   order_unit[i] == UNIT_G ? PREFIX_KILO : 0, e);
	}
	(void) unit_finalize(u, NULL);
}

static inline bool
unit_is_affine(const MeasUnit *u)
{
	return u->offset != 0.0;
}

/* For additive results: keep the display unit unless it is affine. */
static void
unit_for_linear_result(const MeasUnit *src, MeasUnit *dst)
{
	if (unit_is_affine(src))
		unit_set_si(dst, src->dim);
	else
		*dst = *src;
}

static void
append_factor(StringInfo buf, const MeasFactor *f, int exp, bool utf8)
{
	const PrefixDef *pd = &prefix_table[f->prefix];
	const UnitDef *ud = &unit_table[f->unit];

	appendStringInfoString(buf, (utf8 && pd->pretty) ? pd->pretty : pd->name);
	appendStringInfoString(buf, (utf8 && ud->pretty) ? ud->pretty : ud->name);
	if (exp != 1)
		appendStringInfo(buf, "^%d", exp);
}

/* Render the display unit; "" for a dimensionless plain number. */
static void
render_unit(StringInfo buf, const MeasUnit *u)
{
	bool		utf8 = db_is_utf8();
	bool		anypos = false;

	for (int i = 0; i < u->nfactors; i++)
	{
		if (u->f[i].exp > 0)
		{
			if (anypos)
				appendStringInfoChar(buf, '*');
			append_factor(buf, &u->f[i], u->f[i].exp, utf8);
			anypos = true;
		}
	}
	for (int i = 0, n = 0; i < u->nfactors; i++)
	{
		if (u->f[i].exp < 0)
		{
			if (anypos)
			{
				appendStringInfoChar(buf, '/');
				append_factor(buf, &u->f[i], -u->f[i].exp, utf8);
			}
			else
			{
				if (n++ > 0)
					appendStringInfoChar(buf, '*');
				append_factor(buf, &u->f[i], u->f[i].exp, utf8);
			}
		}
	}
}

static char *
dimension_string(const int8 *dim)
{
	StringInfoData buf;
	bool		utf8 = db_is_utf8();
	bool		any = false;

	initStringInfo(&buf);
	for (int d = 0; d < NDIM; d++)
	{
		if (dim[d] == 0)
			continue;
		if (any)
			appendStringInfoString(&buf, utf8 ? "\xC2\xB7" : "*");
		appendStringInfoString(&buf, utf8 ? dim_utf8[d] : dim_ascii[d]);
		if (dim[d] != 1)
			appendStringInfo(&buf, "^%d", dim[d]);
		any = true;
	}
	if (!any)
		appendStringInfoChar(&buf, '1');
	return buf.data;
}

static void
check_same_dim(const Measurement *a, const Measurement *b, const char *what)
{
	if (memcmp(a->unit.dim, b->unit.dim, NDIM) != 0)
		ereport(ERROR,
				errcode(ERRCODE_DATATYPE_MISMATCH),
				errmsg("incompatible measurement dimensions in %s: %s vs %s",
					   what, dimension_string(a->unit.dim),
					   dimension_string(b->unit.dim)));
}

static void
check_unit_dim(const Measurement *a, const MeasUnit *u, const char *unitstr)
{
	if (memcmp(a->unit.dim, u->dim, NDIM) != 0)
		ereport(ERROR,
				errcode(ERRCODE_DATATYPE_MISMATCH),
				errmsg("cannot express a measurement of dimension %s in unit \"%s\" (dimension %s)",
					   dimension_string(a->unit.dim), unitstr,
					   dimension_string(u->dim)));
}

/* Validate a computed result: everything finite, also in display units. */
static Measurement *
check_result(Measurement *r)
{
	if (!isfinite(r->value) || !isfinite(r->unc) ||
		!isfinite((r->value - r->unit.offset) / r->unit.scale) ||
		!isfinite(r->unc / r->unit.scale))
		ereport(ERROR,
				errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("measurement value out of range"));
	if (r->value == 0.0)
		r->value = 0.0;			/* no negative zero */
	return r;
}

static void
check_float_arg(double x, const char *what)
{
	if (!isfinite(x))
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("%s must be a finite number", what));
}

/* ----------------------------------------------------------------
 * Parsing
 * ----------------------------------------------------------------
 */

/* length of the symbol character at p, or 0 if p is not a symbol char */
static int
symbol_char_len(const unsigned char *p, bool utf8)
{
	if (*p < 0x80)
		return isalpha(*p) ? 1 : 0;
	if (utf8 && p[0] == 0xC2 &&
		(p[1] == 0xB1 || p[1] == 0xB2 || p[1] == 0xB3 || p[1] == 0xB7))
		return 0;				/* plus-minus, superscripts, middle dot */
	return pg_mblen((const char *) p);
}

#define UNIT_FAIL(...) \
	do { \
		if (for_input) \
			ereturn(escontext, false, \
					errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), \
					errmsg("invalid input syntax for type %s: \"%s\"", \
						   "measurement", whole), \
					errdetail(__VA_ARGS__)); \
		else \
			ereturn(escontext, false, \
					errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
					errmsg("invalid unit expression \"%s\"", whole), \
					errdetail(__VA_ARGS__)); \
	} while (0)

/*
 * Parse a unit expression such as "kg*m^2/s^2", "m*s^-2", "µm", "°C".
 * Operators are evaluated left to right; an empty string is the
 * dimensionless unit.
 */
static bool
parse_unit_expr(const char *str, MeasUnit *out, const char *whole,
				bool for_input, Node *escontext)
{
	const unsigned char *p = (const unsigned char *) str;
	bool		utf8 = db_is_utf8();
	int			sign = 1;
	int			nterms = 0;

	memset(out, 0, sizeof(MeasUnit));

	while (isspace(*p))
		p++;
	if (*p != '\0')
	{
		for (;;)
		{
			char		sym[MAX_SYMBOL_LEN + 1];
			int			symlen = 0;
			int			unit,
						prefix;
			int			exp = 1;

			while (isspace(*p))
				p++;
			for (;;)
			{
				int			l = symbol_char_len(p, utf8);

				if (l == 0)
					break;
				if (symlen + l > MAX_SYMBOL_LEN)
					UNIT_FAIL("Unit symbol is too long.");
				memcpy(sym + symlen, p, l);
				symlen += l;
				p += l;
			}
			sym[symlen] = '\0';
			if (symlen == 0)
			{
				if (*p == '\0')
					UNIT_FAIL("Unit expression ends with an operator.");
				UNIT_FAIL("Expected a unit symbol at \"%s\".", (const char *) p);
			}
			if (!lookup_symbol(sym, utf8, &unit, &prefix))
				UNIT_FAIL("Unknown unit \"%s\".", sym);

			while (isspace(*p))
				p++;
			if (*p == '^')
			{
				int			esign = 1;
				int			ndig = 0;

				p++;
				while (isspace(*p))
					p++;
				if (*p == '-' || *p == '+')
				{
					esign = (*p == '-') ? -1 : 1;
					p++;
				}
				exp = 0;
				while (isdigit(*p))
				{
					if (++ndig > 3)
						UNIT_FAIL("Unit exponent is out of range.");
					exp = exp * 10 + (*p - '0');
					p++;
				}
				if (ndig == 0)
					UNIT_FAIL("Expected an integer exponent after \"^\".");
				exp *= esign;
			}
			else if (utf8 && p[0] == 0xC2 && (p[1] == 0xB2 || p[1] == 0xB3))
			{
				exp = (p[1] == 0xB2) ? 2 : 3;
				p += 2;
			}
			if (exp == 0)
				UNIT_FAIL("Unit exponent must not be zero.");
			if (exp < PG_INT8_MIN || exp > PG_INT8_MAX)
				UNIT_FAIL("Unit exponent is out of range.");

			nterms++;
			if (unit_table[unit].offset != 0.0 && (nterms > 1 || exp != 1))
				UNIT_FAIL("Affine temperature units (degC, degF) cannot be combined with other units or raised to a power.");
			if (nterms > 1 && out->nfactors == 1 &&
				unit_table[out->f[0].unit].offset != 0.0)
				UNIT_FAIL("Affine temperature units (degC, degF) cannot be combined with other units or raised to a power.");
			if (!unit_add_factor(out, unit, prefix, sign * exp))
				UNIT_FAIL("Unit expression is too complex.");

			while (isspace(*p))
				p++;
			if (*p == '\0')
				break;
			if (*p == '*')
			{
				sign = 1;
				p++;
			}
			else if (*p == '/')
			{
				sign = -1;
				p++;
			}
			else if (utf8 && p[0] == 0xC2 && p[1] == 0xB7)
			{
				sign = 1;
				p += 2;
			}
			else
				UNIT_FAIL("Unexpected character at \"%s\".", (const char *) p);
		}
	}

	if (!unit_finalize(out, escontext))
		return false;
	return true;
}

/* [sign] digits [. digits] ; returns length (0 if no number) */
static int
scan_mantissa(const char *p, bool allow_sign, int *ndec)
{
	const char *q = p;
	int			nint = 0,
				nfrac = 0;

	if (allow_sign && (*q == '+' || *q == '-'))
		q++;
	while (isdigit((unsigned char) *q))
	{
		q++;
		nint++;
	}
	if (*q == '.')
	{
		const char *r = q + 1;

		while (isdigit((unsigned char) *r))
		{
			r++;
			nfrac++;
		}
		if (nint + nfrac > 0)
			q = r;
	}
	*ndec = nfrac;
	if (nint + nfrac == 0)
		return 0;
	return q - p;
}

/* e[sign]digits ; returns length consumed, 0 if absent, -1 if too large */
static int
scan_exponent(const char *p, int *e10)
{
	const char *q = p;
	int			sign = 1;
	int			v = 0;
	int			ndig = 0;

	*e10 = 0;
	if (*q != 'e' && *q != 'E')
		return 0;
	q++;
	if (*q == '+' || *q == '-')
	{
		sign = (*q == '-') ? -1 : 1;
		q++;
	}
	if (!isdigit((unsigned char) *q))
		return 0;				/* not an exponent, e.g. "5 eV" -> unit */
	while (isdigit((unsigned char) *q))
	{
		if (++ndig > 6)
			return -1;
		v = v * 10 + (*q - '0');
		q++;
	}
	*e10 = sign * v;
	return q - p;
}

static bool
number_from_parts(const char *mant, int e10, double *out)
{
	char		buf[MAX_NUMBER_LEN + 32];
	char	   *end;
	double		d;

	snprintf(buf, sizeof(buf), "%se%d", mant, e10);
	errno = 0;
	d = strtod(buf, &end);
	if (*end != '\0' || !isfinite(d))
		return false;
	*out = d;
	return true;
}

#define IN_FAIL(...) \
	ereturn(escontext, false, \
			errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), \
			errmsg("invalid input syntax for type %s: \"%s\"", \
				   "measurement", str), \
			errdetail(__VA_ARGS__))

/*
 * Parse "<value> [± <unc>] [unit]", "(<value> ± <unc>) unit" or the concise
 * "<value>(<digits>)[e<exp>] [unit]" notation.  "+/-" and "+-" may be
 * used for "±".
 */
static bool
parse_measurement(const char *str, Measurement *out, Node *escontext)
{
	const char *p = str;
	bool		utf8 = db_is_utf8();
	bool		paren = false;
	char		mant[MAX_NUMBER_LEN + 1];
	char		cdigits[32];
	bool		concise = false;
	int			ndec,
				len,
				e10;
	double		v,
				u = 0.0;
	MeasUnit	unit;

	while (isspace((unsigned char) *p))
		p++;
	if (*p == '(')
	{
		paren = true;
		p++;
		while (isspace((unsigned char) *p))
			p++;
	}

	len = scan_mantissa(p, true, &ndec);
	if (len == 0)
		IN_FAIL("Expected a number at \"%s\".", p);
	if (len > MAX_NUMBER_LEN)
		IN_FAIL("Number has too many digits.");
	memcpy(mant, p, len);
	mant[len] = '\0';
	p += len;

	if (*p == '(' && isdigit((unsigned char) p[1]))
	{
		const char *q = p + 1;
		int			n = 0;

		while (isdigit((unsigned char) q[n]))
			n++;
		if (q[n] != ')')
			IN_FAIL("Concise uncertainty notation must be digits in parentheses, as in \"12.5(3)\".");
		if (n >= (int) sizeof(cdigits))
			IN_FAIL("Number has too many digits.");
		memcpy(cdigits, q, n);
		cdigits[n] = '\0';
		concise = true;
		p = q + n + 1;
	}

	len = scan_exponent(p, &e10);
	if (len < 0)
		IN_FAIL("Exponent is out of range.");
	p += len;
	if (!number_from_parts(mant, e10, &v))
		IN_FAIL("Value is out of range.");
	if (concise && !number_from_parts(cdigits, e10 - ndec, &u))
		IN_FAIL("Uncertainty is out of range.");

	while (isspace((unsigned char) *p))
		p++;

	len = 0;
	if (strncmp(p, "+/-", 3) == 0)
		len = 3;
	else if (strncmp(p, "+-", 2) == 0)
		len = 2;
	else if (utf8 && strncmp(p, "\xC2\xB1", 2) == 0)
		len = 2;
	if (len > 0)
	{
		if (concise)
			IN_FAIL("Concise and \"+/-\" uncertainty notations cannot be combined.");
		p += len;
		while (isspace((unsigned char) *p))
			p++;
		if (*p == '-')
			IN_FAIL("Uncertainty must not be negative.");
		if (*p == '+')
			p++;
		len = scan_mantissa(p, false, &ndec);
		if (len == 0)
			IN_FAIL("Expected an uncertainty value at \"%s\".", p);
		if (len > MAX_NUMBER_LEN)
			IN_FAIL("Number has too many digits.");
		memcpy(mant, p, len);
		mant[len] = '\0';
		p += len;
		len = scan_exponent(p, &e10);
		if (len < 0)
			IN_FAIL("Exponent is out of range.");
		p += len;
		if (!number_from_parts(mant, e10, &u))
			IN_FAIL("Uncertainty is out of range.");
		while (isspace((unsigned char) *p))
			p++;
	}

	if (paren)
	{
		if (*p != ')')
			IN_FAIL("Missing closing parenthesis.");
		p++;
	}

	if (!parse_unit_expr(p, &unit, str, true, escontext))
		return false;

	memset(out, 0, sizeof(Measurement));
	out->unit = unit;
	out->value = v * unit.scale + unit.offset;
	out->unc = u * unit.scale;
	if (!isfinite(out->value) || !isfinite(out->unc))
		IN_FAIL("Value is out of range.");
	if (out->value == 0.0)
		out->value = 0.0;
	return true;
}

static void
parse_unit_text(text *t, MeasUnit *u, char **cstr)
{
	char	   *s = text_to_cstring(t);

	(void) parse_unit_expr(s, u, s, false, NULL);
	if (cstr)
		*cstr = s;
}

static void
append_number(StringInfo buf, double x)
{
	char		tmp[64];

	if (x == 0.0)
		x = 0.0;
	snprintf(tmp, sizeof(tmp), "%.15g", x);
	appendStringInfoString(buf, tmp);
}

/* ----------------------------------------------------------------
 * I/O functions
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(measurement_in);
Datum
measurement_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	Measurement *m = palloc0(sizeof(Measurement));

	if (!parse_measurement(str, m, fcinfo->context))
		PG_RETURN_NULL();
	PG_RETURN_MEAS(m);
}

/*
 * Display value of a measurement.  For affine units the subtraction of the
 * offset cancels digits (0.1 degC = 273.25 K - 273.15 K), so round the
 * result to the resolution the SI value actually carries.
 */
static double
display_value(const Measurement *m)
{
	double		x = (m->value - m->unit.offset) / m->unit.scale;

	if (unit_is_affine(&m->unit) && fabs(x) < 1e15)
	{
		double		res = 4.0 * DBL_EPSILON *
			Max(fabs(m->value), fabs(m->unit.offset)) / m->unit.scale;
		int			dec = (int) floor(-log10(res));
		char		tmp[64];

		if (dec >= 0 && dec <= 17)
		{
			snprintf(tmp, sizeof(tmp), "%.*f", dec, x);
			x = strtod(tmp, NULL);
		}
	}
	return x;
}

PG_FUNCTION_INFO_V1(measurement_out);
Datum
measurement_out(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);
	StringInfoData buf;

	initStringInfo(&buf);
	append_number(&buf, display_value(m));
	appendStringInfoString(&buf, db_is_utf8() ? " \xC2\xB1 " : " +/- ");
	append_number(&buf, m->unc / m->unit.scale);
	if (m->unit.nfactors > 0)
	{
		appendStringInfoChar(&buf, ' ');
		render_unit(&buf, &m->unit);
	}
	PG_RETURN_CSTRING(buf.data);
}

/*
 * Binary format: float8 SI value, float8 SI uncertainty, uint8 factor
 * count, then per factor int16 unit id, int8 prefix id, int8 exponent.
 * Scale, offset and dimensions are recomputed and everything validated.
 */
PG_FUNCTION_INFO_V1(measurement_recv);
Datum
measurement_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	Measurement *m = palloc0(sizeof(Measurement));
	int			n;

	m->value = pq_getmsgfloat8(buf);
	m->unc = pq_getmsgfloat8(buf);
	n = pq_getmsgint(buf, 1);
	if (n > MEAS_MAX_FACTORS)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				errmsg("invalid number of unit factors in external \"measurement\" value"));
	for (int i = 0; i < n; i++)
	{
		int			unit = (int16) pq_getmsgint(buf, 2);
		int			prefix = (int8) pq_getmsgint(buf, 1);
		int			exp = (int8) pq_getmsgint(buf, 1);

		if (unit < 0 || unit >= N_UNITS || prefix < 0 || prefix >= N_PREFIXES ||
			(prefix != 0 && !unit_table[unit].prefixable) || exp == 0)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					errmsg("invalid unit factor in external \"measurement\" value"));
		for (int j = 0; j < m->unit.nfactors; j++)
			if (m->unit.f[j].unit == unit && m->unit.f[j].prefix == prefix)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						errmsg("duplicate unit factor in external \"measurement\" value"));
		(void) unit_add_factor(&m->unit, unit, prefix, exp);
	}
	(void) unit_finalize(&m->unit, NULL);
	if (!isfinite(m->value) || !isfinite(m->unc) || m->unc < 0.0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				errmsg("invalid value in external \"measurement\" value"));
	PG_RETURN_MEAS(check_result(m));
}

PG_FUNCTION_INFO_V1(measurement_send);
Datum
measurement_send(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendfloat8(&buf, m->value);
	pq_sendfloat8(&buf, m->unc);
	pq_sendint8(&buf, m->unit.nfactors);
	for (int i = 0; i < m->unit.nfactors; i++)
	{
		pq_sendint16(&buf, (uint16) m->unit.f[i].unit);
		pq_sendint8(&buf, (uint8) m->unit.f[i].prefix);
		pq_sendint8(&buf, (uint8) m->unit.f[i].exp);
	}
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------------
 * Constructors, accessors, conversions
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(measurement_make);
Datum
measurement_make(PG_FUNCTION_ARGS)
{
	double		v = PG_GETARG_FLOAT8(0);
	double		u = PG_GETARG_FLOAT8(1);
	Measurement *m = palloc0(sizeof(Measurement));

	check_float_arg(v, "measurement value");
	check_float_arg(u, "measurement uncertainty");
	if (u < 0.0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("measurement uncertainty must not be negative"));
	parse_unit_text(PG_GETARG_TEXT_PP(2), &m->unit, NULL);
	m->value = v * m->unit.scale + m->unit.offset;
	m->unc = u * m->unit.scale;
	PG_RETURN_MEAS(check_result(m));
}

PG_FUNCTION_INFO_V1(measurement_from_float8);
Datum
measurement_from_float8(PG_FUNCTION_ARGS)
{
	double		v = PG_GETARG_FLOAT8(0);
	Measurement *m = palloc0(sizeof(Measurement));

	check_float_arg(v, "measurement value");
	(void) unit_finalize(&m->unit, NULL);
	m->value = v;
	PG_RETURN_MEAS(check_result(m));
}

PG_FUNCTION_INFO_V1(measurement_to_float8);
Datum
measurement_to_float8(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(PG_GETARG_MEAS(0)->value);
}

/* m_value(m, unit DEFAULT NULL) -- not strict */
PG_FUNCTION_INFO_V1(m_value);
Datum
m_value(PG_FUNCTION_ARGS)
{
	Measurement *m;
	MeasUnit	u;
	char	   *s;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	m = PG_GETARG_MEAS(0);
	if (PG_ARGISNULL(1))
		PG_RETURN_FLOAT8((m->value - m->unit.offset) / m->unit.scale);
	parse_unit_text(PG_GETARG_TEXT_PP(1), &u, &s);
	check_unit_dim(m, &u, s);
	PG_RETURN_FLOAT8((m->value - u.offset) / u.scale);
}

/* m_uncertainty(m, unit DEFAULT NULL) -- not strict */
PG_FUNCTION_INFO_V1(m_uncertainty);
Datum
m_uncertainty(PG_FUNCTION_ARGS)
{
	Measurement *m;
	MeasUnit	u;
	char	   *s;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	m = PG_GETARG_MEAS(0);
	if (PG_ARGISNULL(1))
		PG_RETURN_FLOAT8(m->unc / m->unit.scale);
	parse_unit_text(PG_GETARG_TEXT_PP(1), &u, &s);
	check_unit_dim(m, &u, s);
	PG_RETURN_FLOAT8(m->unc / u.scale);
}

PG_FUNCTION_INFO_V1(m_relative_uncertainty);
Datum
m_relative_uncertainty(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);

	if (m->value == 0.0)
		PG_RETURN_NULL();
	PG_RETURN_FLOAT8(m->unc / fabs(m->value));
}

PG_FUNCTION_INFO_V1(m_unit);
Datum
m_unit(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);
	StringInfoData buf;

	initStringInfo(&buf);
	render_unit(&buf, &m->unit);
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

PG_FUNCTION_INFO_V1(m_dimension);
Datum
m_dimension(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);

	PG_RETURN_TEXT_P(cstring_to_text(dimension_string(m->unit.dim)));
}

PG_FUNCTION_INFO_V1(m_convert);
Datum
m_convert(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);
	Measurement *r = palloc0(sizeof(Measurement));
	char	   *s;

	parse_unit_text(PG_GETARG_TEXT_PP(1), &r->unit, &s);
	check_unit_dim(m, &r->unit, s);
	r->value = m->value;
	r->unc = m->unc;
	PG_RETURN_MEAS(check_result(r));
}

static double
coverage_factor(double k)
{
	check_float_arg(k, "coverage factor");
	if (k < 0.0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("coverage factor must not be negative"));
	return k;
}

PG_FUNCTION_INFO_V1(m_interval_lower);
Datum
m_interval_lower(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);
	double		k = coverage_factor(PG_GETARG_FLOAT8(1));

	PG_RETURN_FLOAT8((m->value - k * m->unc - m->unit.offset) / m->unit.scale);
}

PG_FUNCTION_INFO_V1(m_interval_upper);
Datum
m_interval_upper(PG_FUNCTION_ARGS)
{
	Measurement *m = PG_GETARG_MEAS(0);
	double		k = coverage_factor(PG_GETARG_FLOAT8(1));

	PG_RETURN_FLOAT8((m->value + k * m->unc - m->unit.offset) / m->unit.scale);
}

PG_FUNCTION_INFO_V1(m_compatible);
Datum
m_compatible(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);

	PG_RETURN_BOOL(memcmp(a->unit.dim, b->unit.dim, NDIM) == 0);
}

PG_FUNCTION_INFO_V1(m_consistent);
Datum
m_consistent(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);
	double		k = coverage_factor(PG_GETARG_FLOAT8(2));

	check_same_dim(a, b, "m_consistent");
	PG_RETURN_BOOL(fabs(a->value - b->value) <= k * hypot(a->unc, b->unc));
}

/* ----------------------------------------------------------------
 * Arithmetic (GUM first-order propagation, uncorrelated inputs)
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(measurement_add);
Datum
measurement_add(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);
	Measurement *r = palloc0(sizeof(Measurement));

	check_same_dim(a, b, "addition");
	unit_for_linear_result(&a->unit, &r->unit);
	r->value = a->value + b->value;
	r->unc = hypot(a->unc, b->unc);
	PG_RETURN_MEAS(check_result(r));
}

PG_FUNCTION_INFO_V1(measurement_sub);
Datum
measurement_sub(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);
	Measurement *r = palloc0(sizeof(Measurement));

	check_same_dim(a, b, "subtraction");
	unit_for_linear_result(&a->unit, &r->unit);
	r->value = a->value - b->value;
	r->unc = hypot(a->unc, b->unc);
	PG_RETURN_MEAS(check_result(r));
}

PG_FUNCTION_INFO_V1(measurement_neg);
Datum
measurement_neg(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *r = palloc0(sizeof(Measurement));

	unit_for_linear_result(&a->unit, &r->unit);
	r->value = -a->value;
	r->unc = a->unc;
	PG_RETURN_MEAS(check_result(r));
}

/* Display unit of a product (bsign = 1) or quotient (bsign = -1). */
static void
unit_combine(const MeasUnit *a, const MeasUnit *b, int bsign, MeasUnit *out)
{
	MeasUnit	ta,
				tb;
	bool		ok = true;

	unit_for_linear_result(a, &ta);
	unit_for_linear_result(b, &tb);
	*out = ta;
	for (int i = 0; i < tb.nfactors && ok; i++)
		ok = unit_add_factor(out, tb.f[i].unit, tb.f[i].prefix,
							 bsign * tb.f[i].exp);
	if (ok)
	{
		(void) unit_finalize(out, NULL);
		return;
	}
	else
	{
		int8		dim[NDIM];

		for (int d = 0; d < NDIM; d++)
		{
			int			x = a->dim[d] + bsign * b->dim[d];

			if (x < PG_INT8_MIN || x > PG_INT8_MAX)
				ereport(ERROR,
						errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("measurement dimension exponent out of range"));
			dim[d] = (int8) x;
		}
		unit_set_si(out, dim);
	}
}

PG_FUNCTION_INFO_V1(measurement_mul);
Datum
measurement_mul(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);
	Measurement *r = palloc0(sizeof(Measurement));

	unit_combine(&a->unit, &b->unit, 1, &r->unit);
	r->value = a->value * b->value;
	/* u^2 = (b*ua)^2 + (a*ub)^2, i.e. relative uncertainties in quadrature */
	r->unc = hypot(b->value * a->unc, a->value * b->unc);
	PG_RETURN_MEAS(check_result(r));
}

PG_FUNCTION_INFO_V1(measurement_div);
Datum
measurement_div(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);
	Measurement *r = palloc0(sizeof(Measurement));

	if (b->value == 0.0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));
	unit_combine(&a->unit, &b->unit, -1, &r->unit);
	r->value = a->value / b->value;
	/* u^2 = (ua/b)^2 + (a*ub/b^2)^2 */
	r->unc = hypot(a->unc, r->value * b->unc) / fabs(b->value);
	PG_RETURN_MEAS(check_result(r));
}

static Datum
scale_measurement(Measurement *a, double f)
{
	Measurement *r = palloc0(sizeof(Measurement));

	check_float_arg(f, "scale factor");
	unit_for_linear_result(&a->unit, &r->unit);
	r->value = a->value * f;
	r->unc = a->unc * fabs(f);
	PG_RETURN_MEAS(check_result(r));
}

PG_FUNCTION_INFO_V1(measurement_mul_float8);
Datum
measurement_mul_float8(PG_FUNCTION_ARGS)
{
	return scale_measurement(PG_GETARG_MEAS(0), PG_GETARG_FLOAT8(1));
}

PG_FUNCTION_INFO_V1(float8_mul_measurement);
Datum
float8_mul_measurement(PG_FUNCTION_ARGS)
{
	return scale_measurement(PG_GETARG_MEAS(1), PG_GETARG_FLOAT8(0));
}

PG_FUNCTION_INFO_V1(measurement_div_float8);
Datum
measurement_div_float8(PG_FUNCTION_ARGS)
{
	double		f = PG_GETARG_FLOAT8(1);

	if (f == 0.0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));
	check_float_arg(f, "divisor");
	return scale_measurement(PG_GETARG_MEAS(0), 1.0 / f);
}

static Measurement *
meas_pow(const Measurement *a, double p)
{
	Measurement *r = palloc0(sizeof(Measurement));
	MeasUnit	base;
	int8		dim[NDIM];
	bool		factors_ok = true;
	double		x = a->value;

	check_float_arg(p, "exponent");

	/* resulting dimensions must be integral */
	for (int d = 0; d < NDIM; d++)
	{
		double		nd = a->unit.dim[d] * p;

		if (nd != rint(nd))
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("raising a measurement of dimension %s to the power %g gives a fractional dimension",
						   dimension_string(a->unit.dim), p));
		if (nd < PG_INT8_MIN || nd > PG_INT8_MAX)
			ereport(ERROR,
					errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					errmsg("measurement dimension exponent out of range"));
		dim[d] = (int8) nd;
	}

	/* keep the display unit if all factor exponents stay integral */
	unit_for_linear_result(&a->unit, &base);
	for (int i = 0; i < base.nfactors; i++)
	{
		double		ne = base.f[i].exp * p;

		if (ne != rint(ne) || ne < PG_INT8_MIN || ne > PG_INT8_MAX)
			factors_ok = false;
	}
	if (factors_ok)
	{
		for (int i = 0; i < base.nfactors; i++)
		{
			int			ne = (int) rint(base.f[i].exp * p);

			(void) unit_add_factor(&r->unit, base.f[i].unit,
								   base.f[i].prefix, ne);
		}
		(void) unit_finalize(&r->unit, NULL);
	}
	else
		unit_set_si(&r->unit, dim);

	if (x < 0.0 && p != rint(p))
		ereport(ERROR,
				errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				errmsg("a negative measurement cannot be raised to a non-integer power"));
	if (x == 0.0 && p < 0.0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("zero raised to a negative power is undefined"));

	r->value = pow(x, p);
	if (a->unc == 0.0 || p == 0.0)
		r->unc = 0.0;
	else if (x == 0.0)
	{
		if (p > 1.0)
			r->unc = 0.0;		/* first-order sensitivity is zero */
		else if (p == 1.0)
			r->unc = a->unc;
		else
			ereport(ERROR,
					errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					errmsg("uncertainty propagation is undefined for a zero value raised to a power below 1"));
	}
	else
		r->unc = fabs(p * r->value / x) * a->unc;	/* |p| u/|x| relative */
	return check_result(r);
}

PG_FUNCTION_INFO_V1(m_pow);
Datum
m_pow(PG_FUNCTION_ARGS)
{
	PG_RETURN_MEAS(meas_pow(PG_GETARG_MEAS(0), PG_GETARG_FLOAT8(1)));
}

PG_FUNCTION_INFO_V1(m_sqrt);
Datum
m_sqrt(PG_FUNCTION_ARGS)
{
	PG_RETURN_MEAS(meas_pow(PG_GETARG_MEAS(0), 0.5));
}

static void
require_dimensionless(const Measurement *a, const char *fname)
{
	for (int d = 0; d < NDIM; d++)
		if (a->unit.dim[d] != 0)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("%s requires a dimensionless measurement, got dimension %s",
						   fname, dimension_string(a->unit.dim)));
}

PG_FUNCTION_INFO_V1(m_exp);
Datum
m_exp(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *r = palloc0(sizeof(Measurement));

	require_dimensionless(a, "m_exp");
	(void) unit_finalize(&r->unit, NULL);
	r->value = exp(a->value);
	r->unc = r->value * a->unc;
	PG_RETURN_MEAS(check_result(r));
}

PG_FUNCTION_INFO_V1(m_ln);
Datum
m_ln(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *r = palloc0(sizeof(Measurement));

	require_dimensionless(a, "m_ln");
	if (a->value <= 0.0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				errmsg("cannot take logarithm of a non-positive measurement"));
	(void) unit_finalize(&r->unit, NULL);
	r->value = log(a->value);
	r->unc = a->unc / a->value;
	PG_RETURN_MEAS(check_result(r));
}

/* ----------------------------------------------------------------
 * Comparison / btree support (compares SI values; uncertainty ignored)
 * ----------------------------------------------------------------
 */
static int
meas_cmp(const Measurement *a, const Measurement *b)
{
	check_same_dim(a, b, "comparison");
	if (a->value < b->value)
		return -1;
	if (a->value > b->value)
		return 1;
	return 0;
}

#define MEAS_CMP_FUNC(name, expr) \
PG_FUNCTION_INFO_V1(name); \
Datum \
name(PG_FUNCTION_ARGS) \
{ \
	int			c = meas_cmp(PG_GETARG_MEAS(0), PG_GETARG_MEAS(1)); \
	PG_RETURN_BOOL(expr); \
}

MEAS_CMP_FUNC(measurement_eq, c == 0)
MEAS_CMP_FUNC(measurement_ne, c != 0)
MEAS_CMP_FUNC(measurement_lt, c < 0)
MEAS_CMP_FUNC(measurement_le, c <= 0)
MEAS_CMP_FUNC(measurement_gt, c > 0)
MEAS_CMP_FUNC(measurement_ge, c >= 0)

PG_FUNCTION_INFO_V1(measurement_larger);
Datum
measurement_larger(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);

	PG_RETURN_MEAS(meas_cmp(a, b) >= 0 ? a : b);
}

PG_FUNCTION_INFO_V1(measurement_smaller);
Datum
measurement_smaller(PG_FUNCTION_ARGS)
{
	Measurement *a = PG_GETARG_MEAS(0);
	Measurement *b = PG_GETARG_MEAS(1);

	PG_RETURN_MEAS(meas_cmp(a, b) <= 0 ? a : b);
}

PG_FUNCTION_INFO_V1(measurement_cmp);
Datum
measurement_cmp(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(meas_cmp(PG_GETARG_MEAS(0), PG_GETARG_MEAS(1)));
}

/* ----------------------------------------------------------------
 * Aggregates m_mean / m_wmean
 * ----------------------------------------------------------------
 */
typedef struct MeasAggState
{
	int64		n;
	double		sum;			/* sum of values */
	double		sumu2;			/* sum of squared uncertainties */
	double		sumw;			/* sum of 1/u^2 */
	double		sumwv;			/* sum of v/u^2 */
	bool		anyzero;		/* some input had u = 0 */
	Measurement proto;			/* first input: dimension + display unit */
} MeasAggState;

PG_FUNCTION_INFO_V1(m_agg_trans);
Datum
m_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MeasAggState *st = PG_ARGISNULL(0) ? NULL : (MeasAggState *) PG_GETARG_POINTER(0);
	Measurement *m;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "m_agg_trans called in non-aggregate context");
	if (PG_ARGISNULL(1))
	{
		if (st == NULL)
			PG_RETURN_NULL();
		PG_RETURN_POINTER(st);
	}
	m = PG_GETARG_MEAS(1);
	if (st == NULL)
	{
		st = (MeasAggState *) MemoryContextAllocZero(aggcontext, sizeof(MeasAggState));
		st->proto = *m;
	}
	else
		check_same_dim(&st->proto, m, "aggregate");

	st->n++;
	st->sum += m->value;
	st->sumu2 += m->unc * m->unc;
	if (m->unc == 0.0 || !isfinite(1.0 / (m->unc * m->unc)))
		st->anyzero = true;
	else
	{
		double		w = 1.0 / (m->unc * m->unc);

		st->sumw += w;
		st->sumwv += w * m->value;
	}
	PG_RETURN_POINTER(st);
}

static Measurement *
agg_plain_mean(const MeasAggState *st)
{
	Measurement *r = palloc0(sizeof(Measurement));

	r->unit = st->proto.unit;
	r->value = st->sum / st->n;
	r->unc = sqrt(st->sumu2) / st->n;
	return check_result(r);
}

PG_FUNCTION_INFO_V1(m_mean_final);
Datum
m_mean_final(PG_FUNCTION_ARGS)
{
	MeasAggState *st = (MeasAggState *) PG_GETARG_POINTER(0);

	if (st == NULL || st->n == 0)
		PG_RETURN_NULL();
	PG_RETURN_MEAS(agg_plain_mean(st));
}

PG_FUNCTION_INFO_V1(m_wmean_final);
Datum
m_wmean_final(PG_FUNCTION_ARGS)
{
	MeasAggState *st = (MeasAggState *) PG_GETARG_POINTER(0);
	Measurement *r;

	if (st == NULL || st->n == 0)
		PG_RETURN_NULL();
	if (st->anyzero)
		PG_RETURN_MEAS(agg_plain_mean(st));
	r = palloc0(sizeof(Measurement));
	r->unit = st->proto.unit;
	r->value = st->sumwv / st->sumw;
	r->unc = 1.0 / sqrt(st->sumw);
	PG_RETURN_MEAS(check_result(r));
}
