/*-------------------------------------------------------------------------
 *
 * cron_parser.c
 *	  Cron expression parser for the alohadb_cron extension.
 *
 *	  Parses standard 5-field cron expressions into a CronSchedule
 *	  structure and computes the next matching timestamp from a given
 *	  starting point.
 *
 *	  Supported syntax per field:
 *	    *         - all values
 *	    N         - specific value (e.g. 5)
 *	    N-M       - range (e.g. 1-5)
 *	    * /S       - step from 0 (e.g. * /10 means 0,10,20,30,40,50)
 *	    N-M/S     - step within range
 *	    A,B,C     - list (e.g. 1,3,5)
 *
 *	  Fields (left to right):
 *	    minute (0-59), hour (0-23), day-of-month (1-31),
 *	    month (1-12), day-of-week (0-7, where 0 and 7 = Sunday)
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cron/cron_parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <string.h>

#include "pgtime.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

#include "alohadb_cron.h"

/* ----------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------- */

/*
 * parse_number
 *
 * Read a non-negative integer from *p, advancing *p past the digits.
 * Returns false if no digits are found.
 */
static bool
parse_number(const char **p, int *result)
{
	const char *start = *p;
	int			val = 0;

	while (isdigit((unsigned char) **p))
	{
		val = val * 10 + (**p - '0');
		(*p)++;
	}

	if (*p == start)
		return false;

	*result = val;
	return true;
}

/*
 * set_range
 *
 * Set bits in a bool array from lo to hi (inclusive) with the given step.
 * The array is 0-indexed; offset adjusts the logical values (e.g. for
 * day-of-month where 1 maps to index 0, offset = 1).
 */
static bool
set_range(bool *arr, int arr_size, int lo, int hi, int step, int offset)
{
	int			i;

	lo -= offset;
	hi -= offset;

	if (lo < 0 || hi < 0 || lo >= arr_size || hi >= arr_size || lo > hi || step < 1)
		return false;

	for (i = lo; i <= hi; i += step)
		arr[i] = true;

	return true;
}

/*
 * parse_field
 *
 * Parse one cron field (a comma-separated list of elements).
 * Each element may be: *, N, N-M, * /S, N-M/S, or N/S.
 *
 * arr       - bool array to fill
 * arr_size  - size of the array
 * min_val   - minimum logical value (0 for minute/hour/dow, 1 for dom/month)
 * max_val   - maximum logical value (inclusive)
 * field_str - the raw field string
 */
static bool
parse_field(bool *arr, int arr_size, int min_val, int max_val,
			const char *field_str)
{
	const char *p = field_str;
	int			offset = min_val;	/* logical-to-index offset */

	/* Clear the array */
	memset(arr, 0, sizeof(bool) * arr_size);

	while (*p != '\0')
	{
		int		lo, hi, step;
		bool	is_star = false;

		/* Skip leading whitespace */
		while (isspace((unsigned char) *p))
			p++;

		if (*p == '*')
		{
			is_star = true;
			lo = min_val;
			hi = max_val;
			p++;
		}
		else
		{
			if (!parse_number(&p, &lo))
				return false;

			if (*p == '-')
			{
				p++;	/* skip '-' */
				if (!parse_number(&p, &hi))
					return false;
			}
			else
			{
				hi = lo;
			}
		}

		/* Check for step */
		step = 1;
		if (*p == '/')
		{
			p++;	/* skip '/' */
			if (!parse_number(&p, &step))
				return false;
			if (step < 1)
				return false;
			/* If it was a single number with step (e.g. "5/10"), extend hi */
			if (!is_star && lo == hi)
				hi = max_val;
		}

		/* Validate range */
		if (lo < min_val || hi > max_val || lo > hi)
			return false;

		if (!set_range(arr, arr_size, lo, hi, step, offset))
			return false;

		/* Next element in comma list */
		while (isspace((unsigned char) *p))
			p++;

		if (*p == ',')
		{
			p++;
			continue;
		}

		if (*p != '\0')
			return false;		/* unexpected character */
	}

	return true;
}

/* ----------------------------------------------------------------
 * Public functions
 * ---------------------------------------------------------------- */

/*
 * cron_parse
 *
 * Parse a 5-field cron expression into a CronSchedule struct.
 * Returns true on success, false on parse error.
 *
 * Format: "minute hour day-of-month month day-of-week"
 * Example: "0/5 * * * *" (every 5 minutes)
 */
bool
cron_parse(const char *expr, CronSchedule *sched)
{
	char		buf[256];
	char	   *fields[5];
	int			nfields = 0;
	char	   *p;
	char	   *token;

	if (expr == NULL || sched == NULL)
		return false;

	/* Work on a copy */
	strlcpy(buf, expr, sizeof(buf));

	/* Tokenize on whitespace */
	p = buf;
	while (nfields < 5 && (token = strsep(&p, " \t")) != NULL)
	{
		if (*token == '\0')
			continue;			/* skip multiple delimiters */
		fields[nfields++] = token;
	}

	if (nfields != 5)
		return false;

	/* Parse each field */
	if (!parse_field(sched->minute, 60, 0, 59, fields[0]))
		return false;
	if (!parse_field(sched->hour, 24, 0, 23, fields[1]))
		return false;
	if (!parse_field(sched->dom, 31, 1, 31, fields[2]))
		return false;
	if (!parse_field(sched->month, 12, 1, 12, fields[3]))
		return false;
	if (!parse_field(sched->dow, 7, 0, 6, fields[4]))
		return false;

	/*
	 * Normalize day-of-week: if 7 was specified (some cron implementations
	 * allow 0 and 7 for Sunday), we need to handle it.  Since our dow array
	 * is only 7 elements (0-6), value 7 maps to index 7 which is out of
	 * bounds in parse_field.  We handle this by re-parsing the dow field
	 * specially if the expression contains '7'.
	 *
	 * Actually, parse_field for dow uses max_val=6, so '7' alone would fail.
	 * We handle the "0 and 7 both mean Sunday" by pre-processing.
	 */
	{
		/*
		 * Re-check: if the original dow field contained '7', replace with '0'
		 * and re-parse.  This is a simple approach.
		 */
		char		dow_buf[64];
		bool		has_seven = false;
		const char *q;

		strlcpy(dow_buf, fields[4], sizeof(dow_buf));

		/* Check if '7' appears as a value (not part of a larger number) */
		for (q = dow_buf; *q != '\0'; q++)
		{
			if (*q == '7')
			{
				/* Check it's not part of a multi-digit number */
				bool	prev_digit = (q > dow_buf && isdigit((unsigned char) *(q - 1)));
				bool	next_digit = isdigit((unsigned char) *(q + 1));

				if (!prev_digit && !next_digit)
				{
					has_seven = true;
					break;
				}
			}
		}

		if (has_seven)
		{
			/* Replace standalone '7' with '0' */
			char	fixed[64];
			char   *dst = fixed;
			const char *src = dow_buf;

			while (*src != '\0')
			{
				if (*src == '7')
				{
					bool	prev_d = (src > dow_buf && isdigit((unsigned char) *(src - 1)));
					bool	next_d = isdigit((unsigned char) *(src + 1));

					if (!prev_d && !next_d)
					{
						*dst++ = '0';
						src++;
						continue;
					}
				}
				*dst++ = *src++;
			}
			*dst = '\0';

			/* Re-parse with the fixed string */
			if (!parse_field(sched->dow, 7, 0, 6, fixed))
				return false;
		}
	}

	return true;
}

/*
 * cron_next_run
 *
 * Given a CronSchedule and a starting timestamp, compute the next
 * timestamp that matches the schedule.  Starts from 'from + 1 minute'
 * (with seconds/microseconds zeroed) and iterates forward.
 *
 * Returns InvalidTimestampTz if no match is found within CRON_MAX_ITERATIONS
 * minutes (approximately 2 years).
 */
TimestampTz
cron_next_run(const CronSchedule *sched, TimestampTz from)
{
	struct pg_tm tm;
	fsec_t		fsec;
	int			tz;
	int			iterations;
	Timestamp	ts;

	if (sched == NULL)
		return DT_NOBEGIN;

	/* Decompose the starting timestamp */
	if (timestamp2tm(from, &tz, &tm, &fsec, NULL, session_timezone) != 0)
		return DT_NOBEGIN;

	/* Advance to the next whole minute */
	tm.tm_sec = 0;
	fsec = 0;
	tm.tm_min += 1;

	/* Normalize: carry overflow from minutes to hours, etc. */
	if (tm.tm_min >= 60)
	{
		tm.tm_min = 0;
		tm.tm_hour += 1;
	}
	if (tm.tm_hour >= 24)
	{
		tm.tm_hour = 0;
		tm.tm_mday += 1;
	}

	/*
	 * We don't normalize mday overflow here; the iteration loop below
	 * handles it by reconstructing via tm2timestamp + timestamp2tm.
	 */

	for (iterations = 0; iterations < CRON_MAX_ITERATIONS; iterations++)
	{
		int		wday;
		int		tz_offset;

		/* Check month (1-12, index = month - 1) */
		if (!sched->month[tm.tm_mon - 1])
		{
			/* Advance to next month */
			tm.tm_mon += 1;
			tm.tm_mday = 1;
			tm.tm_hour = 0;
			tm.tm_min = 0;

			if (tm.tm_mon > 12)
			{
				tm.tm_mon = 1;
				tm.tm_year += 1;
			}
			continue;
		}

		/* Check day-of-month (1-31, index = mday - 1) */
		if (!sched->dom[tm.tm_mday - 1])
		{
			tm.tm_mday += 1;
			tm.tm_hour = 0;
			tm.tm_min = 0;

			/* Handle month overflow via rebuild */
			tz_offset = DetermineTimeZoneOffset(&tm, session_timezone);
			if (tm2timestamp(&tm, fsec, &tz_offset, &ts) != 0)
				return DT_NOBEGIN;
			if (timestamp2tm(ts, &tz_offset, &tm, &fsec, NULL, session_timezone) != 0)
				return DT_NOBEGIN;
			continue;
		}

		/* Check day-of-week */
		/*
		 * Compute the day of week for the current date.
		 * Use j2day() which returns 0=Sunday..6=Saturday (matching cron convention).
		 * We need to compute the Julian day number first.
		 */
		tz_offset = DetermineTimeZoneOffset(&tm, session_timezone);
		if (tm2timestamp(&tm, 0, &tz_offset, &ts) != 0)
			return DT_NOBEGIN;
		if (timestamp2tm(ts, &tz_offset, &tm, &fsec, NULL, session_timezone) != 0)
			return DT_NOBEGIN;

		/* pg_tm.tm_wday: 0 = Sunday, matching cron convention */
		wday = tm.tm_wday;
		if (wday < 0 || wday > 6)
			wday = 0;

		if (!sched->dow[wday])
		{
			tm.tm_mday += 1;
			tm.tm_hour = 0;
			tm.tm_min = 0;

			tz_offset = DetermineTimeZoneOffset(&tm, session_timezone);
			if (tm2timestamp(&tm, 0, &tz_offset, &ts) != 0)
				return DT_NOBEGIN;
			if (timestamp2tm(ts, &tz_offset, &tm, &fsec, NULL, session_timezone) != 0)
				return DT_NOBEGIN;
			continue;
		}

		/* Check hour (0-23) */
		if (!sched->hour[tm.tm_hour])
		{
			tm.tm_hour += 1;
			tm.tm_min = 0;

			if (tm.tm_hour >= 24)
			{
				tm.tm_hour = 0;
				tm.tm_mday += 1;

				tz_offset = DetermineTimeZoneOffset(&tm, session_timezone);
				if (tm2timestamp(&tm, 0, &tz_offset, &ts) != 0)
					return DT_NOBEGIN;
				if (timestamp2tm(ts, &tz_offset, &tm, &fsec, NULL, session_timezone) != 0)
					return DT_NOBEGIN;
			}
			continue;
		}

		/* Check minute (0-59) */
		if (!sched->minute[tm.tm_min])
		{
			tm.tm_min += 1;

			if (tm.tm_min >= 60)
			{
				tm.tm_min = 0;
				tm.tm_hour += 1;

				if (tm.tm_hour >= 24)
				{
					tm.tm_hour = 0;
					tm.tm_mday += 1;

					tz_offset = DetermineTimeZoneOffset(&tm, session_timezone);
					if (tm2timestamp(&tm, 0, &tz_offset, &ts) != 0)
						return DT_NOBEGIN;
					if (timestamp2tm(ts, &tz_offset, &tm, &fsec, NULL, session_timezone) != 0)
						return DT_NOBEGIN;
				}
			}
			continue;
		}

		/*
		 * All fields match.  Reconstruct the timestamp.
		 */
		tm.tm_sec = 0;
		fsec = 0;
		tz_offset = DetermineTimeZoneOffset(&tm, session_timezone);
		if (tm2timestamp(&tm, 0, &tz_offset, &ts) != 0)
			return DT_NOBEGIN;

		return (TimestampTz) ts;
	}

	/* No match found within iteration limit */
	return DT_NOBEGIN;
}
