/*-------------------------------------------------------------------------
 *
 * alohadb_chart.c
 *    SVG chart generation from SQL query results.
 *
 *    Renders bar, line, pie, scatter, and area charts as inline SVG text
 *    from arbitrary SELECT statements. No external dependencies.
 *
 *    Prior art: gnuplot (1986), GD library (1994), SVG W3C standard (2001),
 *    Oracle APEX charting (2004), Grafana (2014), pgSVG, pg_svg.
 *
 * Copyright (c) 2026, OpenCAN / AlohaDB
 *
 * IDENTIFICATION
 *    contrib/alohadb_chart/alohadb_chart.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <float.h>

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC_EXT(
	.name = "alohadb_chart",
	.version = "1.0"
);

PG_FUNCTION_INFO_V1(chart_bar);
PG_FUNCTION_INFO_V1(chart_line);
PG_FUNCTION_INFO_V1(chart_pie);
PG_FUNCTION_INFO_V1(chart_scatter);
PG_FUNCTION_INFO_V1(chart_area);

/* ----------------------------------------------------------------
 * Color palette (12 distinct colors, colorblind-friendly Tableau 10+2)
 * ---------------------------------------------------------------- */
static const char *palette[] = {
	"#4e79a7", "#f28e2b", "#e15759", "#76b7b2",
	"#59a14f", "#edc948", "#b07aa1", "#ff9da7",
	"#9c755f", "#bab0ac", "#6b6ecf", "#b5cf6b"
};
#define PALETTE_SIZE 12

/* ----------------------------------------------------------------
 * SVG dimension defaults
 * ---------------------------------------------------------------- */
#define DEFAULT_WIDTH   800
#define DEFAULT_HEIGHT  400
#define MARGIN_TOP       40
#define MARGIN_RIGHT     20
#define MARGIN_BOTTOM    60
#define MARGIN_LEFT      70
#define PIE_RADIUS      150
#define LEGEND_BOX       12

/* ----------------------------------------------------------------
 * Helper: XML-escape a string into a StringInfo
 * ---------------------------------------------------------------- */
static void
svg_escape(StringInfo buf, const char *str)
{
	const char *p;

	if (str == NULL)
		return;
	for (p = str; *p; p++)
	{
		switch (*p)
		{
			case '<':  appendStringInfoString(buf, "&lt;"); break;
			case '>':  appendStringInfoString(buf, "&gt;"); break;
			case '&':  appendStringInfoString(buf, "&amp;"); break;
			case '"':  appendStringInfoString(buf, "&quot;"); break;
			case '\'': appendStringInfoString(buf, "&apos;"); break;
			default:   appendStringInfoChar(buf, *p); break;
		}
	}
}

/* ----------------------------------------------------------------
 * Helper: parse a numeric SPI value to double
 * ---------------------------------------------------------------- */
static double
get_spi_double(int row, int col)
{
	char   *val;

	val = SPI_getvalue(SPI_tuptable->vals[row],
					   SPI_tuptable->tupdesc, col + 1);
	if (val == NULL)
		return 0.0;

	return atof(val);
}

/* ----------------------------------------------------------------
 * Helper: get SPI string value (palloc'd copy)
 * ---------------------------------------------------------------- */
static char *
get_spi_string(int row, int col)
{
	char   *val;

	val = SPI_getvalue(SPI_tuptable->vals[row],
					   SPI_tuptable->tupdesc, col + 1);
	if (val == NULL)
		return pstrdup("");
	return pstrdup(val);
}

/* ----------------------------------------------------------------
 * Helper: format a number for axis labels (trim trailing zeros)
 * ---------------------------------------------------------------- */
static void
format_number(char *buf, size_t buflen, double val)
{
	if (fabs(val) >= 1e6)
		snprintf(buf, buflen, "%.1fM", val / 1e6);
	else if (fabs(val) >= 1e3)
		snprintf(buf, buflen, "%.1fK", val / 1e3);
	else if (val == floor(val))
		snprintf(buf, buflen, "%d", (int) val);
	else
		snprintf(buf, buflen, "%.1f", val);
}

/* ----------------------------------------------------------------
 * Helper: SVG header
 * ---------------------------------------------------------------- */
static void
svg_header(StringInfo buf, int width, int height, const char *title)
{
	appendStringInfo(buf,
		"<svg xmlns=\"http://www.w3.org/2000/svg\" "
		"viewBox=\"0 0 %d %d\" width=\"%d\" height=\"%d\" "
		"style=\"font-family:'Segoe UI',system-ui,sans-serif;background:#fff\">\n",
		width, height, width, height);

	if (title && title[0])
	{
		appendStringInfoString(buf,
			"<text x=\"50%\" y=\"24\" text-anchor=\"middle\" "
			"font-size=\"16\" font-weight=\"bold\" fill=\"#333\">");
		svg_escape(buf, title);
		appendStringInfoString(buf, "</text>\n");
	}
}

/* ----------------------------------------------------------------
 * Helper: Y-axis with grid lines
 * ---------------------------------------------------------------- */
static void
svg_y_axis(StringInfo buf, double min_val, double max_val,
		   int plot_x, int plot_y, int plot_w, int plot_h, int ticks)
{
	int		i;
	double	range = max_val - min_val;
	char	label[32];

	/* Axis line */
	appendStringInfo(buf,
		"<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" "
		"stroke=\"#999\" stroke-width=\"1\"/>\n",
		plot_x, plot_y, plot_x, plot_y + plot_h);

	for (i = 0; i <= ticks; i++)
	{
		double	frac = (double) i / ticks;
		int		y = plot_y + plot_h - (int)(frac * plot_h);
		double	val = min_val + frac * range;

		format_number(label, sizeof(label), val);

		/* Grid line */
		appendStringInfo(buf,
			"<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" "
			"stroke=\"#e8e8e8\" stroke-width=\"1\"/>\n",
			plot_x, y, plot_x + plot_w, y);

		/* Tick label */
		appendStringInfo(buf,
			"<text x=\"%d\" y=\"%d\" text-anchor=\"end\" "
			"font-size=\"11\" fill=\"#666\">%s</text>\n",
			plot_x - 8, y + 4, label);
	}
}

/* ----------------------------------------------------------------
 * Helper: X-axis labels (rotated if needed)
 * ---------------------------------------------------------------- */
static void
svg_x_labels(StringInfo buf, char **labels, int count,
			 int plot_x, int plot_y, int plot_w, int plot_h,
			 bool center)
{
	int		i;
	int		max_labels = (plot_w / 40);	/* don't overcrowd */
	int		step = (count > max_labels) ? (count / max_labels + 1) : 1;

	/* Axis line */
	appendStringInfo(buf,
		"<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" "
		"stroke=\"#999\" stroke-width=\"1\"/>\n",
		plot_x, plot_y + plot_h, plot_x + plot_w, plot_y + plot_h);

	for (i = 0; i < count; i += step)
	{
		int		x;
		double	bar_w = (double) plot_w / count;

		if (center)
			x = plot_x + (int)(i * bar_w + bar_w / 2);
		else
			x = plot_x + (int)((double) i / (count - 1) * plot_w);

		appendStringInfo(buf,
			"<text x=\"%d\" y=\"%d\" text-anchor=\"end\" "
			"font-size=\"10\" fill=\"#666\" "
			"transform=\"rotate(-35 %d %d)\">",
			x, plot_y + plot_h + 14, x, plot_y + plot_h + 14);
		svg_escape(buf, labels[i]);
		appendStringInfoString(buf, "</text>\n");
	}
}

/* ----------------------------------------------------------------
 * Helper: run SPI query and validate column count
 * ---------------------------------------------------------------- */
static int
run_chart_query(const char *sql, int min_cols)
{
	int		ret;

	SPI_connect();
	ret = SPI_execute(sql, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chart query must be a SELECT statement")));
	}

	if (SPI_tuptable == NULL || SPI_processed == 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA),
				 errmsg("chart query returned no rows")));
	}

	if (SPI_tuptable->tupdesc->natts < min_cols)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chart query must return at least %d columns", min_cols)));
	}

	return (int) SPI_processed;
}

/* ================================================================
 * chart_bar(sql text, title text DEFAULT '') -> text (SVG)
 *
 * Query: SELECT label, value [, value2, ...] FROM ...
 * Col 1 = category labels (text)
 * Col 2+ = numeric series (one bar group per label)
 * ================================================================ */
Datum
chart_bar(PG_FUNCTION_ARGS)
{
	const char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *title = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			nrows, nseries, natts;
	int			i, s;
	double		max_val = 0, min_val = 0;
	char	  **labels;
	double	  **data;
	StringInfoData buf;
	int			plot_x, plot_y, plot_w, plot_h;
	int			width = DEFAULT_WIDTH, height = DEFAULT_HEIGHT;
	char	  **series_names;

	nrows = run_chart_query(sql, 2);
	natts = SPI_tuptable->tupdesc->natts;
	nseries = natts - 1;

	/* Collect labels, data, series names */
	labels = palloc(nrows * sizeof(char *));
	data = palloc(nseries * sizeof(double *));
	series_names = palloc(nseries * sizeof(char *));

	for (s = 0; s < nseries; s++)
	{
		data[s] = palloc(nrows * sizeof(double));
		series_names[s] = pstrdup(SPI_fname(SPI_tuptable->tupdesc, s + 2));
	}

	for (i = 0; i < nrows; i++)
	{
		labels[i] = get_spi_string(i, 0);
		for (s = 0; s < nseries; s++)
		{
			data[s][i] = get_spi_double(i, s + 1);
			if (data[s][i] > max_val) max_val = data[s][i];
			if (data[s][i] < min_val) min_val = data[s][i];
		}
	}

	SPI_finish();

	/* Adjust range */
	if (min_val > 0) min_val = 0;
	if (max_val == min_val) max_val = min_val + 1;
	max_val *= 1.1;

	/* Compute plot area */
	plot_x = MARGIN_LEFT;
	plot_y = MARGIN_TOP;
	plot_w = width - MARGIN_LEFT - MARGIN_RIGHT;
	plot_h = height - MARGIN_TOP - MARGIN_BOTTOM;

	initStringInfo(&buf);
	svg_header(&buf, width, height, title);
	svg_y_axis(&buf, min_val, max_val, plot_x, plot_y, plot_w, plot_h, 5);
	svg_x_labels(&buf, labels, nrows, plot_x, plot_y, plot_w, plot_h, true);

	/* Draw bars */
	{
		double	group_w = (double) plot_w / nrows;
		double	bar_w = (group_w * 0.8) / nseries;
		double	gap = group_w * 0.1;
		double	range = max_val - min_val;

		for (i = 0; i < nrows; i++)
		{
			for (s = 0; s < nseries; s++)
			{
				double	val = data[s][i];
				double	frac = (val - min_val) / range;
				int		bh = (int)(frac * plot_h);
				int		bx = plot_x + (int)(i * group_w + gap + s * bar_w);
				int		by = plot_y + plot_h - bh;

				appendStringInfo(&buf,
					"<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" "
					"fill=\"%s\" rx=\"2\">"
					"<title>",
					bx, by, (int) bar_w, bh,
					palette[s % PALETTE_SIZE]);
				svg_escape(&buf, labels[i]);
				appendStringInfo(&buf, ": %.2f</title></rect>\n", val);
			}
		}
	}

	/* Legend (if multiple series) */
	if (nseries > 1)
	{
		int		lx = plot_x + plot_w - nseries * 90;
		int		ly = plot_y - 10;

		for (s = 0; s < nseries; s++)
		{
			appendStringInfo(&buf,
				"<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" fill=\"%s\"/>"
				"<text x=\"%d\" y=\"%d\" font-size=\"11\" fill=\"#333\">",
				lx + s * 90, ly - LEGEND_BOX, LEGEND_BOX, LEGEND_BOX,
				palette[s % PALETTE_SIZE],
				lx + s * 90 + LEGEND_BOX + 4, ly);
			svg_escape(&buf, series_names[s]);
			appendStringInfoString(&buf, "</text>\n");
		}
	}

	appendStringInfoString(&buf, "</svg>");
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ================================================================
 * chart_line(sql text, title text DEFAULT '') -> text (SVG)
 *
 * Query: SELECT label, value [, value2, ...] FROM ...
 * Col 1 = X-axis labels (text)
 * Col 2+ = Y-axis series (numeric)
 * ================================================================ */
Datum
chart_line(PG_FUNCTION_ARGS)
{
	const char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *title = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			nrows, nseries, natts;
	int			i, s;
	double		max_val = -DBL_MAX, min_val = DBL_MAX;
	char	  **labels;
	double	  **data;
	char	  **series_names;
	StringInfoData buf;
	int			plot_x, plot_y, plot_w, plot_h;
	int			width = DEFAULT_WIDTH, height = DEFAULT_HEIGHT;

	nrows = run_chart_query(sql, 2);
	natts = SPI_tuptable->tupdesc->natts;
	nseries = natts - 1;

	labels = palloc(nrows * sizeof(char *));
	data = palloc(nseries * sizeof(double *));
	series_names = palloc(nseries * sizeof(char *));

	for (s = 0; s < nseries; s++)
	{
		data[s] = palloc(nrows * sizeof(double));
		series_names[s] = pstrdup(SPI_fname(SPI_tuptable->tupdesc, s + 2));
	}

	for (i = 0; i < nrows; i++)
	{
		labels[i] = get_spi_string(i, 0);
		for (s = 0; s < nseries; s++)
		{
			data[s][i] = get_spi_double(i, s + 1);
			if (data[s][i] > max_val) max_val = data[s][i];
			if (data[s][i] < min_val) min_val = data[s][i];
		}
	}

	SPI_finish();

	if (min_val > 0) min_val = 0;
	if (max_val == min_val) max_val = min_val + 1;
	max_val *= 1.05;

	plot_x = MARGIN_LEFT;
	plot_y = MARGIN_TOP;
	plot_w = width - MARGIN_LEFT - MARGIN_RIGHT;
	plot_h = height - MARGIN_TOP - MARGIN_BOTTOM;

	initStringInfo(&buf);
	svg_header(&buf, width, height, title);
	svg_y_axis(&buf, min_val, max_val, plot_x, plot_y, plot_w, plot_h, 5);
	svg_x_labels(&buf, labels, nrows, plot_x, plot_y, plot_w, plot_h, false);

	/* Draw lines */
	for (s = 0; s < nseries; s++)
	{
		double	range = max_val - min_val;

		appendStringInfoString(&buf, "<polyline points=\"");
		for (i = 0; i < nrows; i++)
		{
			int		px = plot_x + (nrows > 1
						? (int)((double) i / (nrows - 1) * plot_w)
						: plot_w / 2);
			int		py = plot_y + plot_h
						 - (int)((data[s][i] - min_val) / range * plot_h);

			appendStringInfo(&buf, "%d,%d ", px, py);
		}
		appendStringInfo(&buf,
			"\" fill=\"none\" stroke=\"%s\" stroke-width=\"2.5\" "
			"stroke-linejoin=\"round\"/>\n",
			palette[s % PALETTE_SIZE]);

		/* Data points */
		for (i = 0; i < nrows; i++)
		{
			int		px = plot_x + (nrows > 1
						? (int)((double) i / (nrows - 1) * plot_w)
						: plot_w / 2);
			int		py = plot_y + plot_h
						 - (int)((data[s][i] - min_val) / range * plot_h);

			appendStringInfo(&buf,
				"<circle cx=\"%d\" cy=\"%d\" r=\"3.5\" fill=\"%s\">"
				"<title>",
				px, py, palette[s % PALETTE_SIZE]);
			svg_escape(&buf, labels[i]);
			appendStringInfo(&buf, ": %.2f</title></circle>\n", data[s][i]);
		}
	}

	/* Legend */
	if (nseries > 1)
	{
		int		lx = plot_x + plot_w - nseries * 90;
		int		ly = plot_y - 10;

		for (s = 0; s < nseries; s++)
		{
			appendStringInfo(&buf,
				"<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" "
				"stroke=\"%s\" stroke-width=\"2.5\"/>"
				"<text x=\"%d\" y=\"%d\" font-size=\"11\" fill=\"#333\">",
				lx + s * 90, ly - 6, lx + s * 90 + LEGEND_BOX, ly - 6,
				palette[s % PALETTE_SIZE],
				lx + s * 90 + LEGEND_BOX + 4, ly);
			svg_escape(&buf, series_names[s]);
			appendStringInfoString(&buf, "</text>\n");
		}
	}

	appendStringInfoString(&buf, "</svg>");
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ================================================================
 * chart_pie(sql text, title text DEFAULT '') -> text (SVG)
 *
 * Query: SELECT label, value FROM ...
 * Col 1 = slice labels (text)
 * Col 2 = slice values (numeric, positive)
 * ================================================================ */
Datum
chart_pie(PG_FUNCTION_ARGS)
{
	const char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *title = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			nrows, i;
	double		total = 0;
	char	  **labels;
	double	   *values;
	StringInfoData buf;
	int			width = DEFAULT_WIDTH, height = DEFAULT_HEIGHT;
	int			cx, cy, r;
	double		angle;

	nrows = run_chart_query(sql, 2);

	labels = palloc(nrows * sizeof(char *));
	values = palloc(nrows * sizeof(double));

	for (i = 0; i < nrows; i++)
	{
		labels[i] = get_spi_string(i, 0);
		values[i] = get_spi_double(i, 1);
		if (values[i] < 0) values[i] = 0;
		total += values[i];
	}

	SPI_finish();

	if (total <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pie chart requires positive values")));

	cx = width / 2 - 80;
	cy = height / 2 + 10;
	r = PIE_RADIUS;

	initStringInfo(&buf);
	svg_header(&buf, width, height, title);

	/* Draw slices */
	angle = -M_PI / 2;		/* start at 12 o'clock */
	for (i = 0; i < nrows; i++)
	{
		double	frac = values[i] / total;
		double	sweep = frac * 2 * M_PI;
		double	x1 = cx + r * cos(angle);
		double	y1 = cy + r * sin(angle);
		double	x2 = cx + r * cos(angle + sweep);
		double	y2 = cy + r * sin(angle + sweep);
		int		large = (sweep > M_PI) ? 1 : 0;
		double	pct = frac * 100;
		char	pct_str[16];

		snprintf(pct_str, sizeof(pct_str), "%.1f%%", pct);

		appendStringInfo(&buf,
			"<path d=\"M%d,%d L%.1f,%.1f A%d,%d 0 %d,1 %.1f,%.1f Z\" "
			"fill=\"%s\" stroke=\"#fff\" stroke-width=\"2\">"
			"<title>",
			cx, cy, x1, y1, r, r, large, x2, y2,
			palette[i % PALETTE_SIZE]);
		svg_escape(&buf, labels[i]);
		appendStringInfo(&buf, ": %.2f (%s)</title></path>\n",
						 values[i], pct_str);

		/* Percentage label on slice (if big enough) */
		if (frac > 0.04)
		{
			double	mid = angle + sweep / 2;
			double	lx = cx + r * 0.65 * cos(mid);
			double	ly = cy + r * 0.65 * sin(mid);

			appendStringInfo(&buf,
				"<text x=\"%.0f\" y=\"%.0f\" text-anchor=\"middle\" "
				"font-size=\"11\" font-weight=\"bold\" fill=\"#fff\">%s</text>\n",
				lx, ly, pct_str);
		}

		angle += sweep;
	}

	/* Legend */
	{
		int		lx = cx + r + 40;
		int		ly = cy - (nrows * 20) / 2;

		for (i = 0; i < nrows; i++)
		{
			appendStringInfo(&buf,
				"<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" "
				"fill=\"%s\" rx=\"2\"/>"
				"<text x=\"%d\" y=\"%d\" font-size=\"11\" fill=\"#333\">",
				lx, ly + i * 20, LEGEND_BOX, LEGEND_BOX,
				palette[i % PALETTE_SIZE],
				lx + LEGEND_BOX + 6, ly + i * 20 + 11);
			svg_escape(&buf, labels[i]);
			appendStringInfoString(&buf, "</text>\n");
		}
	}

	appendStringInfoString(&buf, "</svg>");
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ================================================================
 * chart_scatter(sql text, title text DEFAULT '') -> text (SVG)
 *
 * Query: SELECT x, y [, series_label] FROM ...
 * Col 1 = X values (numeric)
 * Col 2 = Y values (numeric)
 * Col 3 = optional series/group label (text)
 * ================================================================ */
Datum
chart_scatter(PG_FUNCTION_ARGS)
{
	const char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *title = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			nrows, natts, i;
	double		x_min = DBL_MAX, x_max = -DBL_MAX;
	double		y_min = DBL_MAX, y_max = -DBL_MAX;
	double	   *xvals, *yvals;
	char	  **group_labels;
	bool		has_groups;
	StringInfoData buf;
	int			plot_x, plot_y, plot_w, plot_h;
	int			width = DEFAULT_WIDTH, height = DEFAULT_HEIGHT;

	nrows = run_chart_query(sql, 2);
	natts = SPI_tuptable->tupdesc->natts;
	has_groups = (natts >= 3);

	xvals = palloc(nrows * sizeof(double));
	yvals = palloc(nrows * sizeof(double));
	group_labels = palloc(nrows * sizeof(char *));

	for (i = 0; i < nrows; i++)
	{
		xvals[i] = get_spi_double(i, 0);
		yvals[i] = get_spi_double(i, 1);
		group_labels[i] = has_groups ? get_spi_string(i, 2) : pstrdup("");

		if (xvals[i] < x_min) x_min = xvals[i];
		if (xvals[i] > x_max) x_max = xvals[i];
		if (yvals[i] < y_min) y_min = yvals[i];
		if (yvals[i] > y_max) y_max = yvals[i];
	}

	SPI_finish();

	if (x_min == x_max) { x_min -= 1; x_max += 1; }
	if (y_min == y_max) { y_min -= 1; y_max += 1; }
	/* Add 5% padding */
	{
		double xpad = (x_max - x_min) * 0.05;
		double ypad = (y_max - y_min) * 0.05;
		x_min -= xpad; x_max += xpad;
		y_min -= ypad; y_max += ypad;
	}

	plot_x = MARGIN_LEFT;
	plot_y = MARGIN_TOP;
	plot_w = width - MARGIN_LEFT - MARGIN_RIGHT;
	plot_h = height - MARGIN_TOP - MARGIN_BOTTOM;

	initStringInfo(&buf);
	svg_header(&buf, width, height, title);
	svg_y_axis(&buf, y_min, y_max, plot_x, plot_y, plot_w, plot_h, 5);

	/* X-axis labels for scatter */
	{
		int		ticks = 5;
		char	label[32];

		appendStringInfo(&buf,
			"<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" "
			"stroke=\"#999\" stroke-width=\"1\"/>\n",
			plot_x, plot_y + plot_h, plot_x + plot_w, plot_y + plot_h);

		for (i = 0; i <= ticks; i++)
		{
			double	frac = (double) i / ticks;
			int		x = plot_x + (int)(frac * plot_w);
			double	val = x_min + frac * (x_max - x_min);

			format_number(label, sizeof(label), val);
			appendStringInfo(&buf,
				"<text x=\"%d\" y=\"%d\" text-anchor=\"middle\" "
				"font-size=\"11\" fill=\"#666\">%s</text>\n",
				x, plot_y + plot_h + 18, label);
		}
	}

	/* Draw points */
	for (i = 0; i < nrows; i++)
	{
		int		px = plot_x + (int)((xvals[i] - x_min) / (x_max - x_min) * plot_w);
		int		py = plot_y + plot_h - (int)((yvals[i] - y_min) / (y_max - y_min) * plot_h);
		/* Simple hash for group color */
		int		color_idx = 0;

		if (has_groups && group_labels[i][0])
		{
			unsigned int h = 0;
			const char *p;
			for (p = group_labels[i]; *p; p++)
				h = h * 31 + (unsigned char)*p;
			color_idx = h % PALETTE_SIZE;
		}

		appendStringInfo(&buf,
			"<circle cx=\"%d\" cy=\"%d\" r=\"4\" fill=\"%s\" opacity=\"0.7\">"
			"<title>(%.2f, %.2f)",
			px, py, palette[color_idx], xvals[i], yvals[i]);
		if (has_groups && group_labels[i][0])
		{
			appendStringInfoString(&buf, " - ");
			svg_escape(&buf, group_labels[i]);
		}
		appendStringInfoString(&buf, "</title></circle>\n");
	}

	appendStringInfoString(&buf, "</svg>");
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ================================================================
 * chart_area(sql text, title text DEFAULT '') -> text (SVG)
 *
 * Same input as chart_line. Draws filled areas under each series.
 * ================================================================ */
Datum
chart_area(PG_FUNCTION_ARGS)
{
	const char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *title = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			nrows, nseries, natts;
	int			i, s;
	double		max_val = -DBL_MAX, min_val = DBL_MAX;
	char	  **labels;
	double	  **data;
	char	  **series_names;
	StringInfoData buf;
	int			plot_x, plot_y, plot_w, plot_h;
	int			width = DEFAULT_WIDTH, height = DEFAULT_HEIGHT;

	nrows = run_chart_query(sql, 2);
	natts = SPI_tuptable->tupdesc->natts;
	nseries = natts - 1;

	labels = palloc(nrows * sizeof(char *));
	data = palloc(nseries * sizeof(double *));
	series_names = palloc(nseries * sizeof(char *));

	for (s = 0; s < nseries; s++)
	{
		data[s] = palloc(nrows * sizeof(double));
		series_names[s] = pstrdup(SPI_fname(SPI_tuptable->tupdesc, s + 2));
	}

	for (i = 0; i < nrows; i++)
	{
		labels[i] = get_spi_string(i, 0);
		for (s = 0; s < nseries; s++)
		{
			data[s][i] = get_spi_double(i, s + 1);
			if (data[s][i] > max_val) max_val = data[s][i];
			if (data[s][i] < min_val) min_val = data[s][i];
		}
	}

	SPI_finish();

	if (min_val > 0) min_val = 0;
	if (max_val == min_val) max_val = min_val + 1;
	max_val *= 1.05;

	plot_x = MARGIN_LEFT;
	plot_y = MARGIN_TOP;
	plot_w = width - MARGIN_LEFT - MARGIN_RIGHT;
	plot_h = height - MARGIN_TOP - MARGIN_BOTTOM;

	initStringInfo(&buf);
	svg_header(&buf, width, height, title);
	svg_y_axis(&buf, min_val, max_val, plot_x, plot_y, plot_w, plot_h, 5);
	svg_x_labels(&buf, labels, nrows, plot_x, plot_y, plot_w, plot_h, false);

	/* Draw filled areas (back to front for layering) */
	for (s = nseries - 1; s >= 0; s--)
	{
		double	range = max_val - min_val;
		int		base_y = plot_y + plot_h;

		appendStringInfoString(&buf, "<polygon points=\"");

		/* Bottom-left start */
		appendStringInfo(&buf, "%d,%d ", plot_x, base_y);

		/* Top edge */
		for (i = 0; i < nrows; i++)
		{
			int		px = plot_x + (nrows > 1
						? (int)((double) i / (nrows - 1) * plot_w)
						: plot_w / 2);
			int		py = plot_y + plot_h
						 - (int)((data[s][i] - min_val) / range * plot_h);

			appendStringInfo(&buf, "%d,%d ", px, py);
		}

		/* Bottom-right close */
		appendStringInfo(&buf, "%d,%d ",
						 plot_x + (nrows > 1 ? plot_w : plot_w / 2), base_y);

		appendStringInfo(&buf,
			"\" fill=\"%s\" fill-opacity=\"0.35\" "
			"stroke=\"%s\" stroke-width=\"2\"/>\n",
			palette[s % PALETTE_SIZE], palette[s % PALETTE_SIZE]);
	}

	/* Legend */
	if (nseries > 1)
	{
		int		lx = plot_x + plot_w - nseries * 90;
		int		ly = plot_y - 10;

		for (s = 0; s < nseries; s++)
		{
			appendStringInfo(&buf,
				"<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" "
				"fill=\"%s\" opacity=\"0.6\"/>"
				"<text x=\"%d\" y=\"%d\" font-size=\"11\" fill=\"#333\">",
				lx + s * 90, ly - LEGEND_BOX, LEGEND_BOX, LEGEND_BOX,
				palette[s % PALETTE_SIZE],
				lx + s * 90 + LEGEND_BOX + 4, ly);
			svg_escape(&buf, series_names[s]);
			appendStringInfoString(&buf, "</text>\n");
		}
	}

	appendStringInfoString(&buf, "</svg>");
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
