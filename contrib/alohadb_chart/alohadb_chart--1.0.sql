/* contrib/alohadb_chart/alohadb_chart--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_chart" to load this file. \quit

-- Bar chart: SELECT label, value [, value2, ...] FROM ...
CREATE FUNCTION chart_bar(
    sql text,
    title text DEFAULT ''
)
RETURNS text
AS 'MODULE_PATHNAME', 'chart_bar'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION chart_bar(text, text) IS
'Generate an SVG bar chart from a query. Column 1 = labels (text), columns 2+ = values (numeric).';

-- Line chart: SELECT label, value [, value2, ...] FROM ...
CREATE FUNCTION chart_line(
    sql text,
    title text DEFAULT ''
)
RETURNS text
AS 'MODULE_PATHNAME', 'chart_line'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION chart_line(text, text) IS
'Generate an SVG line chart from a query. Column 1 = X labels (text), columns 2+ = Y series (numeric).';

-- Pie chart: SELECT label, value FROM ...
CREATE FUNCTION chart_pie(
    sql text,
    title text DEFAULT ''
)
RETURNS text
AS 'MODULE_PATHNAME', 'chart_pie'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION chart_pie(text, text) IS
'Generate an SVG pie chart from a query. Column 1 = labels (text), column 2 = values (numeric).';

-- Scatter plot: SELECT x, y [, group_label] FROM ...
CREATE FUNCTION chart_scatter(
    sql text,
    title text DEFAULT ''
)
RETURNS text
AS 'MODULE_PATHNAME', 'chart_scatter'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION chart_scatter(text, text) IS
'Generate an SVG scatter plot from a query. Column 1 = X (numeric), column 2 = Y (numeric), column 3 = optional group label (text).';

-- Area chart: SELECT label, value [, value2, ...] FROM ...
CREATE FUNCTION chart_area(
    sql text,
    title text DEFAULT ''
)
RETURNS text
AS 'MODULE_PATHNAME', 'chart_area'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION chart_area(text, text) IS
'Generate an SVG area chart from a query. Column 1 = X labels (text), columns 2+ = Y series (numeric).';
