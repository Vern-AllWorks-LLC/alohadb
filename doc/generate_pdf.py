"""
AlohaDB User Guide PDF Generator
Vern AllWorks LLC | https://www.OpenCan.ai
"""

from fpdf import FPDF
from fpdf.enums import XPos, YPos
import os

OUT_PATH = os.path.join(os.path.dirname(__file__), "alohadb-user-guide.pdf")

# ── colours ──────────────────────────────────────────────────────────────────
BG      = (255, 255, 255)
FG      = ( 26,  26,  26)
HEADING = (  0,  80, 160)
CODE_BG = (245, 245, 245)
CODE_FG = ( 40,  40,  40)
ACCENT  = (  0, 120,  60)
MUTED   = (100, 100, 100)
LINK    = (  0,  80, 200)

# ── fonts ─────────────────────────────────────────────────────────────────────
# fpdf2 ships DejaVu (has Unicode); use it for body.
# Courier (built-in) for code blocks.

BODY  = "Helvetica"
MONO  = "Courier"

class GuideDoc(FPDF):
    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(20, 20, 20)
        self._toc = []          # (title, level, page)
        self._section_num = 0

    # ── helpers ───────────────────────────────────────────────────────────────
    def _w(self):
        """Usable page width."""
        return self.w - self.l_margin - self.r_margin

    def set_fg(self, rgb=FG):
        self.set_text_color(*rgb)

    def set_bg_fill(self, rgb):
        self.set_fill_color(*rgb)

    # ── header / footer ───────────────────────────────────────────────────────
    def header(self):
        if self.page == 1:
            return
        self.set_font(BODY, "B", 8)
        self.set_text_color(*MUTED)
        self.cell(0, 6, "AlohaDB 20.0 User Guide  |  Vern AllWorks LLC  |  www.OpenCan.ai",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*MUTED)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)

    def footer(self):
        if self.page == 1:
            return
        self.set_y(-15)
        self.set_font(BODY, "", 8)
        self.set_text_color(*MUTED)
        self.cell(0, 6, f"Page {self.page}", align="C",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # ── cover page ────────────────────────────────────────────────────────────
    def cover(self):
        self.add_page()
        self.set_fill_color(*BG)
        self.rect(0, 0, self.w, self.h, "F")

        # top colour bar
        self.set_fill_color(*HEADING)
        self.rect(0, 0, self.w, 30, "F")

        self.set_y(8)
        self.set_font(BODY, "B", 18)
        self.set_text_color(255, 255, 255)
        self.cell(0, 10, "AlohaDB 20.0", align="C",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font(BODY, "", 11)
        self.cell(0, 8, "User Guide", align="C",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.ln(18)
        self.set_text_color(*FG)
        self.set_font(BODY, "B", 24)
        self.cell(0, 14, "AlohaDB", align="C",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font(BODY, "", 13)
        self.set_text_color(*ACCENT)
        self.cell(0, 8, "Unified Analytical Database", align="C",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(4)
        self.set_text_color(*FG)
        self.set_font(BODY, "", 11)
        lines = [
            "34+ Extensions  |  PostgreSQL 18 Core",
            "Streaming  |  Vector  |  Geo  |  Python  |  Object Store",
            "Pipeline  |  Kafka  |  Parquet  |  ML Registry",
        ]
        for l in lines:
            self.cell(0, 7, l, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # version / date block
        self.ln(12)
        self.set_fill_color(*CODE_BG)
        self.rect(self.l_margin, self.get_y(), self._w(), 28, "F")
        self.set_y(self.get_y() + 4)
        info = [
            ("Version", "20.0"),
            ("Release Date", "2026"),
            ("License", "PostgreSQL License"),
            ("Publisher", "Vern AllWorks LLC"),
            ("Website", "https://www.OpenCan.ai"),
        ]
        self.set_font(BODY, "", 9)
        for label, val in info:
            self.set_x(self.l_margin + 4)
            self.set_text_color(*MUTED)
            self.cell(35, 4.5, label + ":", new_x=XPos.RIGHT, new_y=YPos.LAST)
            self.set_text_color(*FG)
            self.cell(0, 4.5, val, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # bottom bar
        self.set_fill_color(*HEADING)
        self.rect(0, self.h - 18, self.w, 18, "F")
        self.set_y(self.h - 13)
        self.set_font(BODY, "", 8)
        self.set_text_color(255, 255, 255)
        self.cell(0, 6,
                  "Copyright 2026 Vern AllWorks LLC. PostgreSQL License.  www.OpenCan.ai",
                  align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # ── typography helpers ────────────────────────────────────────────────────
    def h1(self, text):
        self._section_num += 1
        numbered = f"{self._section_num}.  {text}"
        self._toc.append((numbered, 1, self.page))
        self.ln(6)
        self.set_fill_color(*HEADING)
        self.rect(self.l_margin, self.get_y(), self._w(), 9, "F")
        self.set_y(self.get_y() + 1)
        self.set_font(BODY, "B", 13)
        self.set_text_color(255, 255, 255)
        self.set_x(self.l_margin + 3)
        self.cell(self._w() - 3, 7, numbered,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*FG)
        self.ln(2)

    def h2(self, text):
        self._toc.append(("  " + text, 2, self.page))
        self.ln(4)
        self.set_font(BODY, "B", 11)
        self.set_text_color(*HEADING)
        self.cell(0, 7, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*HEADING)
        self.line(self.l_margin, self.get_y(),
                  self.l_margin + self._w() * 0.4, self.get_y())
        self.set_text_color(*FG)
        self.ln(1)

    def h3(self, text):
        self.ln(3)
        self.set_font(BODY, "B", 10)
        self.set_text_color(*ACCENT)
        self.cell(0, 6, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*FG)

    def para(self, text, indent=0):
        self.set_font(BODY, "", 9.5)
        self.set_text_color(*FG)
        self.set_x(self.l_margin + indent)
        self.multi_cell(self._w() - indent, 5.5, text,
                        new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def bullet(self, text, indent=4):
        self.set_font(BODY, "", 9.5)
        self.set_text_color(*FG)
        self.set_x(self.l_margin + indent)
        self.cell(5, 5.5, "-", new_x=XPos.RIGHT, new_y=YPos.LAST)
        self.multi_cell(self._w() - indent - 5, 5.5, text,
                        new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def note(self, text):
        """Highlighted note box."""
        self.ln(2)
        self.set_fill_color(255, 252, 220)
        self.set_draw_color(200, 180, 0)
        x, y = self.l_margin, self.get_y()
        # draw background later after we know height
        self.set_font(BODY, "I", 9)
        self.set_text_color(80, 60, 0)
        self.set_x(x + 3)
        self.multi_cell(self._w() - 6, 5, "Note: " + text,
                        new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*FG)
        self.ln(2)

    def code(self, text):
        """Monospace code block."""
        self.ln(1)
        lines = text.strip("\n").split("\n")
        h = len(lines) * 4.8 + 4
        self.set_fill_color(*CODE_BG)
        self.set_draw_color(200, 200, 200)
        self.rect(self.l_margin, self.get_y(), self._w(), h, "FD")
        self.set_y(self.get_y() + 2)
        self.set_font(MONO, "", 7.8)
        self.set_text_color(*CODE_FG)
        for line in lines:
            self.set_x(self.l_margin + 3)
            self.cell(self._w() - 6, 4.8, line,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*FG)
        self.ln(2)

    def table(self, headers, rows, col_widths=None):
        """Simple table."""
        w = self._w()
        if col_widths is None:
            cw = w / len(headers)
            col_widths = [cw] * len(headers)
        # header row
        self.set_fill_color(*HEADING)
        self.set_text_color(255, 255, 255)
        self.set_font(BODY, "B", 8)
        for i, h in enumerate(headers):
            self.set_x(self.l_margin + sum(col_widths[:i]))
            self.cell(col_widths[i], 6, h, border=1, fill=True,
                      new_x=XPos.RIGHT, new_y=YPos.LAST)
        self.ln()
        # data rows
        self.set_text_color(*FG)
        self.set_font(BODY, "", 7.8)
        for ri, row in enumerate(rows):
            fill = ri % 2 == 0
            self.set_fill_color(242, 246, 252) if fill else self.set_fill_color(*BG)
            for i, cell in enumerate(row):
                self.set_x(self.l_margin + sum(col_widths[:i]))
                self.cell(col_widths[i], 5.5, str(cell), border=1,
                          fill=True, new_x=XPos.RIGHT, new_y=YPos.LAST)
            self.ln()
        self.ln(3)

    def toc_page(self):
        self.add_page()
        self.set_font(BODY, "B", 16)
        self.set_text_color(*HEADING)
        self.cell(0, 12, "Table of Contents",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*HEADING)
        self.line(self.l_margin, self.get_y(),
                  self.l_margin + self._w(), self.get_y())
        self.ln(4)
        for title, level, page in self._toc:
            indent = 0 if level == 1 else 8
            self.set_x(self.l_margin + indent)
            font_size = 9.5 if level == 1 else 8.5
            style = "B" if level == 1 else ""
            self.set_font(BODY, style, font_size)
            self.set_text_color(*FG if level == 1 else MUTED)
            dots_width = self._w() - indent - 12
            self.cell(dots_width, 6, title, new_x=XPos.RIGHT, new_y=YPos.LAST)
            self.cell(12, 6, str(page), align="R",
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)


# ── content ───────────────────────────────────────────────────────────────────

def build_guide(doc: GuideDoc):

    # ─────────────────────────────────────────────────
    # 1. What Is AlohaDB
    # ─────────────────────────────────────────────────
    doc.add_page()
    doc.h1("What Is AlohaDB?")
    doc.para(
        "AlohaDB is a PostgreSQL 18-based analytical database with 34+ extensions that "
        "deliver streaming ingest, vector similarity search, geospatial operations, "
        "Python scripting, Parquet file I/O, object-store integration, declarative "
        "pipeline orchestration, Kafka/Kinesis consumers, unified batch+stream views, "
        "ontology and intent classification, contextual disambiguation, and an ML model "
        "registry -- all within a single, standards-compliant SQL engine."
    )
    doc.para(
        "AlohaDB is distributed by Vern AllWorks LLC under the PostgreSQL License "
        "(permissive open source). The primary website is https://www.OpenCan.ai."
    )

    doc.h2("Architecture")
    doc.para(
        "AlohaDB extends PostgreSQL 18 through the standard extension mechanism. Each "
        "capability ships as an independent CREATE EXTENSION; all extensions share the "
        "same backend process, buffer pool, WAL, MVCC, and SQL planner."
    )
    arch_rows = [
        ("pg_cronjobs",           "Background scheduler (pg_cron-compatible)"),
        ("pg_pipeline",       "DAG task orchestration with topological dispatch"),
        ("pg_kafka",          "Kafka / Kinesis consumer background worker"),
        ("pg_objectstore",    "S3 / GCS / Azure Blob object storage"),
        ("pg_vector",         "Vector type, IVFFlat ANN index, distance operators"),
        ("pg_geo",            "Point, BBox, Polygon, Circle + ST_ functions"),
        ("pg_parquetio",        "Native Parquet reader (v1.1); read_csv, read_json"),
        ("pg_unified",        "Streaming + batch unified view"),
        ("pg_python",         "Inline Python scripting via PL/Python3"),
        ("pg_model_registry", "ML model versioning, deploy/rollback, experiments"),
        ("pg_ontology",       "RDF-style triple store and hierarchy traversal"),
        ("pg_intent",         "Keyword-based intent classification and auto-tagging"),
        ("pg_context",        "Contextual profile management and disambiguation"),
    ]
    doc.table(["Extension", "Purpose"], arch_rows, [55, 115])

    # ─────────────────────────────────────────────────
    # 2. System Requirements
    # ─────────────────────────────────────────────────
    doc.h1("Minimum System Requirements")
    req_rows = [
        ("Operating System", "Ubuntu 22.04+, RHEL 9+, Windows 10/11, macOS 13+"),
        ("CPU",              "x86-64 or ARM64, 2+ cores"),
        ("RAM",              "4 GB minimum; 16 GB recommended for vector/analytics"),
        ("Disk",             "10 GB minimum; SSD recommended for OLAP workloads"),
        ("PostgreSQL",       "18.0 or later (bundled with AlohaDB packages)"),
        ("Python",           "3.10+ with plpython3u (required for pg_python)"),
        ("libcurl",          "7.68+ with OpenSSL backend (for objectstore / GCS OAuth2)"),
        ("OpenSSL",          "1.1.1+ or 3.x (SigV4 HMAC-SHA256 / RS256 JWT signing)"),
        ("librdkafka",       "1.9+ (optional; required for Kafka consumer support)"),
    ]
    doc.table(["Requirement", "Value"], req_rows, [45, 125])

    doc.h2("Required Network Ports")
    port_rows = [
        ("5432", "PostgreSQL client connections"),
        ("443",  "HTTPS to S3/GCS/Azure and Kafka MSK"),
        ("9092", "Kafka plaintext (self-hosted)"),
        ("9094", "Kafka TLS (self-hosted)"),
    ]
    doc.table(["Port", "Purpose"], port_rows, [25, 145])

    # ─────────────────────────────────────────────────
    # 3. Installation – Linux
    # ─────────────────────────────────────────────────
    doc.h1("Installation on Linux")

    doc.h2("Debian / Ubuntu (.deb)")
    doc.code("""\
# Add AlohaDB repo
curl -fsSL https://www.OpenCan.ai/repo/gpg.key | sudo gpg --dearmor \\
  -o /usr/share/keyrings/alohadb.gpg

echo "deb [signed-by=/usr/share/keyrings/alohadb.gpg] \\
  https://www.OpenCan.ai/repo/apt stable main" \\
  | sudo tee /etc/apt/sources.list.d/alohadb.list

sudo apt update
sudo apt install -y alohadb-20""")

    doc.h2("RHEL / Rocky / Alma (.rpm)")
    doc.code("""\
sudo dnf install -y https://www.OpenCan.ai/repo/rpm/alohadb-repo.rpm
sudo dnf install -y alohadb-20""")

    doc.h2("Source Build")
    doc.code("""\
tar xf alohadb-20.0.tar.gz
cd alohadb-20.0
meson setup build --prefix=/usr/local/pgsql
ninja -C build
sudo ninja -C build install""")

    doc.h2("Post-Install")
    doc.code("""\
# Add extensions to postgresql.conf
shared_preload_libraries = 'pg_cronjobs,pg_pipeline,pg_kafka'

# Restart and create extensions
sudo systemctl restart postgresql-18
psql -U postgres -c "CREATE EXTENSION pg_vector;"
psql -U postgres -c "CREATE EXTENSION pg_geo;" """)

    # ─────────────────────────────────────────────────
    # 4. Installation – Windows
    # ─────────────────────────────────────────────────
    doc.h1("Installation on Windows")

    doc.h2("Installer (GUI)")
    doc.para(
        "Download AlohaDB-20.0-win-x64.exe from https://www.OpenCan.ai/download. "
        "Run as Administrator; the installer configures PostgreSQL 18 as a Windows "
        "service and adds psql to the system PATH."
    )

    doc.h2("CLI / Silent Install")
    doc.code("""\
AlohaDB-20.0-win-x64.exe /SILENT /DIR=\"C:\\AlohaDB\" /COMPONENTS=server,tools""")

    doc.h2("Environment Variables")
    doc.code("""\
set PGDATA=C:\\AlohaDB\\data
set PGPORT=5432
set PATH=%PATH%;C:\\AlohaDB\\bin

# Start service
sc start alohadb""")

    # ─────────────────────────────────────────────────
    # 5. Installation – macOS
    # ─────────────────────────────────────────────────
    doc.h1("Installation on macOS")

    doc.h2("Homebrew")
    doc.code("""\
brew tap vernallworks/alohadb https://www.OpenCan.ai/homebrew-tap
brew install alohadb@20""")

    doc.h2("PKG Installer")
    doc.para(
        "Download AlohaDB-20.0-macos-arm64.pkg (Apple Silicon) or "
        "AlohaDB-20.0-macos-x64.pkg (Intel) from https://www.OpenCan.ai/download. "
        "Open the PKG and follow the installation wizard."
    )

    doc.h2("Post-Install (macOS)")
    doc.code("""\
# Add to ~/.zshrc
export PATH=/usr/local/pgsql/bin:$PATH
export PGDATA=/usr/local/pgsql/data

pg_ctl -D $PGDATA start""")

    # ─────────────────────────────────────────────────
    # 6. First Steps
    # ─────────────────────────────────────────────────
    doc.h1("First Steps")
    doc.para("After installation, connect with psql and enable extensions:")
    doc.code("""\
psql -U postgres

-- Core extensions (add to shared_preload_libraries first):
CREATE EXTENSION pg_cronjobs;
CREATE EXTENSION pg_pipeline;
CREATE EXTENSION pg_kafka;

-- Standalone extensions (no preload required):
CREATE EXTENSION pg_vector;
CREATE EXTENSION pg_geo;
CREATE EXTENSION pg_parquetio;
CREATE EXTENSION pg_unified;
CREATE EXTENSION pg_python;
CREATE EXTENSION pg_model_registry;
CREATE EXTENSION pg_objectstore;
CREATE EXTENSION pg_ontology;
CREATE EXTENSION pg_intent;
CREATE EXTENSION pg_context;""")

    doc.para("Verify all extensions:")
    doc.code("""\
SELECT name, default_version, installed_version
FROM   pg_available_extensions
WHERE  name LIKE 'alohadb%'
ORDER BY name;""")

    # ─────────────────────────────────────────────────
    # 7. Extension Reference
    # ─────────────────────────────────────────────────
    doc.h1("Extension Reference")

    # ── 7.1 pg_vector ──────────────────────────
    doc.h2("pg_vector -- Vector Similarity Search")
    doc.para(
        "Provides the VECTOR type, L2/cosine/dot-product distance operators, and an "
        "IVFFlat approximate nearest-neighbour (ANN) index using Lloyd's k-means clustering."
    )
    doc.h3("Create a vector index and search")
    doc.code("""\
CREATE EXTENSION pg_vector;

-- Create index (128 lists for ~100k vectors)
SELECT vector_index_create('products', 'embedding', 128);

-- Insert a 3-dimensional vector
SELECT vector_index_insert('products_embedding_idx',
       42, '[0.1, 0.9, 0.4]'::vector);

-- Train the index (must be called before first search)
SELECT vector_index_rebuild('products_embedding_idx');

-- Approximate nearest-neighbour search
SELECT entry_id, distance
FROM   vector_index_search('products_embedding_idx',
                           '[0.1, 0.8, 0.5]'::vector, 5)
ORDER BY distance;""")

    doc.h3("Distance operators")
    doc.code("""\
SELECT vector_l2_distance('[1,2,3]'::vector, '[4,5,6]'::vector);
SELECT vector_cosine_distance('[1,0,0]'::vector, '[0,1,0]'::vector);
SELECT vector_neg_dot_product('[1,2]'::vector, '[3,4]'::vector);""")

    fn_vec = [
        ("vector_index_create(table, col, n_lists)", "Create IVFFlat index catalog entry"),
        ("vector_index_rebuild(index_name)",         "Train centroids via Lloyd's k-means"),
        ("vector_index_insert(index, id, vec)",      "Insert/update vector entry"),
        ("vector_index_search(index, vec, k)",       "ANN search returning (id, distance)"),
        ("vector_index_drop(index_name)",            "Remove index and all entries"),
        ("vector_index_stats(index_name)",           "Lists, entries, trained flag, dims"),
        ("vector_dims(v)",                           "Number of dimensions"),
        ("vector_l2_distance(a, b)",                 "Euclidean L2 distance"),
        ("vector_cosine_distance(a, b)",             "Cosine distance (1 - cosine_similarity)"),
        ("vector_neg_dot_product(a, b)",             "Negative dot product (for max-sim)"),
    ]
    doc.table(["Function", "Description"], fn_vec, [90, 80])

    # ── 7.2 pg_geo ─────────────────────────────
    doc.h2("pg_geo -- Geospatial Operations")
    doc.para(
        "Four native types (GeoPoint, GeoBBox, GeoPolygon, GeoCircle) with ST_-prefixed "
        "spatial functions including Haversine great-circle distance and ray-casting "
        "point-in-polygon."
    )
    doc.code("""\
CREATE EXTENSION pg_geo;

-- Types
SELECT '40.7128,-74.0060'::geo_point AS nyc;
SELECT '-74.1,40.6,-73.9,40.8'::geo_bbox AS bbox;
SELECT 'POLYGON((-74 40,-73 40,-73 41,-74 41,-74 40))'::geo_polygon;

-- Great-circle distance NYC -> LAX (metres)
SELECT geo_st_distance_sphere(
    '40.7128,-74.0060'::geo_point,
    '34.0522,-118.2437'::geo_point) AS dist_m;

-- Point in polygon
SELECT geo_st_contains(
    'POLYGON((-74 40,-73 40,-73 41,-74 41,-74 40))'::geo_polygon,
    '40.7,-73.9'::geo_point);

-- Bounding box of a polygon
SELECT geo_st_bbox('POLYGON((0 0,1 0,1 1,0 1,0 0))'::geo_polygon);""")

    fn_geo = [
        ("geo_st_distance(a, b)",          "Planar distance between two GeoPoints"),
        ("geo_st_distance_sphere(a, b)",   "Haversine great-circle distance (metres)"),
        ("geo_st_contains(poly, pt)",      "True if polygon contains point"),
        ("geo_st_within(pt, poly)",        "Alias for st_contains(poly, pt)"),
        ("geo_st_intersects_bbox(a, b)",   "True if two bounding boxes overlap"),
        ("geo_st_area(poly)",              "Shoelace signed area of polygon"),
        ("geo_st_bbox(poly)",              "Bounding box of polygon"),
        ("geo_st_buffer(pt, radius)",      "Approximate circular bounding box"),
        ("geo_make_point(x, y)",           "Construct GeoPoint from floats"),
        ("geo_make_bbox(xmin,ymin,xmax,ymax)", "Construct GeoBBox from four floats"),
    ]
    doc.table(["Function", "Description"], fn_geo, [90, 80])

    # ── 7.3 pg_parquetio ──────────────────────────
    doc.h2("pg_parquetio -- Parquet / CSV / JSON File Reader")
    doc.para(
        "Native Parquet v1.1 reader with Snappy decompression and PLAIN/DICTIONARY "
        "encoding support. Also provides read_csv and read_json set-returning functions."
    )
    doc.code("""\
CREATE EXTENSION pg_parquetio VERSION '1.1';

-- Read a Parquet file
SELECT * FROM read_parquet('/data/sales.parquet');

-- With options (column filter example)
SELECT * FROM read_parquet('/data/sales.parquet', '{"columns":["id","amount"]}');

-- CSV
SELECT * FROM read_csv('/tmp/data.csv');

-- JSON Lines
SELECT * FROM read_json('/tmp/events.jsonl');

-- Upgrade from 1.0 to 1.1
ALTER EXTENSION pg_parquetio UPDATE TO '1.1';""")

    # ── 7.4 pg_unified ──────────────────────────
    doc.h2("pg_unified -- Streaming + Batch Unified View")
    doc.para(
        "Combines a batch (cold) table with a streaming event buffer into a single SQL "
        "view via UNION ALL. Supports ingest, flush, and purge."
    )
    doc.code("""\
CREATE EXTENSION pg_unified;

CREATE TABLE orders (id bigserial PRIMARY KEY, customer text, amount numeric, status text);
INSERT INTO orders VALUES (1,'alice',99,'completed'),(2,'bob',149,'completed');

-- Register unified source
SELECT unified_register('orders_stream', 'orders', 'public', 'orders_unified');

-- Stream new events
SELECT unified_ingest('orders_stream',
    '{"id":3,"customer":"carol","amount":200,"status":"pending"}'::jsonb);

-- Query unified view (batch + buffer)
SELECT count(*) FROM orders_unified;

-- Flush buffer into batch table
SELECT unified_flush('orders_stream') AS rows_flushed;

-- Stats
SELECT * FROM unified_stats('orders_stream');""")

    # ── 7.5 pg_pipeline ────────────────────────
    doc.h2("pg_pipeline -- DAG Task Orchestration")
    doc.para(
        "Declare pipelines as directed acyclic graphs (DAGs) of SQL tasks with "
        "dependency edges. A background worker uses Kahn's topological sort to "
        "dispatch ready tasks up to pipeline_max_parallel concurrently."
    )
    doc.code("""\
CREATE EXTENSION pg_pipeline;

-- Create pipeline
SELECT pipeline_create('nightly_etl', 'Nightly data pipeline');

-- Add tasks
SELECT pipeline_task_add('nightly_etl', 'extract',
    'INSERT INTO stage SELECT * FROM src', NULL);
SELECT pipeline_task_add('nightly_etl', 'transform',
    'UPDATE stage SET amount = amount * 1.1', NULL);
SELECT pipeline_task_add('nightly_etl', 'load',
    'INSERT INTO final SELECT * FROM stage', NULL);

-- Add dependencies (extract -> transform -> load)
SELECT pipeline_dep_add('nightly_etl', 'transform', 'extract');
SELECT pipeline_dep_add('nightly_etl', 'load', 'transform');

-- Start a run
SELECT pipeline_run_start('nightly_etl') AS run_id;

-- Check status
SELECT * FROM pipeline_run_status(1);""")

    fn_pipe = [
        ("pipeline_create(name, desc)",           "Register new pipeline"),
        ("pipeline_task_add(pipe, task, sql, w)",  "Add SQL task with optional worker type"),
        ("pipeline_dep_add(pipe, task, dep)",      "Add dependency edge"),
        ("pipeline_run_start(pipe)",               "Start a pipeline run, returns run_id"),
        ("pipeline_run_status(run_id)",            "Task statuses for a run"),
        ("pipeline_run_cancel(run_id)",            "Cancel a running pipeline"),
        ("pipeline_run_retry(run_id)",             "Retry failed tasks"),
        ("pipeline_list()",                        "List all pipelines"),
    ]
    doc.table(["Function", "Description"], fn_pipe, [90, 80])

    # ── 7.6 pg_kafka ───────────────────────────
    doc.h2("pg_kafka -- Kafka / Kinesis Consumer")
    doc.para(
        "Background worker that polls Kafka topics (or AWS Kinesis shards) and stores "
        "messages in pg_kafka_messages. Requires librdkafka for Kafka support."
    )
    doc.code("""\
CREATE EXTENSION pg_kafka;

-- Register a Kafka consumer
SELECT kafka_consumer_register(
    'events_consumer',
    'kafka',
    'broker1:9092,broker2:9092',
    'events-topic',
    'alohadb-group-1'
);

-- Start consuming
SELECT kafka_consumer_start('events_consumer');

-- Check status
SELECT * FROM kafka_consumer_status();

-- Query messages
SELECT msg_id, received_at, payload
FROM   kafka_messages('events_consumer')
ORDER BY received_at DESC LIMIT 10;

-- Purge consumed messages
SELECT kafka_consumer_purge('events_consumer');""")

    # ── 7.7 pg_objectstore ─────────────────────
    doc.h2("pg_objectstore -- S3 / GCS / Azure Object Store")
    doc.para(
        "Unified object storage API for AWS S3 (SigV4), Google Cloud Storage (OAuth2 JWT), "
        "and Azure Blob Storage (SAS tokens). Requires libcurl and OpenSSL."
    )
    doc.code("""\
CREATE EXTENSION pg_objectstore;

-- Register S3 credentials
SELECT objectstore_register_credential(
    'my_s3',
    's3',
    '{
       "access_key": "AKIAIOSFODNN7EXAMPLE",
       "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
       "region":     "us-east-1"
     }'::jsonb
);

-- Upload an object
SELECT objectstore_put('my_s3', 's3://my-bucket/data/report.csv',
                       pg_read_binary_file('/tmp/report.csv'));

-- Download
SELECT objectstore_get_text('my_s3', 's3://my-bucket/data/report.csv');

-- List objects
SELECT key, size_bytes FROM objectstore_list('my_s3', 's3://my-bucket/data/');

-- Delete
SELECT objectstore_delete('my_s3', 's3://my-bucket/data/report.csv');""")

    # ── 7.8 pg_python ──────────────────────────
    doc.h2("pg_python -- Inline Python Scripting")
    doc.para(
        "Execute Python 3 code inline, or register named scripts for reuse. "
        "Requires PostgreSQL built with plpython3u."
    )
    doc.code("""\
CREATE EXTENSION pg_python;

-- Inline eval
SELECT python_eval('_result = 2 ** 10') AS power;
SELECT python_eval('_result = ",".join(["a","b","c"])') AS joined;

-- Pass JSON args
SELECT python_eval(
    '_args_data = __import__("json").loads(_args); _result = str(_args_data["x"] + _args_data["y"])',
    '{"x": 10, "y": 32}'
) AS answer;

-- Register named script
SELECT python_register('greet', $py$
import json
args = json.loads(_args) if _args else {}
name = args.get("name", "World")
_result = f"Hello, {name}!"
$py$);

-- Execute named script
SELECT python_exec('greet', '{"name": "AlohaDB"}') AS greeting;

-- List registered scripts
SELECT script_name, version_num FROM python_list();""")

    # ── 7.9 pg_model_registry ──────────────────
    doc.h2("pg_model_registry -- ML Model Registry")
    doc.para(
        "Version-controlled registry for machine learning models. Track versions, "
        "deployments, experiments, and parameter sets in PostgreSQL."
    )
    doc.code("""\
CREATE EXTENSION pg_model_registry;

-- Register a model
SELECT model_register('fraud_detector', 'xgboost', 'XGBoost fraud detection model');

-- Add a version
SELECT model_version_add('fraud_detector', '1.0.0',
    '{"n_estimators":200,"max_depth":6,"learning_rate":0.1}'::jsonb,
    '/models/fraud_v1.pkl');

-- Deploy a version
SELECT model_deploy('fraud_detector', '1.0.0');

-- Compare versions
SELECT * FROM model_compare('fraud_detector', '1.0.0', '1.1.0');

-- Log an experiment run
SELECT experiment_log('fraud_detector', 'grid_search_2026',
    '{"n_estimators":300}'::jsonb,
    '{"auc":0.97,"precision":0.91}'::jsonb);

-- List models
SELECT model_name, framework, deployed_version FROM model_list();""")

    # ── 7.10 pg_ontology ───────────────────────
    doc.h2("pg_ontology -- RDF Triple Store")
    doc.para(
        "Store typed subject/predicate/object triples for knowledge graphs, ontologies, "
        "and entity relationship networks."
    )
    doc.code("""\
CREATE EXTENSION pg_ontology;

-- Assert triples
SELECT ontology_assert('Apple_Inc', 'type',         'Company');
SELECT ontology_assert('Apple_Inc', 'founded',      '1976');
SELECT ontology_assert('Apple_Inc', 'CEO',          'Tim_Cook');
SELECT ontology_assert('iPhone',    'madeBy',       'Apple_Inc');
SELECT ontology_assert('iPhone',    'type',         'Product');

-- Query
SELECT object FROM ontology_query('Apple_Inc', 'type',   NULL);
SELECT object FROM ontology_query('iPhone',    'madeBy', NULL);

-- Wildcard: everything Apple_Inc says
SELECT predicate, object FROM ontology_query('Apple_Inc', NULL, NULL);

-- Graph traversal (follow madeBy links)
SELECT * FROM ontology_traverse('iPhone', 'madeBy', 3);

-- Stats
SELECT * FROM ontology_stats();""")

    # ── 7.11 pg_intent ─────────────────────────
    doc.h2("pg_intent -- Intent Classification")
    doc.para(
        "Keyword-based intent registration and document tagging with hierarchical "
        "parent-child intents and automatic scoring inference."
    )
    doc.code("""\
CREATE EXTENSION pg_intent;

-- Register intents
SELECT intent_register('purchase', 'Buy/order intent',
    ARRAY['buy','order','checkout','price'], NULL);
SELECT intent_register('complaint', 'Complaint intent',
    ARRAY['broken','refund','return','damaged'], 'customer_service');

-- Manually tag a document
SELECT intent_tag(1, 'I want to buy a laptop', 'purchase');

-- Infer intent from text (keyword scoring)
SELECT intent_name, round(score::numeric, 3) AS score
FROM   intent_infer('I need to return this broken item')
WHERE  score > 0
ORDER BY score DESC LIMIT 5;

-- Auto-tag using inference
SELECT intent_auto_tag(5, 'Amazing product, love it!');

-- Find all documents tagged as complaints
SELECT doc_id FROM intent_find('complaint') ORDER BY doc_id;

-- Ancestor chain
SELECT ancestor, depth FROM intent_ancestors('complaint') ORDER BY depth;""")

    # ── 7.12 pg_context ────────────────────────
    doc.h2("pg_context -- Context Disambiguation")
    doc.para(
        "Resolves ambiguous terms to contextual meanings. Register context profiles "
        "with aliases and select the best match for a given context label."
    )
    doc.code("""\
CREATE EXTENSION pg_context;

-- Register context profiles
SELECT context_register('apple', 'technology',
    '{"aliases":["Apple Inc","AAPL","iPhone maker"],"description":"Tech company"}'::jsonb);
SELECT context_register('apple', 'fruit',
    '{"aliases":["Malus domestica","red apple"],"description":"Edible fruit"}'::jsonb);

-- Resolve term in context
SELECT * FROM context_resolve('apple', 'technology');

-- Batch disambiguate a list of terms
SELECT * FROM context_disambiguate(
    ARRAY['apple','python','mercury'], 'science');

-- Show disambiguation history
SELECT term, chosen_context FROM context_history('apple');""")

    # ─────────────────────────────────────────────────
    # 8. PostgreSQL Built-in Reference
    # ─────────────────────────────────────────────────
    doc.h1("PostgreSQL 18 Built-in Reference")
    doc.para(
        "AlohaDB is PostgreSQL 18. All standard SQL features, built-in functions, "
        "and extensions shipped with PostgreSQL are available."
    )

    doc.h2("JSON / JSONB")
    doc.code("""\
-- Object construction and extraction
SELECT jsonb_build_object('name','Alice','age',30);
SELECT '{"tags":["a","b"]}'::jsonb -> 'tags' -> 0;
SELECT '{"user":{"id":1}}'::jsonb #>> '{user,id}';

-- Aggregation
SELECT jsonb_agg(row_to_json(t)) FROM my_table t;
SELECT jsonb_object_agg(key_col, val_col) FROM kv;

-- Containment and existence
SELECT '{"a":1,"b":2}'::jsonb @> '{"a":1}';
SELECT '{"a":1}'::jsonb ? 'a';

-- Expansion
SELECT * FROM jsonb_each('{"x":1,"y":2}'::jsonb);
SELECT * FROM jsonb_array_elements('[1,2,3]'::jsonb);""")

    doc.h2("Full-Text Search")
    doc.code("""\
-- tsvector / tsquery
SELECT to_tsvector('english', 'The quick brown fox');
SELECT to_tsquery('english', 'quick & fox');

-- Match
SELECT title FROM articles
WHERE  to_tsvector('english', body) @@ to_tsquery('english', 'database & analytics');

-- Ranking
SELECT title, ts_rank(to_tsvector('english', body),
                      to_tsquery('english', 'analytics')) AS rank
FROM   articles ORDER BY rank DESC;

-- GIN index for fast FTS
CREATE INDEX articles_fts_idx ON articles
USING  gin(to_tsvector('english', body));""")

    doc.h2("Window Functions")
    doc.code("""\
SELECT
    customer,
    amount,
    row_number()    OVER (PARTITION BY customer ORDER BY amount DESC),
    rank()          OVER (PARTITION BY customer ORDER BY amount DESC),
    dense_rank()    OVER w,
    lag(amount, 1)  OVER w AS prev_amount,
    lead(amount, 1) OVER w AS next_amount,
    sum(amount)     OVER w AS running_total,
    avg(amount)     OVER (PARTITION BY customer) AS avg_spend
FROM orders
WINDOW w AS (PARTITION BY customer ORDER BY order_date);""")

    doc.h2("CTEs and Recursive Queries")
    doc.code("""\
-- Standard CTE
WITH monthly AS (
    SELECT date_trunc('month', order_date) AS month,
           sum(amount) AS revenue
    FROM   orders
    GROUP BY 1
)
SELECT month, revenue,
       revenue - lag(revenue) OVER (ORDER BY month) AS delta
FROM   monthly;

-- Recursive CTE (org chart traversal)
WITH RECURSIVE org AS (
    SELECT id, name, manager_id, 0 AS depth
    FROM   employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, o.depth + 1
    FROM   employees e JOIN org o ON e.manager_id = o.id
)
SELECT depth, name FROM org ORDER BY depth, name;""")

    doc.h2("Useful System Queries")
    doc.code("""\
-- Installed extensions
SELECT name, installed_version FROM pg_extension;

-- Table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
FROM   pg_class WHERE relkind = 'r' ORDER BY 2 DESC;

-- Active queries
SELECT pid, state, query_start, query FROM pg_stat_activity
WHERE  state = 'active';

-- Long-running queries (> 30 s)
SELECT pid, now() - query_start AS duration, query
FROM   pg_stat_activity
WHERE  state = 'active' AND now() - query_start > interval '30 seconds';

-- Lock waits
SELECT blocked.pid, blocked.query, blocking.pid AS blocking_pid
FROM   pg_stat_activity blocked
JOIN   pg_stat_activity blocking
       ON  blocking.pid = ANY(pg_blocking_pids(blocked.pid));""")

    # ─────────────────────────────────────────────────
    # 9. GUC Configuration
    # ─────────────────────────────────────────────────
    doc.h1("GUC Configuration Reference")
    doc.para(
        "Set these parameters in postgresql.conf or with SET / ALTER SYSTEM SET. "
        "Parameters marked (*) require a server restart."
    )
    guc_rows = [
        ("pg_pipeline.check_interval", "5",    "Seconds between pipeline scheduler ticks"),
        ("pg_pipeline.max_parallel",   "4",    "Max tasks dispatched simultaneously"),
        ("pg_pipeline.database",       "postgres", "Database for pipeline worker"),
        ("pg_kafka.poll_interval_ms",  "500",  "Kafka consumer poll interval (ms)"),
        ("pg_kafka.database",          "postgres", "Database for Kafka worker"),
        ("pg_cronjobs.check_interval",     "10",   "Seconds between cron scheduler ticks (*)"),
        ("pg_cronjobs.database",           "postgres", "Database for cron worker (*)"),
        ("pg_vector.ivfflat_probes",   "8",    "IVFFlat search probe count"),
        ("pg_vector.max_dims",         "2000", "Maximum vector dimensions"),
        ("pg_parquetio.allowed_dirs",    "/data,/tmp", "Comma-separated allowed paths"),
        ("pg_objectstore.timeout_ms",  "30000","libcurl request timeout (ms)"),
    ]
    doc.table(["GUC Parameter", "Default", "Description"], guc_rows, [75, 22, 73])

    # ─────────────────────────────────────────────────
    # 10. Testing
    # ─────────────────────────────────────────────────
    doc.h1("Testing")

    doc.h2("Regression Tests (meson)")
    doc.para(
        "Each extension ships with SQL regression tests under contrib/<ext>/sql/. "
        "Run all tests with:"
    )
    doc.code("""\
cd alohadb-20.0
meson setup build
ninja -C build
meson test -C build --suite regression -v""")

    doc.para("Run a single extension's tests:")
    doc.code("""\
meson test -C build pg_vector/regression
meson test -C build pg_geo/regression""")

    doc.h2("Smoke Test")
    doc.code("""\
psql -U postgres <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_vector;
SELECT vector_l2_distance('[1,2,3]'::vector,'[4,5,6]'::vector) AS dist;
DROP EXTENSION pg_vector;
SQL""")

    doc.h2("Performance Benchmark")
    doc.code("""\
-- Generate 100k random vectors and measure IVFFlat build + query time
CREATE TABLE bench_vecs (id bigserial, v vector(128));
INSERT INTO bench_vecs (v)
SELECT ('[' || array_to_string(
    ARRAY(SELECT round(random()::numeric, 4) FROM generate_series(1,128)), ',') || ']')::vector
FROM generate_series(1, 100000);

SELECT vector_index_create('bench_vecs', 'v', 256);
\\timing on
SELECT vector_index_rebuild('bench_vecs_v_idx');
SELECT entry_id, distance
FROM   vector_index_search('bench_vecs_v_idx', (SELECT v FROM bench_vecs LIMIT 1), 10);
\\timing off""")

    # ─────────────────────────────────────────────────
    # 11. Troubleshooting
    # ─────────────────────────────────────────────────
    doc.h1("Troubleshooting")

    issues = [
        (
            "Extension load fails: could not open extension control file",
            "The .control file was not installed. Verify the install prefix matches "
            "$PGDATA/../share/postgresql/extension/. Re-run: sudo ninja -C build install",
        ),
        (
            "Shared library not found: libalohadb_vector.so",
            "The shared library directory is not in dynamic linker path. "
            "Run: sudo ldconfig, or add the lib directory to /etc/ld.so.conf.d/.",
        ),
        (
            "Background worker not starting (pipeline / kafka)",
            "Ensure shared_preload_libraries includes the extension name and that the "
            "server was restarted after editing postgresql.conf. Check: "
            "SELECT * FROM pg_stat_activity WHERE backend_type LIKE '%alohadb%';",
        ),
        (
            "pg_python: ERROR: language plpython3u does not exist",
            "PostgreSQL was not built with PL/Python3. Install the plpython3u package: "
            "apt install postgresql-plpython3-18 (Debian) or dnf install postgresql18-plpython3 (RHEL).",
        ),
        (
            "vector_index_search: ERROR: index not trained",
            "Call vector_index_rebuild(index_name) at least once before searching. "
            "Requires at least n_lists vectors to have been inserted.",
        ),
        (
            "read_parquet: ERROR: path not in allowed directories",
            "Set pg_parquetio.allowed_dirs = '/your/data/path' in postgresql.conf "
            "and reload: SELECT pg_reload_conf();",
        ),
        (
            "objectstore_put: ERROR: S3 auth failure (403 Forbidden)",
            "Check access_key and secret_key in the credential JSON. Verify the IAM "
            "policy includes s3:PutObject on the target bucket. Confirm the region is "
            "correct (region mismatch causes SigV4 signature rejection).",
        ),
        (
            "Pipeline tasks stuck in waiting state",
            "Check for circular dependencies: run pipeline_validate(name). Also verify "
            "that all upstream tasks completed successfully -- failed tasks do not "
            "unblock downstream tasks automatically. Use pipeline_run_retry(run_id).",
        ),
        (
            "Kafka consumer shows no messages",
            "Verify librdkafka was present at compile time (check pg_config --configure). "
            "Confirm broker addresses and topic name. Check consumer group offset: "
            "it may be at the end. Reset with: kafka-consumer-groups.sh --reset-offsets.",
        ),
    ]
    for title, body in issues:
        doc.h3(title)
        doc.para(body, indent=4)

    doc.h2("Diagnostic Queries")
    doc.code("""\
-- AlohaDB background workers
SELECT pid, backend_type, state FROM pg_stat_activity
WHERE  backend_type LIKE '%alohadb%';

-- Extension list with versions
SELECT name, default_version, installed_version
FROM   pg_available_extensions
WHERE  name LIKE 'alohadb%';

-- Recent PostgreSQL log (tail)
SELECT log_time, message FROM pg_log_file_settings;  -- find log path
-- then: tail -f /var/log/postgresql/postgresql-18-main.log""")

    # ─────────────────────────────────────────────────
    # 12. Release Notes
    # ─────────────────────────────────────────────────
    doc.h1("Release Notes and Downloads")

    doc.h2("AlohaDB 20.0 -- What's New")
    new_features = [
        "pg_vector: IVFFlat ANN index with Lloyd's k-means training",
        "pg_geo: Native geospatial types and 10 ST_ functions (Haversine, PIP, shoelace)",
        "pg_parquetio v1.1: Full column reader with Snappy decompression and DICTIONARY encoding",
        "pg_unified: Streaming + batch UNION ALL view with flush and purge",
        "pg_pipeline: DAG orchestration background worker with Kahn topological dispatch",
        "pg_kafka: Kafka/Kinesis consumer background worker (librdkafka)",
        "pg_objectstore: Unified S3/GCS/Azure API (SigV4, OAuth2 JWT, SAS tokens)",
        "pg_python: Inline Python scripting + named script registry",
        "pg_model_registry: ML model versioning, deployment, experiments",
        "pg_ontology: RDF triple store with graph traversal",
        "pg_intent: Keyword-based intent hierarchy and auto-tagging",
        "pg_context: Contextual disambiguation and batch resolution",
    ]
    for f in new_features:
        doc.bullet(f)

    doc.ln(3)
    doc.h2("Download Table")
    dl_rows = [
        ("Windows 10/11 x64",  "AlohaDB-20.0-win-x64.exe",       "Installer + PostgreSQL 18"),
        ("Linux x64 (deb)",    "alohadb-20.0-amd64.deb",          "Debian/Ubuntu"),
        ("Linux x64 (rpm)",    "alohadb-20.0-x86_64.rpm",         "RHEL/Rocky/Alma"),
        ("Linux ARM64 (deb)",  "alohadb-20.0-arm64.deb",          "Raspberry Pi / Ampere"),
        ("macOS ARM64",        "AlohaDB-20.0-macos-arm64.pkg",    "Apple Silicon (M1+)"),
        ("macOS x64",          "AlohaDB-20.0-macos-x64.pkg",      "Intel Mac"),
        ("Source tarball",     "alohadb-20.0.tar.gz",             "Build from source"),
    ]
    doc.table(["Platform", "Package", "Notes"], dl_rows, [40, 70, 60])

    doc.para(
        "All downloads are available at: https://www.OpenCan.ai/download"
    )

    doc.h2("License")
    doc.para(
        "AlohaDB is distributed under the PostgreSQL License, a permissive open-source "
        "license approved by the Open Source Initiative (OSI). You are free to use, "
        "copy, modify, and distribute AlohaDB for any purpose, with or without fee, "
        "provided that the copyright notice and this permission notice appear in all "
        "copies. THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND."
    )
    doc.para(
        "Copyright 2026 Vern AllWorks LLC. PostgreSQL is Copyright 1996-2026 "
        "The PostgreSQL Global Development Group."
    )


def main():
    doc = GuideDoc()

    # Build content (collects TOC entries)
    doc.cover()
    build_guide(doc)

    # Insert TOC after cover (page 2 area)
    # We can't insert retroactively with fpdf2 easily, so append at end
    doc.toc_page()

    doc.output(OUT_PATH)
    print(f"PDF written: {OUT_PATH}  ({os.path.getsize(OUT_PATH):,} bytes)")


if __name__ == "__main__":
    main()
