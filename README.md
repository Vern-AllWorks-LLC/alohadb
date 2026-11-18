# AlohaDB

AlohaDB is an AI-native database built on PostgreSQL 18 (currently based on 18.7). It extends standard PostgreSQL with a suite of purpose-built extensions for modern application development — including real-time change notifications, in-database ML inference, vector search, time-travel queries, columnar storage, GraphQL and REST APIs, natural-language-to-SQL, and encrypted audit logging. Every feature ships as a standard `CREATE EXTENSION` module, so it composes naturally with the PostgreSQL you already know. All AlohaDB extensions are MIT-licensed.

Since 21.0, AlohaDB uses the stock PostgreSQL program and file names (`postgres`, `psql`, `initdb`, `pg_ctl`, `postgresql.conf`, `pg_hba.conf`, ...), and every AlohaDB extension is named `pg_*`, so PostgreSQL tools, drivers, packaging scripts and tutorials work unchanged. Coming from AlohaDB 20.x? See [Names in AlohaDB 21 and upgrading from 20.x](#names-in-alohadb-21-and-upgrading-from-20x).

## AI, Vectors & Inference

### `pg_vector` — Vector type & ANN search

IVFFlat approximate nearest-neighbor indexes (k-means trained) and named distance functions on top of the built-in `vector` type, which ships in the AlohaDB core together with the `<->` (L2), `<=>` (cosine), `<#>` (negative inner product) and `<+>` (L1) operators.

The core also has built-in `hnsw` and `ivfflat` index access methods, ported from pgvector 0.8.7 (PostgreSQL License), so `CREATE INDEX ... USING hnsw (embedding vector_cosine_ops)` and `ORDER BY embedding <=> $1 LIMIT k` work as they do with pgvector: opclasses `vector_l2_ops`, `vector_ip_ops`, `vector_cosine_ops`, `vector_l1_ops` (hnsw), parallel builds, filtered search with iterative scans, and the `hnsw.ef_search`, `hnsw.iterative_scan`, `hnsw.max_scan_tuples`, `hnsw.scan_mem_multiplier`, `ivfflat.probes`, `ivfflat.iterative_scan` and `ivfflat.max_probes` settings. No extension is needed for these. As in pgvector, `integer[]`, `real[]`, `double precision[]` and `numeric[]` values cast to `vector` (assignment casts, so arrays can be inserted into `vector(n)` columns directly; the dimension count is checked).

```sql
CREATE INDEX ON docs USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
SET hnsw.ef_search = 100;
SET hnsw.iterative_scan = relaxed_order;          -- keep filtered queries full
SELECT id FROM docs WHERE lang = 'en' ORDER BY embedding <=> $1 LIMIT 10;
```

```sql
CREATE EXTENSION pg_vector;
SELECT vector_index_create('emb_idx', 'docs', 'embedding', 'id', 100, 10, 'cosine');
SELECT vector_index_rebuild('emb_idx');            -- train the index
SELECT vector_index_insert('emb_idx', 'doc-1', '[0.1,0.2,0.3]'::vector);
SELECT * FROM vector_index_search('emb_idx', '[0.1,0.2,0.3]'::vector, 5);
-- or use distance operators directly:
SELECT a <=> b AS cosine_dist FROM embeddings;
```

**Use case:** Power semantic search and RAG retrieval by finding the nearest embeddings to a query vector.

### `pg_embeddings` — Embedding lifecycle & hybrid retrieval

Keeps embeddings in sync with your tables: it records which model and version produced each vector, re-embeds rows in the background when their content changes, keeps several model versions side by side until you promote one, and enforces each model's dimensions. A single SQL statement combines vector similarity, PostgreSQL full-text ranking (fused with Reciprocal Rank Fusion) and a relational filter, under the caller's privileges and row-level security. Embedding is pluggable through any SQL function `fn(text) → vector`; binary and int8 quantization helpers support two-stage search.

```sql
CREATE EXTENSION pg_embeddings;
SELECT embedding_model_register('minilm', 'v1', 384, 'cosine', 'my_embed(text)');
SELECT embedding_track('docs', 'id', 'title || '' '' || body', 'minilm', 'v1');
SELECT embedding_process(500);                 -- schedule with pg_cronjobs
SELECT * FROM embedding_search_text('docs', 'reset my password', 5);
SELECT * FROM embedding_hybrid_search('docs', my_embed('reset password'),
       'reset password', 10, filter => $$lang = 'en'$$);
SELECT embedding_reembed('docs', 'minilm', 'v1', 'minilm', 'v2');
SELECT embedding_promote('docs', 'minilm@v2');
```

**Use case:** Run RAG retrieval and model upgrades without a separate pipeline: change a row and its embedding is refreshed; roll out a new model version next to the old one, then switch over.

### `pg_gpu` — GPU-offloaded vector ops

GPU-offloaded vector operations: batch distance, top-K, and matrix multiply for high-throughput embedding workloads.

```sql
CREATE EXTENSION pg_gpu;
SELECT pg_gpu_available();
SELECT * FROM pg_gpu_topk(
  '[0.1,0.2,0.3]'::vector,
  ARRAY['[0.1,0.0,0.2]'::vector, '[0.4,0.5,0.6]'::vector],
  1);
```

**Use case:** Accelerate large batch similarity searches by offloading distance math to the GPU.

### `pg_inference` — In-database ONNX inference

In-database ML inference using ONNX Runtime — load a model once and score rows without leaving SQL.

```sql
CREATE EXTENSION pg_inference;
SELECT pg_load_model('classifier', :model_bytes);
SELECT pg_infer('classifier', '[0.2,0.7,0.1]'::vector);
SELECT * FROM pg_list_models();
```

**Use case:** Run real-time fraud or churn scoring directly inside a query, co-located with the data.

### `pg_model_registry` — Model versioning & deployment

ML model versioning, deployment, and experiment tracking with rollback and metric comparison.

```sql
CREATE EXTENSION pg_model_registry;
SELECT model_register('churn', 'onnx', 'Churn predictor');
SELECT model_version_add('churn', :model_bytes, '{"auc":0.91}');
SELECT model_deploy('churn');
SELECT model_rollback('churn');
```

**Use case:** Track experiments and promote, compare, or roll back model versions in production.

### `pg_nl2sql` — Natural language to SQL

Natural language to SQL translation via an LLM API, with optional read-only execution and plain-English explanations.

```sql
CREATE EXTENSION pg_nl2sql;
SELECT pg_nl2sql('top 5 customers by revenue last month');
SELECT * FROM pg_nl2sql_execute('how many orders shipped today')
  AS t(order_count bigint);
```

**Use case:** Let non-technical users query data conversationally without writing SQL.

### `pg_federated` — Federated learning

Federated learning with Flower framework integration, training a shared model across nodes without centralizing data.

```sql
CREATE EXTENSION pg_federated;
SET pg_federated.flower_server = 'localhost:8080';
SET pg_federated.fl_training_query = 'SELECT features, label FROM training_data';
SELECT pg_fl_start();
SELECT * FROM pg_fl_status();
```

**Use case:** Collaboratively train ML models across data silos while keeping raw data in place.

### `pg_learned_stats` — ML cardinality estimation

ML-assisted cardinality estimation using gradient-boosted trees to improve the query planner's row estimates.

```sql
CREATE EXTENSION pg_learned_stats;
SELECT pg_learned_stats_train();
SELECT * FROM pg_learned_stats_status();
```

**Use case:** Improve plan quality for complex queries where standard statistics misestimate cardinality.

## Knowledge, Context & Semantics

### `pg_ontology` — Triple-store context graph

Triple-store context graph for entity and relationship mapping, with cycle-safe subclass hierarchies, bidirectional graph traversal, and a change history of every assertion and retraction.

```sql
CREATE EXTENSION pg_ontology;
SELECT ontology_assert('Honolulu', 'capital_of', 'Hawaii');
SELECT ontology_subclass('City', 'Place');
SELECT * FROM ontology_query('Honolulu', NULL, NULL);
SELECT * FROM ontology_related('Hawaii', 2);        -- entity, via_predicate, hops, path
SELECT ontology_is_a('City', 'Place');
SELECT * FROM ontology_history('Honolulu');
```

**Use case:** Model domain knowledge as subject-predicate-object facts and traverse relationships for reasoning.

### `pg_graph` — Property graphs over ordinary tables

Turns ordinary tables into a property graph: register vertex and edge labels, enforce the graph schema with real foreign keys and CHECK constraints (and report violations in existing data), then run BFS, Dijkstra shortest paths, time-respecting earliest-arrival paths, PageRank, connected components and degree in C. Results are sets, so they compose with CTEs, joins and window functions, and edges are read as the calling user, so privileges and row-level security apply. Edge labels with `valid_from`/`valid_to` columns support point-in-time (`as_of`) traversal, and seeded GraphSAGE-style uniform neighbour sampling plus neighbour-feature aggregation build GNN training sets that never see future edges.

```sql
CREATE EXTENSION pg_graph;
SELECT graph_create('social');
SELECT graph_add_vertex_label('social', 'person', 'person');
SELECT graph_add_edge_label('social', 'follows', 'follows', 'src', 'person', 'dst', 'person',
                            valid_from_column => 'since', valid_to_column => 'until');
SELECT * FROM graph_bfs('social', 42, 2, as_of => '2025-06-01');
SELECT * FROM graph_pagerank('social') ORDER BY rank DESC LIMIT 10;
SELECT * FROM graph_sample_neighbors('social', ARRAY[42, 7], ARRAY[10, 5],
                                     as_of => '2025-06-01', seed => 1);
```

**Use case:** Fraud rings, recommendation and supply-chain graphs kept in normal tables, queried with SQL, and sampled at a point in time to build leak-free GNN training sets.

### `pg_context` — Entity disambiguation & reranking

Entity disambiguation and context-aware search reranking. A term can have several registered senses; the surrounding text picks one by counting each sense's context cues (domain, type, keywords, and its `pg_ontology` neighbours). Requires `pg_ontology`.

```sql
CREATE EXTENSION pg_context CASCADE;   -- also installs pg_ontology
--                     term     entity          type        hint  prio domain    keywords
SELECT context_register('Apple', 'customer:42',   'customer', NULL, 100, 'sales',   ARRAY['invoice','contract']);
SELECT context_register('Apple', 'produce:apple', 'product',  NULL, 100, 'grocery', ARRAY['fruit','orchard']);
SELECT * FROM context_resolve('Apple', 'renew the Apple contract');   -- customer:42
SELECT * FROM context_rerank(:candidates, 'Apple invoice status');    -- boosts/penalises hits
```

**Use case:** Disambiguate homonyms and rerank search results based on contextual intent.

### `pg_intent` — Intent classification & why-indexing

Document intent classification and why-indexing — register intents, infer them from text, and auto-tag documents.

```sql
CREATE EXTENSION pg_intent;
SELECT intent_register('refund_request', 'Customer wants money back',
  ARRAY['refund','return','money back']);
SELECT * FROM intent_infer('I would like a refund please');
```

**Use case:** Classify incoming support tickets or documents by intent for routing and analytics.

### `pg_compat19` — PostgreSQL 19 function backports

Self-contained PostgreSQL 19 SQL additions for the 18.x base: `error_on_null()`, `tid_block()`/`tid_offset()`, `random(min, max)` for date/timestamp/timestamptz, `range_minus_multi()`/`multirange_minus_multi()`, and `uuid` ⇄ `bytea` casts. Drop it before upgrading a cluster to a PostgreSQL 19-based AlohaDB, where these are built in.

```sql
CREATE EXTENSION pg_compat19;
SELECT random('2026-01-01'::date, '2026-12-31'::date);
SELECT * FROM range_minus_multi('[1,10)'::int4range, '[3,5)'::int4range);
```

## Real-time, Streaming & Queues

### `pg_realtime` — Realtime change notifications

Trigger-based realtime change notifications built on LISTEN/NOTIFY, with subscriptions and event polling.

```sql
CREATE EXTENSION pg_realtime;
SELECT pg_realtime_subscribe('orders', 'status = ''shipped''');
SELECT * FROM pg_realtime_poll(0);
SELECT pg_realtime_cleanup('1 hour');
```

**Use case:** Push live updates to clients when rows change, powering dashboards and collaborative apps.

### `pg_queue` — Message queue with consumer groups

Durable message queue with consumer groups, visibility timeouts, acknowledgements, and retention.

```sql
CREATE EXTENSION pg_queue;
SELECT queue_create('jobs', '30 seconds', '7 days');
SELECT queue_send('jobs', '{"task":"resize"}');
SELECT * FROM queue_receive('jobs', 1);
SELECT queue_ack('jobs', 1);
```

**Use case:** Run a reliable background job queue without deploying a separate message broker.

### `pg_kafka` — Kafka & Kinesis consumer

Inbound streaming consumer for Apache Kafka and Amazon Kinesis that lands messages as queryable rows.

```sql
CREATE EXTENSION pg_kafka;
SELECT kafka_consumer_create('events', 'broker:9092', 'clicks');
SELECT kafka_consumer_start('events');
SELECT * FROM kafka_messages_poll('events', 1000);
```

**Use case:** Ingest event streams from Kafka or Kinesis directly into PostgreSQL for processing.

### `pg_cdc_arrow` — Arrow IPC change data capture

Logical decoding output plugin that emits change data capture in Arrow IPC format for efficient downstream consumption.

```sql
CREATE EXTENSION pg_cdc_arrow;
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'pg_cdc_arrow');
SELECT * FROM pg_logical_slot_get_binary_changes('cdc_slot', NULL, NULL);
```

**Use case:** Stream row changes into Arrow-native analytics pipelines and data lakes with minimal serialization overhead.

### `pg_unified` — Unified streaming + batch model

Unified streaming and batch query model over the same relation — a view UNIONs the durable batch table with an unflushed event buffer.

```sql
CREATE EXTENSION pg_unified;
CREATE TABLE clicks_batch (url text);
SELECT unified_register('clicks', 'clicks_batch');
SELECT unified_ingest('clicks', '{"url":"/home"}');
SELECT * FROM clicks_unified;
SELECT unified_flush('clicks');
```

**Use case:** Query fresh streaming events and historical batch data through a single consistent relation.

## Storage, Formats & Tiering

### `pg_columnar` — Columnar table storage

Columnar storage table access method with per-column zstd compression for analytical workloads.

```sql
CREATE EXTENSION pg_columnar;
CREATE TABLE events_col (id bigint, ts timestamptz, payload jsonb) USING columnar;
SELECT * FROM pg_columnar_info('events_col');
```

**Use case:** Store large analytical tables compressed and scan only the columns a query needs.

### `pg_ndarray` — Chunked N-d arrays with server-side math

Zarr/HDF5-style N-dimensional arrays stored as ordinary tables: a regular grid of zstd-compressed, row-major chunks (float32/float64/int32/int64) where unwritten chunks read as a fill value. Slice with start/stop/step into `float8[]` or the core `tensor` type, write regions with row-level chunk locking, and run streaming reductions, per-axis reductions, elementwise map/binary ops and blocked matmul without materialising the whole array. Also adds `tensor_slice` / `tensor_index` for the core `tensor` type.

```sql
CREATE EXTENSION pg_ndarray;
SELECT ndarray_create('temps', 'float32', '{365,180,360}', '{30,90,90}', fill_value => 'NaN');
SELECT ndarray_write('temps', '{0,0,0}', :day0_grid);                  -- float8[] region
SELECT ndarray_read('temps', '{0,40,100}', '{31,50,120}', '{1,1,2}');  -- strided slice
SELECT ndarray_reduce('temps', 'mean', '{0,40,100}', '{31,50,120}');
SELECT ndarray_map('temps', 'temps_f', 'mul', 1.8);                   -- chunkwise into a new array
SELECT * FROM ndarray_storage('temps');                                -- chunks, bytes, ratio
```

**Use case:** Keep large scientific, imaging or ML feature arrays inside the database and compute slices and statistics where the data lives.

### `pg_parquetio` — Parquet, CSV & JSON reader; Parquet & Arrow writer

Read Parquet, CSV and JSON files directly as sets of `jsonb` rows, and export any query straight to Parquet or a memory-mappable Arrow IPC file that PyTorch/JAX dataloaders, PyArrow, Polars, DuckDB and lakehouse engines read without an ETL round trip. Writing requires `pg_write_server_files` and a directory listed in `pg_parquetio.allowed_paths`; `arrow_ipc_stream()` returns Arrow stream messages to the client instead.

```sql
CREATE EXTENSION pg_parquetio;
SELECT * FROM read_parquet('/data/sales.parquet');
SELECT * FROM read_csv('/data/import.csv', ',', true);
SELECT write_parquet('SELECT id, ts, embedding FROM events', '/data/events.parquet', 'snappy');
SELECT write_arrow('SELECT id, embedding FROM items', '/data/items.arrow');
```

**Use case:** Query files on disk in place, and hand query results to ML training and lakehouse tools as Parquet/Arrow without a separate ETL step.

### `pg_objectstore` — S3, GCS & Azure storage

Cloud object storage reader and writer for S3, GCS, and Azure Blob, with credential management.

```sql
CREATE EXTENSION pg_objectstore;
SELECT objectstore_credential_register('s3', 'prod', '{"region":"us-west-2"}');
SELECT objectstore_put('s3://bucket/report.txt', 'hello'::bytea, 'prod');
SELECT * FROM objectstore_list('s3://bucket/', 'prod');
```

**Use case:** Read and write backups, exports, and data files in cloud object storage from SQL.

### `pg_tiering` — Automatic storage tiering

Automatic storage tiering for partitioned tables, moving aged partitions to a colder tablespace by time-based rules.

```sql
CREATE EXTENSION pg_tiering;
INSERT INTO pg_tiering_rules(parent_table, age_threshold, target_tablespace)
  VALUES ('events', '90 days', 'cold_ts');
SELECT pg_tiering_check_now();
SELECT * FROM pg_tiering_status();
```

**Use case:** Keep hot data on fast storage while automatically demoting old partitions to cheaper disks.

### `pg_nosql` — Document database API

Document database API with flexible, optionally schema-validated JSON collections. Filters match exact values and arrays by containment, or use operator objects (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `like`, `contains`, `exists`); search options support `sort`, `fields`, `limit` and `offset`.

```sql
CREATE EXTENSION pg_nosql;
SELECT doc_create_collection('users');
SELECT doc_insert('users', '{"name":"Kai","city":"Hilo","age":31,"tags":["surf"]}');
SELECT * FROM doc_search('users', '{"city":"Hilo"}');
SELECT * FROM doc_search('users', '{"age":{"gte":21},"tags":["surf"]}',
                         '{"sort":{"age":"desc"},"fields":["name","age"],"limit":10}');
```

**Use case:** Build document-oriented features with a MongoDB-style API backed by PostgreSQL durability.

### `pg_cache` — Transactional shared-memory key-value cache

A key-value cache in shared memory that stays consistent with your transactions. A cache write made inside a transaction is applied when that transaction commits and thrown away if it rolls back (subtransactions included), so a table update and the cache entry that mirrors it change together or not at all. That removes the classic stale-cache bug of database + Redis setups, which cannot know when the database commits. The writing transaction sees its own pending writes.

- Values are `jsonb` (`cache_set` / `cache_get`) or `text` (`cache_set_text` / `cache_get_text`) of any size up to a quarter of the cache, with optional TTLs (`cache_expire`, `cache_ttl`, `cache_purge_expired`).
- Memory is hard-capped by `pg_cache.max_memory` (default 32MB) and `pg_cache.max_entries` (default 10000), so the cache never competes with `shared_buffers`. When full it evicts with SIEVE (NSDI '24), which keeps a higher hit ratio than LRU and lets cache hits run under a shared lock.
- `cache_add` (store if absent, like Redis `SET NX`) and `cache_incr` (atomic counter, like `INCR`) take effect immediately, like `nextval()`, so they work for locks, rate limits and idempotency keys.
- `cache_keys(pattern)` lists keys; `cache_stats()` reports entries, memory, hits, misses, evictions and expirations.
- `SET pg_cache.transactional = off` makes writes immediate (classic cache behaviour).
- Requires `shared_preload_libraries = 'pg_cache'`. The cache is shared by all databases of the server and is empty after a restart. Functions are revoked from `PUBLIC`; grant `EXECUTE` to the roles that need them.

```sql
CREATE EXTENSION pg_cache;
BEGIN;
UPDATE users SET name = 'Kai' WHERE id = 42;
SELECT cache_set('user:42', to_jsonb(u), '10 minutes') FROM users u WHERE id = 42;
COMMIT;                                        -- row and cache change together
SELECT cache_get('user:42');
SELECT cache_add('lock:job:7', 'worker-3', '30 seconds');   -- true for one caller only
SELECT cache_incr('hits:/home');
SELECT * FROM cache_stats();
```

**Use case:** Cache rendered objects, sessions and counters next to the data they come from, without cache invalidation bugs or a separate Redis deployment.

### `pg_kv` — Key-value API on an unlogged table

A Redis-style key-value API (`kv_set`, `kv_get`, `kv_get_many`, `kv_del`, `kv_add`, `kv_incr`, `kv_expire`, `kv_ttl`, `kv_keys`, `kv_purge_expired`, `kv_flush`) over an unlogged table. Writes skip the WAL, every operation is an ordinary transactional SQL statement, and the table is emptied after a crash, as a cache should be. No preloading is needed, data survives clean restarts, and there is no memory cap other than disk. It is slower than `pg_cache` under heavy update load (each write is a row version that vacuum has to clean up), which makes it a good fit for session stores, idempotency keys and feature flags at moderate rates.

```sql
CREATE EXTENSION pg_kv;
SELECT kv_set('session:9f2c', '{"user":7}', '30 minutes');
SELECT kv_get('session:9f2c');
SELECT kv_incr('signups:2026-11-18');
SELECT cron_schedule('*/5 * * * *', 'SELECT kv_purge_expired()');   -- pg_cronjobs
```

**Use case:** Durable-until-crash key-value data with TTLs, queried and joined with SQL, without running a separate store.

## Query, API & Interfaces

### `pg_gql` — Auto-generated GraphQL API

Auto-generated GraphQL API derived from the database schema, executed entirely in SQL.

```sql
CREATE EXTENSION pg_gql;
SELECT graphql_execute('{ users(limit: 5) { id name } }');
SELECT graphql_schema();
```

**Use case:** Expose your tables as a GraphQL endpoint without writing resolver code.

### `pg_restapi` — Auto-generated REST API

Auto-generated REST API served by a background-worker HTTP server, also callable from SQL.

```sql
CREATE EXTENSION pg_restapi;
SELECT pg_restapi_handle('GET', '/api/users');
SELECT * FROM pg_restapi_endpoints();
```

**Use case:** Stand up a CRUD REST API over your schema with no application server.

### `pg_http` — HTTP client

An HTTP client for PostgreSQL, supporting GET/POST/PUT/DELETE and arbitrary methods with headers and timeouts.

```sql
CREATE EXTENSION pg_http;
SELECT status, body FROM http_get('https://api.example.com/health');
SELECT status FROM http_post('https://api.example.com/hook', '{"x":1}');
```

**Use case:** Call external webhooks and APIs from triggers or scheduled jobs inside the database.

### `pg_ftsearch` — BM25, fuzzy, phonetic & geo search

Full-text relevance with BM25 scoring, fuzzy and phonetic matching, autocomplete, and geo proximity search.

```sql
CREATE EXTENSION pg_ftsearch;
SELECT search_bm25(to_tsvector(body), to_tsquery('database')) FROM articles;
SELECT search_fuzzy_match('alohadb', 'alohdb', 2);
SELECT * FROM search_nearby(21.3, -157.8, 5000, 'stores', 'lat', 'lon');
```

**Use case:** Build relevance-ranked, typo-tolerant, location-aware search over your tables.

### `pg_chart` — SVG charts from SQL

Generate SVG charts (bar, line, pie, scatter, area) directly from a SQL query.

```sql
CREATE EXTENSION pg_chart;
SELECT chart_bar('SELECT region, sum(amount) FROM sales GROUP BY region',
  'Sales by Region');
```

**Use case:** Embed ready-to-render charts in reports and emails without a charting library.

### `pg_vecexec` — Vectorized query acceleration

Analytical query acceleration using vectorized (batched) execution, with explain and benchmark helpers.

```sql
CREATE EXTENSION pg_vecexec;
SELECT * FROM vectorize_query('SELECT region, sum(amt) FROM sales GROUP BY region')
  AS t(region text, total numeric);
SELECT * FROM vectorize_benchmark('SELECT count(*) FROM sales', 10);
```

**Use case:** Speed up large aggregation queries by processing rows in batches.

## Time, Temporal & Series

### `pg_temporal` — System-versioned time-travel

SQL:2011-style system-versioned temporal tables with time-travel queries over historical row versions.

```sql
CREATE EXTENSION pg_temporal;
SELECT pg_enable_system_versioning('orders');
SELECT * FROM pg_as_of('orders', '2026-01-01') AS t(id int, status text);
SELECT * FROM pg_versions_between('orders', '2026-01-01', '2026-02-01')
  AS t(id int, status text);
```

**Use case:** Audit how a record looked at any past point in time without manual history tables.

### `pg_bitemporal` — Bitemporal tables

SQL:2011-style bitemporal tables that track two timelines per row: when a fact is true in the real world (`valid_from`/`valid_to`) and when the database recorded it (`sys_from`/`sys_to`, managed by triggers). Superseded versions move to a `<tbl>_history` table, overlapping valid periods per key are rejected, sequenced `FOR PORTION OF`-style updates and deletes split or trim periods automatically, and `bitemporal_as_of` answers "what did we believe on date X about date Y".

```sql
CREATE EXTENSION pg_bitemporal;
SELECT bitemporal_enable('price', ARRAY['product_id']);
INSERT INTO price (product_id, price, valid_from) VALUES (1, 100, '2024-01-01');
-- retroactive correction: the price was really 110 from Feb 1st
SELECT bitemporal_update('price', '{"product_id": 1}', '2024-02-01', NULL, '{"price": 110}');
-- what did we believe on 2024-01-20 about 2024-02-10?
SELECT * FROM bitemporal_as_of(NULL::price, '2024-02-10', '2024-01-20');
SELECT * FROM bitemporal_history('price', '{"product_id": 1}');
```

**Use case:** Auditable pricing, insurance, HR, lab and financial records where late corrections must not erase what was known when a decision was made.

### `pg_tsmanage` — Auto-partition management

Automatic partition management for time-series tables via a background worker, including pre-creation and retention.

```sql
CREATE EXTENSION pg_tsmanage;
SELECT pg_tsmanage_manage('metrics', 'ts', '1 day', '30 days');
SELECT pg_time_bucket('1 hour', now());
SELECT * FROM pg_tsmanage_status();
```

**Use case:** Keep a high-volume metrics table partitioned by day and auto-drop data past a retention window.

### `pg_contagg` — Continuous aggregates & hyperfunctions

Continuous aggregates and time-series hyperfunctions: gap-fill, moving averages, rates, deltas, and first/last.

```sql
CREATE EXTENSION pg_contagg;
SELECT analytics_create_cont_agg('hourly', 'readings',
  'SELECT time_bucket, avg(v) FROM readings GROUP BY 1', 'ts', '1 hour');
SELECT analytics_moving_avg(value, 7) OVER (ORDER BY ts) FROM readings;
```

**Use case:** Maintain rolled-up dashboards and smooth noisy time-series with windowed hyperfunctions.

### `pg_tsfunc` — Scientific time-series functions

Native functions for sensor and lab-instrument data: LTTB and min/max downsampling for charts, resampling of irregularly sampled series onto a regular grid (linear, LOCF, nearest or bucket mean, with a `max_gap` cut-off), as-of joins (a merge-based `ts_asof_join` over timestamp arrays plus a `ts_asof` row lookup for `LATERAL`), out-of-order cleanup with `ts_sort_dedup`, and standalone Gorilla-style codecs (delta-of-delta timestamps, XOR-packed floats) that store a whole series in one `bytea` value with bit-exact round trips — about 1 bit per regular timestamp.

```sql
CREATE EXTENSION pg_tsfunc;
SELECT sensor, ts_resample(ts, v, '1 minute', 'linear', NULL, '5 minutes')
  FROM readings GROUP BY sensor;
SELECT * FROM unnest((SELECT ts_lttb(ts, v, 500) FROM readings WHERE sensor = 7));
-- as-of join: latest quote at or before each trade
SELECT t.*, q.bid FROM trades t
LEFT JOIN LATERAL (SELECT bid FROM quotes q WHERE q.sym = t.sym AND q.ts <= t.ts
                   ORDER BY q.ts DESC LIMIT 1) q ON true;
INSERT INTO daily_series SELECT sensor, day,
  ts_encode_timestamps(array_agg(ts ORDER BY ts)), ts_encode_floats(array_agg(v ORDER BY ts))
FROM raw GROUP BY sensor, day;
```

**Use case:** Align irregular instrument readings to a common grid, plot a million-point trace with 500 faithful points, and keep one compact row per sensor per day.

### `pg_approx` — Approximate query processing

Approximate query processing with HyperLogLog distinct counts and Count-Min Sketch frequency estimation.

```sql
CREATE EXTENSION pg_approx;
SELECT approx_count_distinct(user_id) FROM events;
SELECT cms_estimate(cms_add(cms_create(2048, 5), 'apple'), 'apple');
```

**Use case:** Get fast, memory-bounded distinct counts and frequency estimates over massive datasets.

## Scaling, Distribution & Pooling

### `pg_distributed` — Horizontal sharding

Horizontal sharding across worker nodes, with distributed tables, reference tables, and shard rebalancing.

```sql
CREATE EXTENSION pg_distributed;
SELECT add_node('worker1', '10.0.0.2', 5432);
SELECT distribute_table('orders', 'customer_id', 32);
SELECT create_reference_table('countries');
SELECT * FROM rebalance_shards();
```

**Use case:** Scale a large table across multiple nodes by sharding on a distribution key.

### `pg_pool` — Built-in connection pooling

Built-in connection pooling with status, settings, and reset controls.

```sql
CREATE EXTENSION pg_pool;
SELECT * FROM pool_status();
SELECT pool_reset();
```

**Use case:** Handle many short-lived client connections efficiently without an external pooler like PgBouncer.

### `pg_scale` — Scale-to-zero management

Scale-to-zero management that suspends an idle instance and tracks connection activity.

```sql
CREATE EXTENSION pg_scale;
SELECT scale_configure(suspend_after => '5 min', min_connections => 0);
SELECT * FROM scale_activity();
SELECT scale_suspend();
```

**Use case:** Suspend dev or bursty databases when idle to cut compute costs.

### `pg_ratelimit` — Rate limiting & session store

Rate limiting (token bucket and sliding window) plus a TTL-based session key-value store.

```sql
CREATE EXTENSION pg_ratelimit;
SELECT ratelimit_check('ip:1.2.3.4', 100, 1, '1 second');
SELECT session_set('sess-9', 'cart', '{"items":3}', '24 hours');
SELECT session_get('sess-9', 'cart');
```

**Use case:** Throttle abusive API clients and store ephemeral session state at the database tier.

## Ops, Security & Audit

### `pg_audit` — Encrypted audit logging

DML and DDL audit logging with AES-256-GCM encryption of log records.

```sql
CREATE EXTENSION pg_audit;
SELECT * FROM audit_log_status();
SELECT audit_decrypt_log(:encrypted_line, :key, true);
```

**Use case:** Maintain a tamper-resistant, encrypted record of who changed what for compliance.

### `pg_vault` — Encrypted secrets store

Encrypted secrets store built on pgcrypto, with row-level security, audit trail, and key rotation. Requires `pgcrypto`.

```sql
CREATE EXTENSION pg_vault;
SELECT pg_vault_store('api_key', 'sk-123', 'Stripe key');
SELECT pg_vault_fetch('api_key');
SELECT * FROM pg_vault_list();
```

**Use case:** Safely store API keys and credentials that other in-database extensions need at runtime.

### `pg_query_store` — Query store & index advisor

Query Store with per-statement timing history plus an index advisor and unused-index detector.

```sql
CREATE EXTENSION pg_query_store;
SELECT * FROM query_store_entries() ORDER BY total_time DESC LIMIT 10;
SELECT * FROM index_advisor_recommend();
SELECT * FROM index_advisor_unused_indexes();
```

**Use case:** Find your slowest queries and get concrete index recommendations to fix them.

### `pg_remote_config` — Centralized configuration

Centralized configuration management, backing a fleet-wide management schema and control plane.

```sql
CREATE EXTENSION pg_remote_config;
SELECT * FROM remote_config_status();
```

**Use case:** Manage and push configuration to many AlohaDB servers from a central control plane.

## Developer, Schema & Automation

### `pg_branch` — Database branching

Lightweight database branching for testing migrations and experiments, optionally from a WAL LSN. Each branch is a separate server (created with `pg_basebackup`, started with `pg_ctl`) on its own port under `<data>/../branches/<name>`. Branches do not load port-binding or side-effecting workers (`pg_pool`, `pg_restapi`, `pg_cronjobs`, `pg_kafka`, `pg_scale`) and keep their audit logs separate.

```sql
CREATE EXTENSION pg_branch;
SELECT * FROM pg_create_branch('feature-x');
SELECT * FROM pg_list_branches();
SELECT pg_drop_branch('feature-x');
```

**Use case:** Spin up an isolated copy of the database to test a risky migration, then discard it.

### `pg_lineage` — Dataset snapshots, model lineage & zero-copy branches

Named, zero-copy snapshots of system-versioned (`pg_temporal`) or bitemporal tables: each records a timestamp plus per-table row counts and an order-independent content hash, and `lineage_verify` proves it can still be reproduced from history. Model versions link to the snapshot they were trained on, so "what did the dataset look like when model v3 was trained?" is one query. Git-like table branches overlay a delta table and tombstones on the base as of the branch point; the branch view is writable and can be diffed and merged back with conflict detection.

```sql
CREATE EXTENSION pg_lineage;
SELECT * FROM lineage_snapshot('train_v3', ARRAY['customers'::regclass], 'churn v3 training set');
SELECT lineage_link_model('churn', 'v3', 'train_v3', '{"auc": 0.91}');
SELECT * FROM lineage_read('customers', 'churn', 'v3');      -- rows as trained on
SELECT * FROM lineage_verify('train_v3');                    -- still reproducible?
SELECT lineage_branch_create('exp1', 'customers');           -- writable view customers__exp1
UPDATE customers__exp1 SET segment = 'vip' WHERE id = 3;
SELECT * FROM lineage_branch_diff('exp1', 'customers');
SELECT * FROM lineage_branch_merge('exp1', 'customers');
```

**Use case:** Reproducible ML training and safe what-if experiments on production tables without copying data.

### `pg_schema_change` — Online DDL

Online schema changes (DDL) performed via a copy-and-swap process to avoid long table locks.

```sql
CREATE EXTENSION pg_schema_change;
SELECT online_alter_table('ALTER TABLE orders ADD COLUMN note text');
SELECT * FROM online_alter_status();
```

**Use case:** Alter large production tables without blocking writes during the change.

### `pg_json_schema` — JSON Schema validation

JSON Schema Draft-07 validation, usable in CHECK constraints and via a named-schema registry.

```sql
CREATE EXTENSION pg_json_schema;
SELECT jsonschema_register('user', '{"type":"object","required":["name"]}');
SELECT jsonschema_validate_named('{"name":"Kai"}', 'user');
SELECT * FROM jsonschema_validate('{}', '{"required":["name"]}');
```

**Use case:** Enforce the shape of `jsonb` columns at write time with reusable schemas.

### `pg_cronjobs` — Cron job scheduling

Job scheduling with standard cron syntax, including named jobs and run history.

```sql
CREATE EXTENSION pg_cronjobs;
SELECT cron_schedule_named('nightly_vacuum', '0 3 * * *', 'VACUUM ANALYZE');
SELECT * FROM cron_job_status();
```

**Use case:** Run recurring maintenance or data jobs on a schedule without an external scheduler.

### `pg_pipeline` — DAG-based ETL orchestration

DAG-based ETL pipeline orchestration with task dependency tracking and run history.

```sql
CREATE EXTENSION pg_pipeline;
SELECT pipeline_create('nightly_etl');
SELECT pipeline_task_add('nightly_etl', 'load', 'sql', 'INSERT INTO staging ...');
SELECT pipeline_task_depend('nightly_etl', 'transform', 'load');
SELECT pipeline_run('nightly_etl');
```

**Use case:** Orchestrate multi-step ETL with dependencies entirely inside the database.

### `pg_python` — Managed Python execution

Python as a first-class workload with a script registry, environments, and managed execution. Code may `return` a value or assign `_result`; arguments are available as `args` (dict), `_args` (JSON text) and as plain variables named after each key. Requires PL/Python: the prebuilt packages include it together with their own Python runtime in `<install>/python` (add libraries with `<install>/python/bin/python3 -m pip install ...` on Linux/macOS); source builds need `-Dplpython=enabled`.

```sql
CREATE EXTENSION pg_python CASCADE;   -- also installs plpython3u
SELECT python_eval('return {"sum": args["a"] + args["b"]}', '{"a":2,"b":3}');
SELECT python_register('normalize', 'return value.strip().lower()');
SELECT python_exec('normalize', '{"value":"  Hello "}');
```

**Use case:** Register and version reusable Python transforms and call them from SQL.

### `pg_codefs` — Versioned virtual filesystem

A virtual filesystem with git-like versioning, branches, locks, and policies for AI-native development.

```sql
CREATE EXTENSION pg_codefs;
SELECT codefs_write('src/app.py', 'print("hi")', 'main', 'initial commit');
SELECT codefs_read('src/app.py');           -- paths are relative
SELECT * FROM codefs_history('src/app.py');
SELECT * FROM codefs_branch_merge('feature', 'main');
```

**Use case:** Give AI agents a versioned, branchable file workspace stored transactionally in the database.

### `pg_geo` — 2D geometry & spatial indexing

2D geometry types, spatial predicates, and GiST bounding-box indexing with familiar `ST_` functions.

```sql
CREATE EXTENSION pg_geo;
SELECT ST_DistanceSphere(ST_Point(-157.8, 21.3), ST_Point(-156.3, 20.8));
SELECT ST_Contains(poly, ST_Point(-157.8, 21.3)) FROM regions;
```

**Use case:** Store points and polygons and run distance and containment queries with bounding-box indexes.

### `pg_measure` — Units, uncertainty & provenance

A `measurement` column type that stores a value, its standard uncertainty and its SI unit together: error bars propagate through arithmetic and aggregates using the GUM first-order rules, incompatible units are rejected, and conversion covers SI prefixes plus common units (°C, mi, kWh, eV…). A W3C PROV-style provenance log records who wrote each row (graph edges included) and traces what each result was derived from; row-level security limits each role to provenance of tables it can access.

```sql
CREATE EXTENSION pg_measure;
CREATE TABLE runs (id int PRIMARY KEY, g measurement);
SELECT prov_track('runs', 'id', 'pendulum');
INSERT INTO runs VALUES (1, '9.81 ± 0.02 m/s^2'), (2, '9.79(3) m/s^2');
SELECT m_wmean(g), m_convert(m_wmean(g), 'ft/s^2') FROM runs;
SELECT '5 ± 0.1 kg'::measurement * '9.81 ± 0.02 m/s^2';
SELECT * FROM prov_lineage('runs', '1');
```

**Use case:** Keep scientific and engineering results qualified: units checked, error bars propagated, and the origin of every value traceable.

## Building for macOS, Linux & Windows

AlohaDB builds from source with Meson + Ninja (the toolchain PostgreSQL 18 uses), producing the server plus the bundled `pg_*` extensions. Build natively on each OS. A few extensions depend on external native libraries (ONNX Runtime, Apache Arrow, librdkafka, OpenCL/CUDA): the core server builds everywhere, while full extension parity on Windows may require a reduced set.

### Linux

```sql
# build deps: meson, ninja, bison, flex, readline, zlib, openssl, icu
git clone https://github.com/Vern-AllWorks-LLC/alohadb.git
cd alohadb/alohadb-20.0
meson setup builddir --prefix=/opt/alohadb -Drpath=true -Dplpython=enabled   # plpython needs python3-devel
ninja -C builddir -j$(nproc)
sudo ninja -C builddir install
# or run the guided installer:  sudo bash install.sh
```

**Tip:** The bundled `install.sh` checks prerequisites, installs dependencies, builds, and initializes a data directory automatically.

### macOS

```sql
brew install meson ninja icu4c openssl readline
git clone https://github.com/Vern-AllWorks-LLC/alohadb.git
cd alohadb/alohadb-20.0
meson setup builddir --prefix=/opt/alohadb -Drpath=true
ninja -C builddir
ninja -C builddir install
```

**Tip:** Export `PKG_CONFIG_PATH` for keg-only Homebrew formulae (icu4c, openssl) so Meson can locate them.

### Windows

```sql
:: Visual Studio 2022 + Meson + Ninja (PG18 supports MSVC via Meson)
git clone https://github.com/Vern-AllWorks-LLC/alohadb.git
cd alohadb\alohadb-20.0
meson setup builddir --prefix=C:\alohadb
ninja -C builddir
ninja -C builddir install
```

**Tip:** Run from an "x64 Native Tools Command Prompt for VS 2022". Extensions needing ONNX/Arrow/CUDA may be excluded from Windows builds.

### Automated builds (GitHub Actions)

A matrix of `ubuntu-latest`, `macos-14`, `macos-13` and `windows-latest` runners can run the Meson/Ninja build on each and publish the packaged server (tarball, `.pkg`, or `.msi`) as a release artifact.

**Use case:** Produce installable AlohaDB packages for all three platforms from one push, with optional notarization/signing steps.

## Names in AlohaDB 21 and upgrading from 20.x

AlohaDB 21 dropped its own program, file and extension names in favour of PostgreSQL's, so that PostgreSQL tooling (drivers, `pg_config`/PGXS builds, Patroni, packaging scripts, monitoring agents, tutorials) works without changes.

**Programs:** `alohadb` → `postgres`, `al_amcheck` → `pg_amcheck`, `al_archivecleanup` → `pg_archivecleanup`, `al_basebackup` → `pg_basebackup`, `al_checksums` → `pg_checksums`, `al_combinebackup` → `pg_combinebackup`, `al_config` → `pg_config`, `al_controldata` → `pg_controldata`, `al_createsubscriber` → `pg_createsubscriber`, `al_ctl` → `pg_ctl`, `al_dump` → `pg_dump`, `al_dumpall` → `pg_dumpall`, `al_isready` → `pg_isready`, `al_receivewal` → `pg_receivewal`, `al_recvlogical` → `pg_recvlogical`, `al_resetwal` → `pg_resetwal`, `al_restore` → `pg_restore`, `al_rewind` → `pg_rewind`, `al_test_fsync` → `pg_test_fsync`, `al_test_timing` → `pg_test_timing`, `al_upgrade` → `pg_upgrade`, `al_verifybackup` → `pg_verifybackup`, `al_waldump` → `pg_waldump`, `al_walsummary` → `pg_walsummary`, `albench` → `pgbench`, `alclusterdb` → `clusterdb`, `alcreatedb` → `createdb`, `alcreateuser` → `createuser`, `aldropdb` → `dropdb`, `aldropuser` → `dropuser`, `alinitdb` → `initdb`, `alreindexdb` → `reindexdb`, `alsql` → `psql`, `alvacuumdb` → `vacuumdb`. The server process title is `postgres: ...` and the default `syslog_ident` is `postgres`.

**Data directory files:** `alohadb.conf` → `postgresql.conf`, `alohadb.auto.conf` → `postgresql.auto.conf`, `al_hba.conf` → `pg_hba.conf`, `al_ident.conf` → `pg_ident.conf`, `alohadb.pid` → `postmaster.pid`, `alohadb.opts` → `postmaster.opts`.

**Extensions:** every AlohaDB extension is now `pg_<name>`. Where `pg_<name>` already belongs to a well-known third-party extension with a different API (pg_cron, ParadeDB's pg_search, Supabase's pg_graphql and pg_jsonschema, pg_analytics, Crunchy's pg_parquet, Tembo's pg_vectorize and pg_timeseries), AlohaDB uses a distinct name so the real one can still be installed next to it.

| AlohaDB 20.x | AlohaDB 21 |
|---|---|
| `alohadb_analytics` | `pg_contagg` |
| `alohadb_approx` | `pg_approx` |
| `alohadb_audit` | `pg_audit` |
| `alohadb_bitemporal` | `pg_bitemporal` |
| `alohadb_branch` | `pg_branch` |
| `alohadb_cache` | `pg_cache` |
| `alohadb_cdc_arrow` | `pg_cdc_arrow` |
| `alohadb_chart` | `pg_chart` |
| `alohadb_codefs` | `pg_codefs` |
| `alohadb_columnar` | `pg_columnar` |
| `alohadb_context` | `pg_context` |
| `alohadb_cron` | `pg_cronjobs` |
| `alohadb_distributed` | `pg_distributed` |
| `alohadb_embeddings` | `pg_embeddings` |
| `alohadb_federated` | `pg_federated` |
| `alohadb_geo` | `pg_geo` |
| `alohadb_gpu` | `pg_gpu` |
| `alohadb_graph` | `pg_graph` |
| `alohadb_graphql` | `pg_gql` |
| `alohadb_http` | `pg_http` |
| `alohadb_inference` | `pg_inference` |
| `alohadb_intent` | `pg_intent` |
| `alohadb_jsonschema` | `pg_json_schema` |
| `alohadb_kafka` | `pg_kafka` |
| `alohadb_learned_stats` | `pg_learned_stats` |
| `alohadb_lineage` | `pg_lineage` |
| `alohadb_measure` | `pg_measure` |
| `alohadb_model_registry` | `pg_model_registry` |
| `alohadb_ndarray` | `pg_ndarray` |
| `alohadb_nl2sql` | `pg_nl2sql` |
| `alohadb_nosql` | `pg_nosql` |
| `alohadb_objectstore` | `pg_objectstore` |
| `alohadb_ontology` | `pg_ontology` |
| `alohadb_parquet` | `pg_parquetio` |
| `alohadb_pg19compat` | `pg_compat19` |
| `alohadb_pipeline` | `pg_pipeline` |
| `alohadb_pool` | `pg_pool` |
| `alohadb_python` | `pg_python` |
| `alohadb_query_store` | `pg_query_store` |
| `alohadb_queue` | `pg_queue` |
| `alohadb_ratelimit` | `pg_ratelimit` |
| `alohadb_realtime` | `pg_realtime` |
| `alohadb_remote_config` | `pg_remote_config` |
| `alohadb_restapi` | `pg_restapi` |
| `alohadb_scale` | `pg_scale` |
| `alohadb_schema_change` | `pg_schema_change` |
| `alohadb_search` | `pg_ftsearch` |
| `alohadb_temporal` | `pg_temporal` |
| `alohadb_tiering` | `pg_tiering` |
| `alohadb_timeseries` | `pg_tsmanage` |
| `alohadb_tsfunc` | `pg_tsfunc` |
| `alohadb_unified` | `pg_unified` |
| `alohadb_vault` | `pg_vault` |
| `alohadb_vector` | `pg_vector` |
| `alohadb_vectorize` | `pg_vecexec` |

**Inside the extensions:** functions, tables and types named `alohadb_<name>...` take the new extension name as prefix (`alohadb_vault_store()` → `pg_vault_store()`, `alohadb_cron_jobs` → `pg_cronjobs_jobs`, `alohadb_time_bucket()` → `pg_time_bucket()`), except the role `alohadb_vault_admin`, which is now `vault_admin` (role names starting with `pg_` are reserved). Settings moved under the extension's own prefix, as is the PostgreSQL convention: `alohadb.audit_enabled` → `pg_audit.enabled`, `alohadb.cron_database` → `pg_cronjobs.database`, `alohadb.cache_max_entries` → `pg_cache.max_entries`, `alohadb.remote_host` → `pg_remote_config.host`, and so on. Core functions `alohadb_apply_profile()` / `alohadb_detect_hardware()` are now `pg_apply_profile()` / `pg_detect_hardware()`. Functions without an `alohadb_` prefix (`cache_get`, `cron_schedule`, `read_parquet`, `search_bm25`, ...) are unchanged.

**Converting your SQL and scripts:** `pg_alohadb_convert_sql` rewrites SQL scripts, dumps, configuration files and shell scripts from the old names to the new ones. It changes only names AlohaDB defined, prints a report of every rename, and keeps `FILE.bak` copies.

```sql
pg_alohadb_convert_sql --check migrations/*.sql deploy.sh     # report only
pg_alohadb_convert_sql migrations/*.sql deploy.sh             # convert in place
pg_alohadb_convert_sql < old_dump.sql > new_dump.sql
```

**Before upgrading a 20.2 / 20.3 cluster that uses `alohadb_embeddings`:** its `embedding_model.embed_fn` column was a `regprocedure`, which `pg_upgrade` refuses to carry over. Change it to text on the old server first (`pg_embeddings` 1.1 stores it as text and re-qualifies the values):

```sql
ALTER TABLE embedding_model ALTER COLUMN embed_fn TYPE text USING embed_fn::text;
```

**Converting a database cluster:** upgrade with `pg_upgrade` as usual (it still finds the old `al_*` programs and `alohadb.conf` / `alohadb.pid` of the 20.x cluster, and the server still loads libraries and functions stored under their old names), then run `pg_alohadb_convert_db`. It renames the installed `alohadb_*` extensions in every database in place — tables, functions, types, indexes, constraints, roles, comments, `ALTER DATABASE/ROLE ... SET` settings and scheduled job commands — without copying or losing data, and updates them to their current versions (for example `pg_cache` 1.0 → 2.0). With `-D` it also renames and converts the configuration files.

```sql
pg_upgrade -b /old/bin -B /new/bin -d /old/data -D /new/data --copy
pg_alohadb_convert_db -D /new/data --config-only     # server stopped: conf files, shared_preload_libraries
pg_ctl -D /new/data start
pg_alohadb_convert_db -U postgres                     # every database (or -d DBNAME)
```

**Tip:** Application code that calls renamed functions (for example `alohadb_vault_fetch()`) or sets renamed settings needs the same conversion; run `pg_alohadb_convert_sql` over it. A dump taken from 20.x can be converted with `pg_alohadb_convert_sql` and restored into 21.

## Release notes

### 21.0.0 — November 2026

- **Stock PostgreSQL names.** Programs, data-directory files and extensions now use PostgreSQL's naming (`postgres`, `psql`, `pg_ctl`, `postgresql.conf`, `pg_*` extensions); see [Names in AlohaDB 21 and upgrading from 20.x](#names-in-alohadb-21-and-upgrading-from-20x) for the full mapping. New tools `pg_alohadb_convert_sql` and `pg_alohadb_convert_db` convert scripts and existing clusters; `pg_upgrade` upgrades 20.x clusters directly, and libraries, C functions and `shared_preload_libraries` entries stored under the old names keep loading.
- **`pg_cache` 2.0 — transactional KV cache.** Cache writes are applied when the transaction commits and discarded on rollback (with read-your-writes and savepoint support); variable-size `jsonb` or `text` values under a hard memory cap (`pg_cache.max_memory`); SIEVE eviction with shared-lock reads; new `cache_set_text`, `cache_get_text`, `cache_add`, `cache_incr`, `cache_expire`, `cache_ttl`, `cache_keys` and `cache_purge_expired`; `cache_stats()` reports memory and expirations. `pg_cache.max_entries` now defaults to 10000. The functions are no longer executable by `PUBLIC`.
- **New `pg_kv`:** a key-value API with TTLs on an unlogged table.
- `pg_embeddings` 1.1: `embedding_model.embed_fn` is stored as a schema-qualified text signature instead of a `regprocedure`, so clusters using it can be upgraded with `pg_upgrade` (see the note above for upgrading from 20.x).
- **Upgrade note:** the catalog version changed; 20.x clusters need `pg_upgrade` followed by `pg_alohadb_convert_db`.


### 20.3.0 — November 2026

- Casts from `integer[]`, `real[]`, `double precision[]` and `numeric[]` to `vector`, as in pgvector (assignment casts via `array_to_vector(array, typmod, explicit)`): `INSERT INTO items (embedding) VALUES (ARRAY[1,2,3])` and `SELECT ARRAY[1,2,3]::vector(3)` now work, with the same 1-D / no-NULL / dimension / finite-value checks as vector input.
- pgvector's `hnsw_vector` and `ivfflat_vector` regression suites now run unmodified as the core `vector_index` test.
- **Upgrade note:** the catalog version changed; existing 20.2 clusters need `al_upgrade` (or dump/restore). On-disk index formats are unchanged.

### 20.2.0 — November 2026

- New extensions: `alohadb_graph` (enforced property-graph schemas, traversal, temporal paths, PageRank, point-in-time GNN sampling), `alohadb_bitemporal`, `alohadb_lineage` (dataset snapshots, model→data lineage, zero-copy table branches), `alohadb_embeddings` (embedding lifecycle, hybrid vector + full-text search), `alohadb_ndarray` (chunked compressed N-d arrays), `alohadb_measure` (units, uncertainty, provenance), `alohadb_tsfunc` (LTTB, resampling, as-of joins, Gorilla codecs).
- `alohadb_parquet` 1.2: `write_parquet`, `write_arrow`, `arrow_ipc_stream`; `read_parquet` rewritten (it returned no rows for files written by PyArrow).
- `alohadb_temporal` 1.1: history tables no longer copy the base table's primary key, which made the second update of any row fail. `ALTER EXTENSION alohadb_temporal UPDATE` repairs existing history tables.
- Core `hnsw` / `ivfflat` index access methods replaced with a port of pgvector 0.8.7 (PostgreSQL License) for the core `vector` type. The previous implementation never served ORDER BY queries (and returned wrong or no results when forced); the new one gives pgvector's behaviour — e.g. recall@10 ≈ 0.97 in ~1 ms on 50k 32-dimension vectors — plus parallel builds, iterative (filtered) scans and an `hnsw` `vector_l1_ops` opclass. New functions `vector_l2_squared_distance`, `vector_negative_inner_product`, `vector_spherical_distance`; new settings `hnsw.*` / `ivfflat.iterative_scan` / `ivfflat.max_probes`.
- **Upgrade note:** the catalog version changed, so existing clusters need `al_upgrade` (or dump/restore), and any existing `hnsw` / `ivfflat` indexes must be rebuilt with `REINDEX` (the on-disk format changed; old indexes report "not valid").
- Patent-aware design: time-series compression is offered only as value-level codecs, and there is no chunk tiering to object storage or invalidation-based continuous aggregation; bitemporal storage uses one current + one history table; branches are logical overlays. Prior art is cited in each extension's source.

### 20.1.1 — November 2026

- PL/Python and `alohadb_python` work out of the box in the prebuilt packages, which bundle a Python runtime (CPython 3.12 on Linux/macOS, the MinGW CPython on Windows) in `<install>/python`. PL/Python uses it automatically unless `PYTHONHOME` is set.

### 20.1.0 — November 2026

- Based on PostgreSQL **18.7** (from 18.3). Same on-disk format: existing clusters run on the new binaries without `al_upgrade`. `output_plugin_libraries` (new in 18.7) defaults to `pgoutput, test_decoding, alohadb_cdc_arrow`.
- Tooling fixed for AlohaDB's file names: base backups no longer copy `alohadb.pid`; `al_basebackup -R` writes standby settings to `alohadb.auto.conf`; `al_resetwal` detects a running server; `al_dumpall`, `al_verifybackup`, `al_ctl initdb`, `al_createsubscriber` and `al_upgrade` find their helper programs without workarounds.
- Online DDL: `ALTER COLUMN ... TYPE` to a toastable type (e.g. `int` -> `text`) works again.
- `alohadb_ontology`, `alohadb_context`, `alohadb_intent` 1.1 (multi-sense disambiguation, bidirectional cycle-safe graph traversal, change history); new `alohadb_pg19compat`; `alohadb_unified` 1.1.
- Fixed: `alohadb_vector` installs and searches correctly; `alohadb_nosql` string/array filters, sort and projection; `alohadb_python` execution; `alohadb_branch` start/stop safety; an `alohadb_objectstore` crash on credentials without keys.
- Extension data (ontology, context, intent, vector, python registries) is included in `al_dump` / `al_dumpall`.
- `alohadb_codefs` is now built and installed (it was documented but missing from the build).
- Prebuilt, self-contained packages for Linux (x86_64, aarch64), macOS (Apple Silicon, Intel) and Windows (x86_64): unpack anywhere and run `bin/alinitdb`. Download from the GitHub Release or <https://vernallworks.com/downloads.php>. The Windows package omits the POSIX-only `alohadb_http`, `alohadb_remote_config`, `alohadb_restapi` and `alohadb_branch`.

---

## Documentation

Full documentation: <https://vernallworks.com/docs-alohadb.php>

## License

AlohaDB extensions are MIT-licensed; the PostgreSQL base is under the PostgreSQL License. See `COPYRIGHT` and `LICENSE`.
