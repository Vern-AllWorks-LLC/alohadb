# AlohaDB

AlohaDB is an AI-native database built on PostgreSQL 18 (currently based on 18.7). It extends standard PostgreSQL with a suite of purpose-built extensions for modern application development — including real-time change notifications, in-database ML inference, vector search, time-travel queries, columnar storage, GraphQL and REST APIs, natural-language-to-SQL, and encrypted audit logging. Every feature ships as a standard `CREATE EXTENSION` module, so it composes naturally with the PostgreSQL you already know. All AlohaDB extensions are MIT-licensed.

## AI, Vectors & Inference

### `alohadb_vector` — Vector type & ANN search

IVFFlat approximate nearest-neighbor indexes (k-means trained) and named distance functions on top of the built-in `vector` type, which ships in the AlohaDB core together with the `<->` (L2), `<=>` (cosine), `<#>` (negative inner product) and `<+>` (L1) operators.

```sql
CREATE EXTENSION alohadb_vector;
SELECT vector_index_create('emb_idx', 'docs', 'embedding', 'id', 100, 10, 'cosine');
SELECT vector_index_rebuild('emb_idx');            -- train the index
SELECT vector_index_insert('emb_idx', 'doc-1', '[0.1,0.2,0.3]'::vector);
SELECT * FROM vector_index_search('emb_idx', '[0.1,0.2,0.3]'::vector, 5);
-- or use distance operators directly:
SELECT a <=> b AS cosine_dist FROM embeddings;
```

**Use case:** Power semantic search and RAG retrieval by finding the nearest embeddings to a query vector.

### `alohadb_gpu` — GPU-offloaded vector ops

GPU-offloaded vector operations: batch distance, top-K, and matrix multiply for high-throughput embedding workloads.

```sql
CREATE EXTENSION alohadb_gpu;
SELECT alohadb_gpu_available();
SELECT * FROM alohadb_gpu_topk(
  '[0.1,0.2,0.3]'::vector,
  ARRAY['[0.1,0.0,0.2]'::vector, '[0.4,0.5,0.6]'::vector],
  1);
```

**Use case:** Accelerate large batch similarity searches by offloading distance math to the GPU.

### `alohadb_inference` — In-database ONNX inference

In-database ML inference using ONNX Runtime — load a model once and score rows without leaving SQL.

```sql
CREATE EXTENSION alohadb_inference;
SELECT alohadb_load_model('classifier', :model_bytes);
SELECT alohadb_infer('classifier', '[0.2,0.7,0.1]'::vector);
SELECT * FROM alohadb_list_models();
```

**Use case:** Run real-time fraud or churn scoring directly inside a query, co-located with the data.

### `alohadb_model_registry` — Model versioning & deployment

ML model versioning, deployment, and experiment tracking with rollback and metric comparison.

```sql
CREATE EXTENSION alohadb_model_registry;
SELECT model_register('churn', 'onnx', 'Churn predictor');
SELECT model_version_add('churn', :model_bytes, '{"auc":0.91}');
SELECT model_deploy('churn');
SELECT model_rollback('churn');
```

**Use case:** Track experiments and promote, compare, or roll back model versions in production.

### `alohadb_nl2sql` — Natural language to SQL

Natural language to SQL translation via an LLM API, with optional read-only execution and plain-English explanations.

```sql
CREATE EXTENSION alohadb_nl2sql;
SELECT alohadb_nl2sql('top 5 customers by revenue last month');
SELECT * FROM alohadb_nl2sql_execute('how many orders shipped today')
  AS t(order_count bigint);
```

**Use case:** Let non-technical users query data conversationally without writing SQL.

### `alohadb_federated` — Federated learning

Federated learning with Flower framework integration, training a shared model across nodes without centralizing data.

```sql
CREATE EXTENSION alohadb_federated;
SET alohadb.flower_server = 'localhost:8080';
SET alohadb.fl_training_query = 'SELECT features, label FROM training_data';
SELECT alohadb_fl_start();
SELECT * FROM alohadb_fl_status();
```

**Use case:** Collaboratively train ML models across data silos while keeping raw data in place.

### `alohadb_learned_stats` — ML cardinality estimation

ML-assisted cardinality estimation using gradient-boosted trees to improve the query planner's row estimates.

```sql
CREATE EXTENSION alohadb_learned_stats;
SELECT alohadb_learned_stats_train();
SELECT * FROM alohadb_learned_stats_status();
```

**Use case:** Improve plan quality for complex queries where standard statistics misestimate cardinality.

## Knowledge, Context & Semantics

### `alohadb_ontology` — Triple-store context graph

Triple-store context graph for entity and relationship mapping, with cycle-safe subclass hierarchies, bidirectional graph traversal, and a change history of every assertion and retraction.

```sql
CREATE EXTENSION alohadb_ontology;
SELECT ontology_assert('Honolulu', 'capital_of', 'Hawaii');
SELECT ontology_subclass('City', 'Place');
SELECT * FROM ontology_query('Honolulu', NULL, NULL);
SELECT * FROM ontology_related('Hawaii', 2);        -- entity, via_predicate, hops, path
SELECT ontology_is_a('City', 'Place');
SELECT * FROM ontology_history('Honolulu');
```

**Use case:** Model domain knowledge as subject-predicate-object facts and traverse relationships for reasoning.

### `alohadb_context` — Entity disambiguation & reranking

Entity disambiguation and context-aware search reranking. A term can have several registered senses; the surrounding text picks one by counting each sense's context cues (domain, type, keywords, and its `alohadb_ontology` neighbours). Requires `alohadb_ontology`.

```sql
CREATE EXTENSION alohadb_context CASCADE;   -- also installs alohadb_ontology
--                     term     entity          type        hint  prio domain    keywords
SELECT context_register('Apple', 'customer:42',   'customer', NULL, 100, 'sales',   ARRAY['invoice','contract']);
SELECT context_register('Apple', 'produce:apple', 'product',  NULL, 100, 'grocery', ARRAY['fruit','orchard']);
SELECT * FROM context_resolve('Apple', 'renew the Apple contract');   -- customer:42
SELECT * FROM context_rerank(:candidates, 'Apple invoice status');    -- boosts/penalises hits
```

**Use case:** Disambiguate homonyms and rerank search results based on contextual intent.

### `alohadb_intent` — Intent classification & why-indexing

Document intent classification and why-indexing — register intents, infer them from text, and auto-tag documents.

```sql
CREATE EXTENSION alohadb_intent;
SELECT intent_register('refund_request', 'Customer wants money back',
  ARRAY['refund','return','money back']);
SELECT * FROM intent_infer('I would like a refund please');
```

**Use case:** Classify incoming support tickets or documents by intent for routing and analytics.

### `alohadb_pg19compat` — PostgreSQL 19 function backports

Self-contained PostgreSQL 19 SQL additions for the 18.x base: `error_on_null()`, `tid_block()`/`tid_offset()`, `random(min, max)` for date/timestamp/timestamptz, `range_minus_multi()`/`multirange_minus_multi()`, and `uuid` ⇄ `bytea` casts. Drop it before upgrading a cluster to a PostgreSQL 19-based AlohaDB, where these are built in.

```sql
CREATE EXTENSION alohadb_pg19compat;
SELECT random('2026-01-01'::date, '2026-12-31'::date);
SELECT * FROM range_minus_multi('[1,10)'::int4range, '[3,5)'::int4range);
```

## Real-time, Streaming & Queues

### `alohadb_realtime` — Realtime change notifications

Trigger-based realtime change notifications built on LISTEN/NOTIFY, with subscriptions and event polling.

```sql
CREATE EXTENSION alohadb_realtime;
SELECT alohadb_realtime_subscribe('orders', 'status = ''shipped''');
SELECT * FROM alohadb_realtime_poll(0);
SELECT alohadb_realtime_cleanup('1 hour');
```

**Use case:** Push live updates to clients when rows change, powering dashboards and collaborative apps.

### `alohadb_queue` — Message queue with consumer groups

Durable message queue with consumer groups, visibility timeouts, acknowledgements, and retention.

```sql
CREATE EXTENSION alohadb_queue;
SELECT queue_create('jobs', '30 seconds', '7 days');
SELECT queue_send('jobs', '{"task":"resize"}');
SELECT * FROM queue_receive('jobs', 1);
SELECT queue_ack('jobs', 1);
```

**Use case:** Run a reliable background job queue without deploying a separate message broker.

### `alohadb_kafka` — Kafka & Kinesis consumer

Inbound streaming consumer for Apache Kafka and Amazon Kinesis that lands messages as queryable rows.

```sql
CREATE EXTENSION alohadb_kafka;
SELECT kafka_consumer_create('events', 'broker:9092', 'clicks');
SELECT kafka_consumer_start('events');
SELECT * FROM kafka_messages_poll('events', 1000);
```

**Use case:** Ingest event streams from Kafka or Kinesis directly into PostgreSQL for processing.

### `alohadb_cdc_arrow` — Arrow IPC change data capture

Logical decoding output plugin that emits change data capture in Arrow IPC format for efficient downstream consumption.

```sql
CREATE EXTENSION alohadb_cdc_arrow;
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'alohadb_cdc_arrow');
SELECT * FROM pg_logical_slot_get_binary_changes('cdc_slot', NULL, NULL);
```

**Use case:** Stream row changes into Arrow-native analytics pipelines and data lakes with minimal serialization overhead.

### `alohadb_unified` — Unified streaming + batch model

Unified streaming and batch query model over the same relation — a view UNIONs the durable batch table with an unflushed event buffer.

```sql
CREATE EXTENSION alohadb_unified;
CREATE TABLE clicks_batch (url text);
SELECT unified_register('clicks', 'clicks_batch');
SELECT unified_ingest('clicks', '{"url":"/home"}');
SELECT * FROM clicks_unified;
SELECT unified_flush('clicks');
```

**Use case:** Query fresh streaming events and historical batch data through a single consistent relation.

## Storage, Formats & Tiering

### `alohadb_columnar` — Columnar table storage

Columnar storage table access method with per-column zstd compression for analytical workloads.

```sql
CREATE EXTENSION alohadb_columnar;
CREATE TABLE events_col (id bigint, ts timestamptz, payload jsonb) USING columnar;
SELECT * FROM alohadb_columnar_info('events_col');
```

**Use case:** Store large analytical tables compressed and scan only the columns a query needs.

### `alohadb_parquet` — Parquet, CSV & JSON reader

Read Parquet, CSV, and JSON files directly as sets of `jsonb` rows.

```sql
CREATE EXTENSION alohadb_parquet;
SELECT * FROM read_parquet('/data/sales.parquet');
SELECT * FROM read_csv('/data/import.csv', ',', true);
```

**Use case:** Query files on disk in place without a separate ETL load step.

### `alohadb_objectstore` — S3, GCS & Azure storage

Cloud object storage reader and writer for S3, GCS, and Azure Blob, with credential management.

```sql
CREATE EXTENSION alohadb_objectstore;
SELECT objectstore_credential_register('s3', 'prod', '{"region":"us-west-2"}');
SELECT objectstore_put('s3://bucket/report.txt', 'hello'::bytea, 'prod');
SELECT * FROM objectstore_list('s3://bucket/', 'prod');
```

**Use case:** Read and write backups, exports, and data files in cloud object storage from SQL.

### `alohadb_tiering` — Automatic storage tiering

Automatic storage tiering for partitioned tables, moving aged partitions to a colder tablespace by time-based rules.

```sql
CREATE EXTENSION alohadb_tiering;
INSERT INTO alohadb_tiering_rules(parent_table, age_threshold, target_tablespace)
  VALUES ('events', '90 days', 'cold_ts');
SELECT alohadb_tiering_check_now();
SELECT * FROM alohadb_tiering_status();
```

**Use case:** Keep hot data on fast storage while automatically demoting old partitions to cheaper disks.

### `alohadb_nosql` — Document database API

Document database API with flexible, optionally schema-validated JSON collections. Filters match exact values and arrays by containment, or use operator objects (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `like`, `contains`, `exists`); search options support `sort`, `fields`, `limit` and `offset`.

```sql
CREATE EXTENSION alohadb_nosql;
SELECT doc_create_collection('users');
SELECT doc_insert('users', '{"name":"Kai","city":"Hilo","age":31,"tags":["surf"]}');
SELECT * FROM doc_search('users', '{"city":"Hilo"}');
SELECT * FROM doc_search('users', '{"age":{"gte":21},"tags":["surf"]}',
                         '{"sort":{"age":"desc"},"fields":["name","age"],"limit":10}');
```

**Use case:** Build document-oriented features with a MongoDB-style API backed by PostgreSQL durability.

### `alohadb_cache` — Shared-memory key-value cache

Shared memory key-value cache with TTLs and LRU eviction, exposed through SQL.

```sql
CREATE EXTENSION alohadb_cache;
SELECT cache_set('session:42', '{"user":7}', '10 minutes');
SELECT cache_get('session:42');
SELECT * FROM cache_stats();
```

**Use case:** Cache expensive computed values or session data without a separate Redis deployment.

## Query, API & Interfaces

### `alohadb_graphql` — Auto-generated GraphQL API

Auto-generated GraphQL API derived from the database schema, executed entirely in SQL.

```sql
CREATE EXTENSION alohadb_graphql;
SELECT graphql_execute('{ users(limit: 5) { id name } }');
SELECT graphql_schema();
```

**Use case:** Expose your tables as a GraphQL endpoint without writing resolver code.

### `alohadb_restapi` — Auto-generated REST API

Auto-generated REST API served by a background-worker HTTP server, also callable from SQL.

```sql
CREATE EXTENSION alohadb_restapi;
SELECT alohadb_restapi_handle('GET', '/api/users');
SELECT * FROM alohadb_restapi_endpoints();
```

**Use case:** Stand up a CRUD REST API over your schema with no application server.

### `alohadb_http` — HTTP client

An HTTP client for PostgreSQL, supporting GET/POST/PUT/DELETE and arbitrary methods with headers and timeouts.

```sql
CREATE EXTENSION alohadb_http;
SELECT status, body FROM http_get('https://api.example.com/health');
SELECT status FROM http_post('https://api.example.com/hook', '{"x":1}');
```

**Use case:** Call external webhooks and APIs from triggers or scheduled jobs inside the database.

### `alohadb_search` — BM25, fuzzy, phonetic & geo search

Full-text relevance with BM25 scoring, fuzzy and phonetic matching, autocomplete, and geo proximity search.

```sql
CREATE EXTENSION alohadb_search;
SELECT search_bm25(to_tsvector(body), to_tsquery('database')) FROM articles;
SELECT search_fuzzy_match('alohadb', 'alohdb', 2);
SELECT * FROM search_nearby(21.3, -157.8, 5000, 'stores', 'lat', 'lon');
```

**Use case:** Build relevance-ranked, typo-tolerant, location-aware search over your tables.

### `alohadb_chart` — SVG charts from SQL

Generate SVG charts (bar, line, pie, scatter, area) directly from a SQL query.

```sql
CREATE EXTENSION alohadb_chart;
SELECT chart_bar('SELECT region, sum(amount) FROM sales GROUP BY region',
  'Sales by Region');
```

**Use case:** Embed ready-to-render charts in reports and emails without a charting library.

### `alohadb_vectorize` — Vectorized query acceleration

Analytical query acceleration using vectorized (batched) execution, with explain and benchmark helpers.

```sql
CREATE EXTENSION alohadb_vectorize;
SELECT * FROM vectorize_query('SELECT region, sum(amt) FROM sales GROUP BY region')
  AS t(region text, total numeric);
SELECT * FROM vectorize_benchmark('SELECT count(*) FROM sales', 10);
```

**Use case:** Speed up large aggregation queries by processing rows in batches.

## Time, Temporal & Series

### `alohadb_temporal` — System-versioned time-travel

SQL:2011-style system-versioned temporal tables with time-travel queries over historical row versions.

```sql
CREATE EXTENSION alohadb_temporal;
SELECT alohadb_enable_system_versioning('orders');
SELECT * FROM alohadb_as_of('orders', '2026-01-01') AS t(id int, status text);
SELECT * FROM alohadb_versions_between('orders', '2026-01-01', '2026-02-01')
  AS t(id int, status text);
```

**Use case:** Audit how a record looked at any past point in time without manual history tables.

### `alohadb_timeseries` — Auto-partition management

Automatic partition management for time-series tables via a background worker, including pre-creation and retention.

```sql
CREATE EXTENSION alohadb_timeseries;
SELECT alohadb_timeseries_manage('metrics', 'ts', '1 day', '30 days');
SELECT alohadb_time_bucket('1 hour', now());
SELECT * FROM alohadb_timeseries_status();
```

**Use case:** Keep a high-volume metrics table partitioned by day and auto-drop data past a retention window.

### `alohadb_analytics` — Continuous aggregates & hyperfunctions

Continuous aggregates and time-series hyperfunctions: gap-fill, moving averages, rates, deltas, and first/last.

```sql
CREATE EXTENSION alohadb_analytics;
SELECT analytics_create_cont_agg('hourly', 'readings',
  'SELECT time_bucket, avg(v) FROM readings GROUP BY 1', 'ts', '1 hour');
SELECT analytics_moving_avg(value, 7) OVER (ORDER BY ts) FROM readings;
```

**Use case:** Maintain rolled-up dashboards and smooth noisy time-series with windowed hyperfunctions.

### `alohadb_approx` — Approximate query processing

Approximate query processing with HyperLogLog distinct counts and Count-Min Sketch frequency estimation.

```sql
CREATE EXTENSION alohadb_approx;
SELECT approx_count_distinct(user_id) FROM events;
SELECT cms_estimate(cms_add(cms_create(2048, 5), 'apple'), 'apple');
```

**Use case:** Get fast, memory-bounded distinct counts and frequency estimates over massive datasets.

## Scaling, Distribution & Pooling

### `alohadb_distributed` — Horizontal sharding

Horizontal sharding across worker nodes, with distributed tables, reference tables, and shard rebalancing.

```sql
CREATE EXTENSION alohadb_distributed;
SELECT add_node('worker1', '10.0.0.2', 5432);
SELECT distribute_table('orders', 'customer_id', 32);
SELECT create_reference_table('countries');
SELECT * FROM rebalance_shards();
```

**Use case:** Scale a large table across multiple nodes by sharding on a distribution key.

### `alohadb_pool` — Built-in connection pooling

Built-in connection pooling with status, settings, and reset controls.

```sql
CREATE EXTENSION alohadb_pool;
SELECT * FROM pool_status();
SELECT pool_reset();
```

**Use case:** Handle many short-lived client connections efficiently without an external pooler like PgBouncer.

### `alohadb_scale` — Scale-to-zero management

Scale-to-zero management that suspends an idle instance and tracks connection activity.

```sql
CREATE EXTENSION alohadb_scale;
SELECT scale_configure(suspend_after => '5 min', min_connections => 0);
SELECT * FROM scale_activity();
SELECT scale_suspend();
```

**Use case:** Suspend dev or bursty databases when idle to cut compute costs.

### `alohadb_ratelimit` — Rate limiting & session store

Rate limiting (token bucket and sliding window) plus a TTL-based session key-value store.

```sql
CREATE EXTENSION alohadb_ratelimit;
SELECT ratelimit_check('ip:1.2.3.4', 100, 1, '1 second');
SELECT session_set('sess-9', 'cart', '{"items":3}', '24 hours');
SELECT session_get('sess-9', 'cart');
```

**Use case:** Throttle abusive API clients and store ephemeral session state at the database tier.

## Ops, Security & Audit

### `alohadb_audit` — Encrypted audit logging

DML and DDL audit logging with AES-256-GCM encryption of log records.

```sql
CREATE EXTENSION alohadb_audit;
SELECT * FROM audit_log_status();
SELECT audit_decrypt_log(:encrypted_line, :key, true);
```

**Use case:** Maintain a tamper-resistant, encrypted record of who changed what for compliance.

### `alohadb_vault` — Encrypted secrets store

Encrypted secrets store built on pgcrypto, with row-level security, audit trail, and key rotation. Requires `pgcrypto`.

```sql
CREATE EXTENSION alohadb_vault;
SELECT alohadb_vault_store('api_key', 'sk-123', 'Stripe key');
SELECT alohadb_vault_fetch('api_key');
SELECT * FROM alohadb_vault_list();
```

**Use case:** Safely store API keys and credentials that other in-database extensions need at runtime.

### `alohadb_query_store` — Query store & index advisor

Query Store with per-statement timing history plus an index advisor and unused-index detector.

```sql
CREATE EXTENSION alohadb_query_store;
SELECT * FROM query_store_entries() ORDER BY total_time DESC LIMIT 10;
SELECT * FROM index_advisor_recommend();
SELECT * FROM index_advisor_unused_indexes();
```

**Use case:** Find your slowest queries and get concrete index recommendations to fix them.

### `alohadb_remote_config` — Centralized configuration

Centralized configuration management, backing a fleet-wide management schema and control plane.

```sql
CREATE EXTENSION alohadb_remote_config;
SELECT * FROM remote_config_status();
```

**Use case:** Manage and push configuration to many AlohaDB servers from a central control plane.

## Developer, Schema & Automation

### `alohadb_branch` — Database branching

Lightweight database branching for testing migrations and experiments, optionally from a WAL LSN. Each branch is a separate server (created with `al_basebackup`, started with `al_ctl`) on its own port under `<data>/../branches/<name>`. Branches do not load port-binding or side-effecting workers (`alohadb_pool`, `alohadb_restapi`, `alohadb_cron`, `alohadb_kafka`, `alohadb_scale`) and keep their audit logs separate.

```sql
CREATE EXTENSION alohadb_branch;
SELECT * FROM alohadb_create_branch('feature-x');
SELECT * FROM alohadb_list_branches();
SELECT alohadb_drop_branch('feature-x');
```

**Use case:** Spin up an isolated copy of the database to test a risky migration, then discard it.

### `alohadb_schema_change` — Online DDL

Online schema changes (DDL) performed via a copy-and-swap process to avoid long table locks.

```sql
CREATE EXTENSION alohadb_schema_change;
SELECT online_alter_table('ALTER TABLE orders ADD COLUMN note text');
SELECT * FROM online_alter_status();
```

**Use case:** Alter large production tables without blocking writes during the change.

### `alohadb_jsonschema` — JSON Schema validation

JSON Schema Draft-07 validation, usable in CHECK constraints and via a named-schema registry.

```sql
CREATE EXTENSION alohadb_jsonschema;
SELECT jsonschema_register('user', '{"type":"object","required":["name"]}');
SELECT jsonschema_validate_named('{"name":"Kai"}', 'user');
SELECT * FROM jsonschema_validate('{}', '{"required":["name"]}');
```

**Use case:** Enforce the shape of `jsonb` columns at write time with reusable schemas.

### `alohadb_cron` — Cron job scheduling

Job scheduling with standard cron syntax, including named jobs and run history.

```sql
CREATE EXTENSION alohadb_cron;
SELECT cron_schedule_named('nightly_vacuum', '0 3 * * *', 'VACUUM ANALYZE');
SELECT * FROM cron_job_status();
```

**Use case:** Run recurring maintenance or data jobs on a schedule without an external scheduler.

### `alohadb_pipeline` — DAG-based ETL orchestration

DAG-based ETL pipeline orchestration with task dependency tracking and run history.

```sql
CREATE EXTENSION alohadb_pipeline;
SELECT pipeline_create('nightly_etl');
SELECT pipeline_task_add('nightly_etl', 'load', 'sql', 'INSERT INTO staging ...');
SELECT pipeline_task_depend('nightly_etl', 'transform', 'load');
SELECT pipeline_run('nightly_etl');
```

**Use case:** Orchestrate multi-step ETL with dependencies entirely inside the database.

### `alohadb_python` — Managed Python execution

Python as a first-class workload with a script registry, environments, and managed execution. Code may `return` a value or assign `_result`; arguments are available as `args` (dict), `_args` (JSON text) and as plain variables named after each key. Requires a server built with PL/Python (`-Dplpython=enabled`).

```sql
CREATE EXTENSION alohadb_python CASCADE;   -- also installs plpython3u
SELECT python_eval('return {"sum": args["a"] + args["b"]}', '{"a":2,"b":3}');
SELECT python_register('normalize', 'return value.strip().lower()');
SELECT python_exec('normalize', '{"value":"  Hello "}');
```

**Use case:** Register and version reusable Python transforms and call them from SQL.

### `alohadb_codefs` — Versioned virtual filesystem

A virtual filesystem with git-like versioning, branches, locks, and policies for AI-native development.

```sql
CREATE EXTENSION alohadb_codefs;
SELECT codefs_write('/src/app.py', 'print("hi")', 'main', 'initial commit');
SELECT codefs_read('/src/app.py');
SELECT * FROM codefs_history('/src/app.py');
SELECT * FROM codefs_branch_merge('feature', 'main');
```

**Use case:** Give AI agents a versioned, branchable file workspace stored transactionally in the database.

### `alohadb_geo` — 2D geometry & spatial indexing

2D geometry types, spatial predicates, and GiST bounding-box indexing with familiar `ST_` functions.

```sql
CREATE EXTENSION alohadb_geo;
SELECT ST_DistanceSphere(ST_Point(-157.8, 21.3), ST_Point(-156.3, 20.8));
SELECT ST_Contains(poly, ST_Point(-157.8, 21.3)) FROM regions;
```

**Use case:** Store points and polygons and run distance and containment queries with bounding-box indexes.

## Building for macOS, Linux & Windows

AlohaDB builds from source with Meson + Ninja (the toolchain PostgreSQL 18 uses), producing the server plus the bundled `alohadb_*` extensions. Build natively on each OS. A few extensions depend on external native libraries (ONNX Runtime, Apache Arrow, librdkafka, OpenCL/CUDA): the core server builds everywhere, while full extension parity on Windows may require a reduced set.

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

## Release notes

### November 2026

- Based on PostgreSQL **18.7** (from 18.3). Same on-disk format: existing clusters run on the new binaries without `al_upgrade`. `output_plugin_libraries` (new in 18.7) defaults to `pgoutput, test_decoding, alohadb_cdc_arrow`.
- Tooling fixed for AlohaDB's file names: base backups no longer copy `alohadb.pid`; `al_basebackup -R` writes standby settings to `alohadb.auto.conf`; `al_resetwal` detects a running server; `al_dumpall`, `al_verifybackup`, `al_ctl initdb`, `al_createsubscriber` and `al_upgrade` find their helper programs without workarounds.
- Online DDL: `ALTER COLUMN ... TYPE` to a toastable type (e.g. `int` -> `text`) works again.
- `alohadb_ontology`, `alohadb_context`, `alohadb_intent` 1.1 (multi-sense disambiguation, bidirectional cycle-safe graph traversal, change history); new `alohadb_pg19compat`; `alohadb_unified` 1.1.
- Fixed: `alohadb_vector` installs and searches correctly; `alohadb_nosql` string/array filters, sort and projection; `alohadb_python` execution; `alohadb_branch` start/stop safety; an `alohadb_objectstore` crash on credentials without keys.
- Extension data (ontology, context, intent, vector, python registries) is included in `al_dump` / `al_dumpall`.

---

## Documentation

Full documentation: <https://vernallworks.com/docs-alohadb.php>

## License

AlohaDB extensions are MIT-licensed; the PostgreSQL base is under the PostgreSQL License. See `COPYRIGHT` and `LICENSE`.
