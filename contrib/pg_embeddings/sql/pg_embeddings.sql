--
-- pg_embeddings regression test
--
CREATE EXTENSION pg_embeddings;
SET client_min_messages = warning;

-- Deterministic fake embedders.
-- v1: 4 dims = 0.1 + occurrences of 'cat', 'dog', 'car', 'bike'.
CREATE FUNCTION occ(t text, w text) RETURNS real
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT ((length(lower(t)) - length(replace(lower(t), w, ''))) / length(w))::real $$;

CREATE FUNCTION fake_embed_v1(t text) RETURNS vector
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT array_to_vector(ARRAY[0.1 + occ(t, 'cat'), 0.1 + occ(t, 'dog'),
                                0.1 + occ(t, 'car'), 0.1 + occ(t, 'bike')]::real[]) $$;

-- v2 (batch form): 3 dims = [animals, vehicles, 1].
CREATE FUNCTION fake_embed_v2(t text[]) RETURNS vector[]
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT array_agg(array_to_vector(ARRAY[occ(x, 'cat') + occ(x, 'dog'),
                                          occ(x, 'car') + occ(x, 'bike'), 1]::real[])
                    ORDER BY o)
     FROM unnest(t) WITH ORDINALITY AS u(x, o) $$;

-- broken: claims 4 dims but returns 3
CREATE FUNCTION broken_embed(t text) RETURNS vector
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT '[1,2,3]'::vector $$;

CREATE FUNCTION wrong_sig(t int) RETURNS vector
LANGUAGE sql AS $$ SELECT '[1]'::vector $$;

------------------------------------------------------------------
-- Model registry
------------------------------------------------------------------
SELECT embedding_model_register('fake', 'v1', 4, 'cosine', 'fake_embed_v1(text)',
                                '{"note": "keyword counter"}') AS v1_id;
SELECT embedding_model_register('fake', 'v2', 3, 'l2', 'fake_embed_v2(text[])') AS v2_id;
SELECT embedding_model_register('broken', '1', 4, 'cosine', 'broken_embed(text)') AS broken_id;
SELECT embedding_model_register('manual', '1', 2);                      -- no embed_fn
SELECT embedding_model_register('fake', 'v1', 4);                       -- duplicate
SELECT embedding_model_register('a@b', '1', 4);                         -- bad name
SELECT embedding_model_register('x', '1', 4, 'hamming');                -- bad metric
SELECT embedding_model_register('x', '1', 4, 'cosine', 'wrong_sig(int)'); -- bad fn
SELECT model_id, model, dims, metric, embed_fn, batch, metadata, retired_at
  FROM embedding_models();

------------------------------------------------------------------
-- Tracking
------------------------------------------------------------------
CREATE TABLE docs (
    id       int PRIMARY KEY,
    body     text,
    category text,
    views    int DEFAULT 0
);
INSERT INTO docs (id, body, category) VALUES
    (1, 'The cat sat on the mat', 'pets'),
    (2, 'A dog chased the cat', 'pets'),
    (3, 'My car needs new tires', 'vehicles'),
    (4, 'Riding a bike beats driving a car', 'vehicles'),
    (5, 'The dog sleeps', 'pets');

CREATE TABLE nokey (a int, b text);
SELECT embedding_track('nokey', 'a', 'b', 'fake', 'v1');               -- no unique key
SELECT embedding_track('docs', 'id', 'no_such_col', 'fake', 'v1');     -- bad expression
SELECT embedding_track('docs', 'id', 'body; DROP TABLE docs', 'fake', 'v1');
SELECT embedding_track('docs', 'id', 'body', 'nope', 'v1');            -- unknown model

SELECT embedding_track('docs', 'id', 'body', 'fake', 'v1');
SELECT embedding_track('docs', 'id', 'body', 'fake@v1', NULL);         -- already tracked
\d docs_embeddings
SELECT model_id, key FROM embedding_queue ORDER BY id;

------------------------------------------------------------------
-- Processing
------------------------------------------------------------------
SELECT embedding_process(2);
SELECT count(*) AS queued FROM embedding_queue;
SELECT embedding_process();
SELECT key, embedding, binary_code, status FROM docs_embeddings ORDER BY key;
SELECT * FROM embedding_status('docs');

------------------------------------------------------------------
-- Triggers enqueue only on content change
------------------------------------------------------------------
UPDATE docs SET views = views + 1, category = 'cars' WHERE id = 3;  -- not content
SELECT count(*) AS queued FROM embedding_queue;
UPDATE docs SET body = body WHERE id IN (1, 2);                     -- same content
SELECT count(*) AS queued FROM embedding_queue;
UPDATE docs SET body = 'My car needs a new car battery' WHERE id = 3;
SELECT key FROM embedding_queue;
SELECT key, status FROM docs_embeddings WHERE key = 3;
-- the stale vector stays searchable until processed
SELECT * FROM embedding_status('docs');
SELECT embedding_process();
SELECT key, embedding, status FROM docs_embeddings WHERE key = 3;
-- content changed then reverted before processing: process just re-validates
UPDATE docs SET body = 'temporary' WHERE id = 1;
UPDATE docs SET body = 'The cat sat on the mat' WHERE id = 1;
SELECT key FROM embedding_queue;
SELECT embedding_process() AS embedded;
SELECT key, status FROM docs_embeddings WHERE key = 1;
-- insert + delete before processing
INSERT INTO docs (id, body, category) VALUES (6, 'cat and dog and car', 'misc');
SELECT key FROM embedding_queue;
DELETE FROM docs WHERE id = 6;
SELECT count(*) AS queued FROM embedding_queue;
-- delete removes stored vectors
INSERT INTO docs (id, body, category) VALUES (7, 'bike bike bike', 'vehicles');
SELECT embedding_process();
DELETE FROM docs WHERE id = 7;
SELECT count(*) FROM docs_embeddings WHERE key = 7;
-- key change: old key forgotten, new key enqueued
UPDATE docs SET id = 50 WHERE id = 5;
SELECT key FROM embedding_queue;
SELECT count(*) FROM docs_embeddings WHERE key = 5;
UPDATE docs SET id = 5 WHERE id = 50;
SELECT embedding_process();
-- NULL content: nothing to embed
INSERT INTO docs (id, body) VALUES (8, NULL);
SELECT count(*) AS queued FROM embedding_queue;
DELETE FROM docs WHERE id = 8;

------------------------------------------------------------------
-- Dimension enforcement
------------------------------------------------------------------
INSERT INTO docs_embeddings (key, model_id, embedding, content_hash)
VALUES (1, 1, '[1,2,3]', '\x00');
UPDATE docs_embeddings SET embedding = '[1,2]' WHERE key = 1 AND model_id = 1;
SELECT embedding_search('docs', '[1,0,0]');                          -- query dims

------------------------------------------------------------------
-- Search (active model fake@v1, cosine)
------------------------------------------------------------------
-- cos dist to [1,0,0,0]: doc1 [1.1,.1,.1,.1] -> 1 - 1.1/sqrt(1.24) = 0.0122
SELECT key, round(distance::numeric, 4) AS distance
  FROM embedding_search('docs', '[1,0,0,0]', 5);
SELECT key, round(distance::numeric, 4) AS distance
  FROM embedding_search_text('docs', 'dog', 3);
SELECT key, round(distance::numeric, 4) AS distance
  FROM embedding_search('docs', fake_embed_v1('car'), 2, 'fake', 'v1');

------------------------------------------------------------------
-- Hybrid search (RRF)
------------------------------------------------------------------
-- cosine ranks for fake_embed_v1('cat') = [1.1,.1,.1,.1]: docs 1,2,4,5,3
-- (cos sim .9839/.7042/.1955/.1935/.1449); text ranks for 'dog': docs 2,5
-- doc2 = 1/(60+2)+1/(60+1) = 0.032522, doc5 = 1/64+1/62 = 0.031754,
-- doc1 = 1/61 = 0.016393, doc4 = 1/63 = 0.015873, doc3 = 1/65 = 0.015385
SELECT key, round(score::numeric, 6) AS score, vector_rank, text_rank
  FROM embedding_hybrid_search('docs', fake_embed_v1('cat'), 'dog', 5);
-- relational filter: only pets (1,2,5)
-- doc2 = 1/62+1/61, doc5 = 1/63+1/62 = 0.032002, doc1 = 1/61
SELECT key, round(score::numeric, 6) AS score, vector_rank, text_rank
  FROM embedding_hybrid_search('docs', fake_embed_v1('cat'), 'dog', 5,
                               filter => $$category = 'pets'$$);
-- text only / vector only / explicit text column / rrf_k
SELECT key, vector_rank, text_rank
  FROM embedding_hybrid_search('docs', NULL, 'car', 5, text_column => 'body');
-- rrf_k = 0: doc3 = text #1 + vector #5 -> 1/1 + 1/5 = 1.2,
-- doc4 = vector #1 -> 1/1, doc2 = vector #2 -> 1/2
SELECT key, round(score::numeric, 6) AS score, vector_rank, text_rank
  FROM embedding_hybrid_search('docs', fake_embed_v1('bike'), 'battery', 3, rrf_k => 0);
SELECT * FROM embedding_hybrid_search('docs', NULL, 'dog', filter => 'true; DROP TABLE docs');
SELECT * FROM embedding_hybrid_search('docs', NULL, 'dog', text_column => 'nope');

------------------------------------------------------------------
-- Quantization
------------------------------------------------------------------
SELECT embedding_binary_quantize('[0.5,-1,0,2]');
SELECT embedding_hamming(B'1001', B'0011');
SELECT embedding_hamming(embedding_binary_quantize('[1,1,1,1]'),
                         embedding_binary_quantize('[-1,1,-1,1]'));
SELECT embedding_int8_quantize('[0.5,-1,0,2]');
SELECT embedding_int8_quantize('[0.5,-1,0,2]', 10);
SELECT embedding_int8_quantize('[0.5,-100,0,2]', 10);
SELECT embedding_int8_quantize(NULL);
SELECT embedding_int8_dequantize('{5,-10,0,20}', 10);
SELECT embedding_int8_quantize('[1,2]', 0);

-- two-stage search on an externally embedded (manual) model: vectors are
-- written straight into the store, nothing is queued
CREATE TABLE pts (id int PRIMARY KEY, label text);
INSERT INTO pts VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
SELECT embedding_track('pts', 'id', 'label', 'manual', '1', 'none');
SELECT count(*) AS queued FROM embedding_queue;
INSERT INTO pts_embeddings (key, model_id, embedding, content_hash)
SELECT id, 4, v::vector, embedding__hash(label)
  FROM pts JOIN (VALUES (1, '[1,0.1]'), (2, '[0.9,-0.2]'), (3, '[-1,0.5]'),
                        (4, '[0.2,1]'), (5, '[-0.3,-1]')) AS x(i, v) ON x.i = pts.id;
SELECT key, binary_code FROM pts_embeddings ORDER BY key;
-- query [1,0.05] -> code 11; hamming 1:0 2:1 3:1 4:0 5:2
SELECT key, round(distance::numeric, 4) AS distance
  FROM embedding_search('pts', '[1,0.05]', 3);
SELECT key, round(distance::numeric, 4) AS distance, hamming
  FROM embedding_search_rerank('pts', '[1,0.05]', 3, 3);
-- too few candidates: prefilter keeps {1,4} only, so 2 is missed
SELECT key, round(distance::numeric, 4) AS distance, hamming
  FROM embedding_search_rerank('pts', '[1,0.05]', 2, 1);
-- content change marks the manual vector stale (not queued)
UPDATE pts SET label = 'z' WHERE id = 2;
SELECT key, status FROM pts_embeddings WHERE key = 2;
SELECT * FROM embedding_status('pts');
SELECT embedding_untrack('pts');

------------------------------------------------------------------
-- Re-embed with fake@v2 side by side, then promote
------------------------------------------------------------------
SELECT embedding_reembed('docs', 'fake', 'v1', 'fake', 'v2') AS enqueued;
SELECT * FROM embedding_status('docs');
-- search still uses v1 (active); v2 has nothing yet
SELECT key FROM embedding_search('docs', fake_embed_v1('cat'), 1);
SELECT count(*) FROM embedding_search('docs', '[1,0,1]', 10, 'fake', 'v2');
SELECT embedding_process();
SELECT key, model_id, embedding FROM docs_embeddings WHERE model_id = 2 ORDER BY key;
SELECT embedding_promote('docs', 'fake@v2');
SELECT * FROM embedding_status('docs');
-- now l2 over [animals, vehicles, 1]; doc3 = [0,2,1] is closest to [0,2,1]
SELECT key, round(distance::numeric, 4) AS distance
  FROM embedding_search('docs', '[0,2,1]', 3);
SELECT key, round(distance::numeric, 4) AS distance
  FROM embedding_search_text('docs', 'cat dog', 2);
-- old version still queryable explicitly
SELECT key FROM embedding_search('docs', fake_embed_v1('cat'), 1, 'fake@v1');
-- both models maintained by the trigger
UPDATE docs SET body = 'The cat sat on the dog' WHERE id = 1;
SELECT model_id, key FROM embedding_queue ORDER BY model_id;
SELECT embedding_process();

-- partial ANN index per model
SELECT indexname, indexdef FROM pg_indexes
 WHERE tablename = 'docs_embeddings' AND indexdef LIKE '%hnsw%' ORDER BY indexname;

------------------------------------------------------------------
-- Failures are recorded, retire / untrack
------------------------------------------------------------------
SELECT embedding_reembed('docs', 'fake', 'v2', 'broken', '1') AS enqueued;
SELECT embedding_process();
SELECT key, attempts, last_error FROM embedding_queue ORDER BY key LIMIT 2;
SELECT embedding_promote('docs', 'manual@1');                          -- not tracked
SELECT embedding_model_retire('fake', 'v2');                           -- active
SELECT embedding_model_retire('broken@1');
SELECT count(*) AS queued FROM embedding_queue;
SELECT embedding_reembed('docs', 'fake', 'v2', 'broken', '1');          -- retired
SELECT embedding_untrack('docs', 'broken', '1');
SELECT embedding_untrack('docs', 'fake', 'v1');
SELECT * FROM embedding_status('docs');
SELECT indexname FROM pg_indexes WHERE tablename = 'docs_embeddings' ORDER BY indexname;

-- TRUNCATE clears the store
TRUNCATE docs;
SELECT count(*) FROM docs_embeddings;

-- untrack whole source
SELECT embedding_untrack('docs');
SELECT to_regclass('docs_embeddings');
SELECT tgname FROM pg_trigger WHERE tgrelid = 'docs'::regclass;
SELECT embedding_status('docs');

DROP EXTENSION pg_embeddings;
