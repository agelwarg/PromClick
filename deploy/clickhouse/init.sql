-- data__tagset schema for PromClick
-- Matches the tagset-based schema used by the companion writer/reader projects.
-- Uses non-replicated engines suitable for single-node deployments.

CREATE DATABASE IF NOT EXISTS data__tagset;

-- ────────────────────────────────────────────────────────────
-- __ts  (Null engine — ingest point; all MVs fire on INSERT)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts
(
    `insert_ts`        DateTime DEFAULT now(),
    `source`           String,
    `fingerprint`      FixedString(16),
    `tag_names`        Array(String),
    `tag_values`       Array(String),
    `__name__`         String MATERIALIZED tag_values[indexOf(tag_names, '__name__')],
    `values__boolean`  Array(Tuple(Int64, UInt8,    LowCardinality(String))),
    `values__float64`  Array(Tuple(Int64, Float64,  LowCardinality(String))),
    `values__int64`    Array(Tuple(Int64, Int64,    LowCardinality(String))),
    `values__string`   Array(Tuple(Int64, String,   LowCardinality(String))),
    `values__uint64`   Array(Tuple(Int64, UInt64,   LowCardinality(String)))
)
ENGINE = Null;

-- ────────────────────────────────────────────────────────────
-- __ts_by_name  (series index — fingerprint + tag arrays)
-- AggregatingMergeTree keeps first/last timestamps per series.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts_by_name
(
    `fingerprint`   FixedString(16),
    `__name__`      LowCardinality(String),
    `__field__`     LowCardinality(String),
    `tag_names`     Array(LowCardinality(String)),
    `tag_values`    Array(String),
    `source`        LowCardinality(String),
    `first_epoch_ns` SimpleAggregateFunction(min, Int64),
    `last_epoch_ns`  SimpleAggregateFunction(max, Int64),
    `insert_ts`     SimpleAggregateFunction(min, DateTime),
    `update_ts`     SimpleAggregateFunction(max, DateTime),
    `first_poll_ts` DateTime ALIAS toDateTime(toInt64(first_epoch_ns / 1000000000)),
    `last_poll_ts`  DateTime ALIAS toDateTime(toInt64(last_epoch_ns  / 1000000000)),
    `__datatype__`  LowCardinality(String)
)
ENGINE = AggregatingMergeTree
ORDER BY (__name__, __field__, fingerprint, tag_names, tag_values)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS data__tagset.__ts_mv_by_name
TO data__tagset.__ts_by_name AS
WITH
    arrayMap(x -> x.1, values__boolean) AS boolean_epochs_ns,
    arrayMap(x -> x.1, values__float64) AS float64_epochs_ns,
    arrayMap(x -> x.1, values__int64)   AS int64_epochs_ns,
    arrayMap(x -> x.1, values__string)  AS string_epochs_ns,
    arrayMap(x -> x.1, values__uint64)  AS uint64_epochs_ns,
    arrayConcat(boolean_epochs_ns, float64_epochs_ns, int64_epochs_ns, string_epochs_ns, uint64_epochs_ns) AS poll_epochs_ns
SELECT
    fingerprint,
    __name__,
    ''           AS __field__,
    tag_names,
    tag_values,
    source,
    min(arrayReduce('min', poll_epochs_ns)) AS first_epoch_ns,
    max(arrayReduce('max', poll_epochs_ns)) AS last_epoch_ns,
    min(insert_ts) AS insert_ts,
    max(insert_ts) AS update_ts
FROM data__tagset.__ts
GROUP BY fingerprint, __name__, __field__, tag_names, tag_values, source;

-- ────────────────────────────────────────────────────────────
-- __ts_by_tag  (inverted tag index — tag → fingerprints)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts_by_tag
(
    `fingerprint` FixedString(16),
    `__name__`    LowCardinality(String),
    `__field__`   LowCardinality(String),
    `tag`         LowCardinality(String),
    `value`       String
)
ENGINE = ReplacingMergeTree
ORDER BY (__name__, __field__, tag, fingerprint)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS data__tagset.__ts_mv_by_tag
TO data__tagset.__ts_by_tag AS
WITH
    arrayMap((t, v) -> [t, v], tag_names, tag_values) AS pairs,
    arrayJoin(pairs) AS pair,
    pair[1] AS tag,
    pair[2] AS value
SELECT DISTINCT
    fingerprint,
    __name__,
    '' AS __field__,
    tag,
    value
FROM data__tagset.__ts;

-- ────────────────────────────────────────────────────────────
-- __ts_samples__float64  (Float64 time-series samples)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts_samples__float64
(
    `fingerprint`   FixedString(16),
    `__name__`      LowCardinality(String),
    `__field__`     LowCardinality(String),
    `poll_epoch_ns` Int64   CODEC(Delta(8), ZSTD(1)),
    `value`         Float64 CODEC(Gorilla(8), ZSTD(1)),
    `insert_ts`     DateTime,
    `poll_ts`       DateTime ALIAS toDateTime(toInt64(poll_epoch_ns / 1000000000))
)
ENGINE = ReplacingMergeTree(insert_ts)
PARTITION BY toDate(poll_epoch_ns / 1000000000)
ORDER BY (__name__, __field__, fingerprint, poll_epoch_ns)
TTL toDateTime(toInt64(poll_epoch_ns / 1000000000)) + toIntervalDay(365)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS data__tagset.__ts_mv_samples_float64
TO data__tagset.__ts_samples__float64 AS
WITH
    arrayJoin(values__float64) AS sample,
    sample.1 AS poll_epoch_ns,
    sample.2 AS value,
    sample.3 AS __field__
SELECT
    fingerprint,
    __name__,
    __field__,
    poll_epoch_ns,
    value,
    insert_ts
FROM data__tagset.__ts
WHERE notEmpty(values__float64);

-- ────────────────────────────────────────────────────────────
-- __ts_names  (metric name registry)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts_names
(
    `__name__` LowCardinality(String),
    `type`     String DEFAULT 'gauge',
    `help`     Nullable(String),
    `unit`     Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY __name__
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS data__tagset.__ts_mv_names
TO data__tagset.__ts_names AS
SELECT DISTINCT
    __name__,
    'gauge' AS type,
    __name__ AS help,
    ''       AS unit
FROM data__tagset.__ts;

-- ────────────────────────────────────────────────────────────
-- __ts_tags  (all tag names seen)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts_tags
(
    `tag` LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY tag
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS data__tagset.__ts_mv_tags
TO data__tagset.__ts_tags AS
WITH arrayJoin(tag_names) AS tag
SELECT DISTINCT tag
FROM data__tagset.__ts;

-- ────────────────────────────────────────────────────────────
-- __ts_tag_values  (all tag values per tag name)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data__tagset.__ts_tag_values
(
    `tag`   LowCardinality(String),
    `value` String
)
ENGINE = ReplacingMergeTree
ORDER BY (tag, value)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS data__tagset.__ts_mv_tag_values
TO data__tagset.__ts_tag_values AS
WITH
    arrayMap((t, v) -> [t, v], tag_names, tag_values) AS pairs,
    arrayJoin(pairs) AS pair,
    pair[1] AS tag,
    pair[2] AS value
SELECT DISTINCT tag, value
FROM data__tagset.__ts;
