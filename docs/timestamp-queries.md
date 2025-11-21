# High-performance timestamp queries

RKL is optimized for topics with millions or billions of messages. The query engine uses partition-parallel consumers plus timestamp-aware seeking so it only reads the smallest possible window of data that can satisfy your query.

## Ordering by timestamp

- `ORDER BY timestamp DESC` means "newest first" with a global timestamp sort across partitions.
- If you omit `ORDER BY`, RKL uses `ORDER BY poffset DESC` by default, which tails each partition by offset without reordering across partitions.
- Timestamp-ordered queries seek near the latest offsets in each partition and scan backwards in windows, instead of starting from the beginning of the topic.
- Results are globally ordered by timestamp across partitions, so it behaves like a database query on a `timestamp` column.

## Offset sampling modes

- `ORDER BY poffset DESC` (the default) samples the tail of each partition independently so you see the most recent offsets from each shard with minimal reordering cost.
- `ORDER BY poffset ASC` walks forward from the effective start offset in each partition, which is useful for historical replays.
- `ORDER BY poffset_ts` combines offset-based sampling with a global timestamp sort at the end, so you get deterministic ordering without repeated backward seeks per partition.

### Examples

```sql
-- Latest offsets per partition (implicit ORDER BY poffset DESC)
SELECT partition, offset, timestamp, key, value
FROM random-data
LIMIT 100;

-- Same, but explicit
SELECT key, value
FROM random-data
ORDER BY timestamp DESC
LIMIT 100;

-- Oldest 50 messages (ASC order)
SELECT key, value
FROM random-data
ORDER BY timestamp ASC
LIMIT 50;
```

## Timestamp filters and performance

When you add a `WHERE timestamp ...` clause, RKL uses Kafka's timestamp index to seek directly into the relevant time window in each partition. That means:

- Far fewer messages are scanned, even on very large topics.
- The TUI shows partial results quickly while it fills in the rest.
- You can use timestamp filters together with other JSON filters and `LIMIT`.

Supported forms:

- `timestamp >  'YYYY-MM-DDTHH:MM:SS[Z]'`
- `timestamp >= 'YYYY-MM-DDTHH:MM:SS[Z]'`
- `timestamp <  'YYYY-MM-DDTHH:MM:SS[Z]'`
- `timestamp <= 'YYYY-MM-DDTHH:MM:SS[Z]'`
- Any combination using `AND`, for example: `timestamp >= ... AND timestamp < ...`.

### Examples on large topics

```sql
-- Errors in the last hour, newest first
SELECT key, value
FROM prod.events
WHERE value->response->msg CONTAINS 'error'
  AND timestamp >= '2024-01-01T12:00:00Z'
ORDER BY timestamp DESC
LIMIT 200;

-- Traffic from yesterday (full day window)
SELECT partition, offset, timestamp, key
FROM access.logs
WHERE timestamp >= '2024-01-02T00:00:00Z'
  AND timestamp <  '2024-01-03T00:00:00Z'
ORDER BY timestamp DESC
LIMIT 500;

-- Oldest events from a specific maintenance window
SELECT key, value
FROM maintenance.events
WHERE timestamp >= '2024-01-05T22:00:00Z'
  AND timestamp <  '2024-01-06T02:00:00Z'
ORDER BY timestamp ASC
LIMIT 100;

-- Drill into a narrow 5-minute interval
SELECT key, value
FROM random-data
WHERE timestamp >= '2024-01-10T14:30:00Z'
  AND timestamp <  '2024-01-10T14:35:00Z'
  AND value->event->type = 'purchase'
ORDER BY timestamp DESC;
```

## Local time vs UTC

Timestamp comparisons accept either raw millisecond values or ISO-8601 timestamps:

- Values ending with `Z` are treated as UTC, for example:
  - `timestamp >= '2024-01-01T00:00:00Z'`
  - `timestamp <  '2024-01-02T00:00:00Z'`
- Values without `Z` are interpreted in your **system timezone** and converted to UTC internally, for example:
  - `timestamp >= '2024-01-01T09:00:00'` (local 9 AM)
  - `timestamp <  '2024-01-01T18:00:00'` (local 6 PM)

This lets you write queries using either UTC (for reproducible, environment-independent queries) or your local time (for quick, ad-hoc debugging) without doing manual timezone math.

