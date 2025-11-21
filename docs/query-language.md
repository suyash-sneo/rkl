# Query language

RKL provides a small, focused SQL-inspired language for querying Kafka topics.

## Syntax

- `SELECT columns FROM topic [WHERE expr] [ORDER BY timestamp|poffset|poffset_ts ASC|DESC] [LIMIT n]`.
- `ORDER BY poffset DESC` is applied automatically when omitted so queries sample the latest offsets per partition without a global timestamp sort.
- `ORDER BY poffset` keeps each partition independent, `ORDER BY poffset_ts` uses the same per-partition sampling but globally re-sorts by timestamp.
- Filter JSON by walking nested fields with `value->meta->service`, `value->response->status`, etc. `key`, raw `value`, and `timestamp` all support comparisons.
- Operators: `=`, `!=`, `<>`, `CONTAINS`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, and parentheses for grouping.
- `timestamp` comparisons accept either milliseconds or ISO-8601 strings; values ending in `Z` are treated as UTC while others use your system timezone.
- End queries with `;` to separate multiple statements; the editor highlights the current query under the cursor.

## Examples

```sql
-- Basic sampling
SELECT key, value FROM random-data LIMIT 5;

-- Simple JSON filter
SELECT key FROM random-data
WHERE value->response->msg CONTAINS 'error';

-- Multiple JSON predicates
SELECT key, value FROM random-data
WHERE value->event->type = 'purchase'
  AND value->response->status = 200;

-- Restrict to an exact UTC day
SELECT key FROM random-data
WHERE timestamp >= '2024-01-01T00:00:00Z'
  AND timestamp <  '2024-01-02T00:00:00Z';

-- Complex boolean filter plus explicit DESC and LIMIT
SELECT key FROM random-data
WHERE (key = 'a' OR key = 'b')
  AND value->foo CONTAINS 'x'
ORDER BY timestamp DESC
LIMIT 100;
```

