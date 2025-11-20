# RKL

RKL (pronounced “racle”, like “oracle” with a silent “o”) is a terminal UI for exploring Kafka topics with an SQL-like experience. It pairs a query editor, results table, JSON payload viewer, and environment manager so you can inspect data quickly without writing ad-hoc consumers.

![RKL TUI Screenshot](/assets/rkl-screenshot.png?raw=true)

## Features

- SQL-inspired query engine (`SELECT`, `WHERE`, `ORDER BY timestamp` with DESC default, `LIMIT`) with JSON-path filtering via `value->field->subfield`.
- Real-time results table with horizontal scrolling plus a right-side JSON pane for the focused record.
- Topic inspection with the `LIST topics;` command and an Info screen (F12) that caches broker metadata.
- Fuzzy topic autocomplete triggered after `FROM`, accepted with `Ctrl-Y` (or Right arrow), and navigated with `Ctrl-N`/`Ctrl-P`.
- Environment manager for hosts, credentials, and PEM-encoded CA/cert/key material with a built-in connectivity test.
- Dedicated CLI mode for one-shot queries (`rkl run ...`) when you need to script output or run inside CI.

## Quickstart

1. Install the binary with one of the scripts below (no sudo required).
2. Launch `rkl` to open the Home screen, tab into the Query editor, and press `Ctrl-Enter` to run your `SELECT`.
3. Switch environments with `F2` or Enter on the host bar, and use `LIST topics;` or the sample queries below to explore data.

Install latest:

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/install.sh | bash
```

Install a specific version:

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/install.sh | RKL_VERSION=v0.1.0 bash
```

Custom install location:

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/install.sh | RKL_INSTALL_DIR="$HOME/bin" bash
```

Uninstall (default path `~/.local/bin`):

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/uninstall.sh | bash
```

Uninstall from a custom location:

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/uninstall.sh | RKL_INSTALL_DIR="$HOME/bin" bash
```

## Query Language

- Syntax: `SELECT columns FROM topic [WHERE expr] [ORDER BY timestamp ASC|DESC] [LIMIT n]`.
- ORDER BY timestamp DESC is applied automatically when omitted so queries stream newest data first.
- Filter JSON by walking nested fields with `value->meta->service`, `value->response->status`, etc. `key`, raw `value`, and `timestamp` all support comparisons.
- Operators: `=`, `!=`, `<>`, `CONTAINS`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, and parentheses for grouping. `timestamp` comparisons accept either milliseconds or ISO-8601 strings; values ending in `Z` are treated as UTC while others use your system timezone.
- End queries with `;` to separate multiple statements; the editor highlights the current query under the cursor.

Examples:

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

## High-performance timestamp queries

RKL is optimized for topics with millions or billions of messages. The query engine uses partition-parallel consumers plus timestamp-aware seeking so it only reads the smallest possible window of data that can satisfy your query.

### Ordering by timestamp

- `ORDER BY timestamp DESC` (or omitting `ORDER BY` entirely) means “newest first”.
- RKL seeks near the latest offsets in each partition and scans backwards in windows, instead of starting from the beginning of the topic.
- Results are globally ordered by timestamp across partitions, so it behaves like a database query on a `timestamp` column.

Examples:

```sql
-- Latest 100 messages, newest first (implicit DESC)
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

### Timestamp filters and performance

When you add a `WHERE timestamp ...` clause, RKL uses Kafka’s timestamp index to seek directly into the relevant time window in each partition. That means:

- Far fewer messages are scanned, even on very large topics.
- The TUI shows partial results quickly while it fills in the rest.
- You can use timestamp filters together with other JSON filters and LIMIT.

Supported forms:

- `timestamp >  'YYYY-MM-DDTHH:MM:SS[Z]'`
- `timestamp >= 'YYYY-MM-DDTHH:MM:SS[Z]'`
- `timestamp <  'YYYY-MM-DDTHH:MM:SS[Z]'`
- `timestamp <= 'YYYY-MM-DDTHH:MM:SS[Z]'`
- Any combination using `AND`, for example: `timestamp >= ... AND timestamp < ...`.

Examples on large topics:

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

Behind the scenes, RKL uses the timestamp bounds to derive an effective earliest and latest offset per partition and only scans that window, which keeps queries fast even when the topic contains billions of messages.

### Local time vs UTC

Timestamp comparisons accept either raw millisecond values or ISO-8601 timestamps:

- Values ending with `Z` are treated as UTC, for example:
  - `timestamp >= '2024-01-01T00:00:00Z'`
  - `timestamp <  '2024-01-02T00:00:00Z'`
- Values without `Z` are interpreted in your **system timezone** and converted to UTC internally, for example:
  - `timestamp >= '2024-01-01T09:00:00'` (local 9 AM)
  - `timestamp <  '2024-01-01T18:00:00'` (local 6 PM)

This lets you write queries using either UTC (for reproducible, environment-independent queries) or your local time (for quick, ad-hoc debugging) without doing manual timezone math.

For realistic payloads to experiment with, see `local-test/README.md`.

## Commands (LIST topics)

`LIST topics;` runs against the currently selected environment and switches the results view into topic-list mode. Use the arrow keys or mouse wheel to inspect partitions, `F5` to copy the selected value, and `Tab` to return to the query editor for the next command.

## Autocomplete

- Trigger: type `FROM ` inside a valid `SELECT` statement.
- Suggestions are fuzzy-matched against the cached topic list; refresh the list from the Info screen with `F6`.
- Right arrow accepts the highlighted topic, `Ctrl-N`/`Ctrl-P` move through the list, and `Esc` dismisses the popup.

## TUI controls (concise)

- `Tab` cycles focus between Host bar, Query editor, and Results. The footer displays context-aware hints for each focus.
- `Ctrl-Enter` runs the current `SELECT`. Plain `Enter` inserts a newline.
- `Right` accepts autocomplete suggestions, while `Ctrl-N`/`Ctrl-P` navigate within them.
- `Shift-Left/Right` horizontally scrolls the results table; `F5` copies the value column and `F7` copies the status panel.
- `F2` opens the Environments screen, `F8` jumps Home, `F12` opens the Info screen, and `F10` toggles the full help dialog.
- `Ctrl-Q`/`Ctrl-C` exits at any time.

## Environments & SSL

- Press `F2` or hit `Enter` on the Host bar to open the Environments manager. The left list stores named hosts; the right pane contains fields for broker URL plus optional PEM fields for private key, certificate, and CA.
- Create (`F1`), edit (`F2`), delete (`F3`), and save (`F4`) environments. Use `F5` to test connectivity with the currently edited credentials before returning to the Home screen.
- Fields accept pasted PEM blobs, and `F9` toggles mouse-selection mode for easier copying.
- For end-to-end TLS experiments (including mTLS), try the docker-compose scenario documented in `local-test/README.md`.

## CLI usage

RKL ships with a one-shot CLI that shares the same query parser as the TUI. Either run `rkl run --help` directly or set `RKL_MODE=cli` to make the CLI the default mode.

```sh
# Run a SELECT and print a table once
RKL_MODE=cli rkl run --broker localhost:9092 --query "SELECT key, value FROM random-data LIMIT 20;"

# Use --topic/--search when you just need a key/value grep
rkl run --broker localhost:9092 --topic random-data --search error --max-messages 50
```

CLI flags mirror the environment fields (including `--ssl-ca-pem`, `--ssl-certificate-pem`, and `--ssl-key-pem`) so you can reuse the same credentials outside of the TUI.

## Build

- `cargo build --release` produces the optimized binary in `target/release/rkl`.
- `cargo test` and `cargo clippy` keep the parser and helper crates healthy.
- `cargo run --bin rkl` launches the binary from source; set `RKL_MODE` as needed for TUI vs CLI.

## Troubleshooting

- **SSL or SASL handshake errors**: confirm the CA, certificate, and private key PEMs belong to the selected broker; use `F5 Test` inside the Environments screen to validate before running queries.
- **Metadata timeouts or empty topic lists**: verify the broker address, firewall rules, and authentication; run `LIST topics;` after pressing `F6` (Info screen) to refresh metadata.
- **Queries returning no rows**: remove `LIMIT`, double-check `WHERE` clauses (case-sensitive `CONTAINS`), and ensure the timestamp ordering matches your expectation.
- **CLI output wrapping oddly**: tweak `--max-cell-width` or supply `--no-color` when piping into other tools.

### Performance tips

- Think of `timestamp` as an indexed column in a SQL database:
  - Adding `WHERE timestamp >= ... AND timestamp < ...` is analogous to using an index on a `TIMESTAMP` column, and lets RKL seek directly into the desired time range instead of scanning the whole topic.
  - Adding `ORDER BY timestamp DESC LIMIT n` is similar to “give me the last N rows ordered by time” on an indexed table.
- On very large topics, always narrow your queries by time whenever you can:
  - Combine JSON filters with a time window, for example:
    ```sql
    SELECT key, value
    FROM prod.events
    WHERE value->response->status = 500
      AND timestamp >= '2024-01-10T00:00:00Z'
      AND timestamp <  '2024-01-11T00:00:00Z'
    ORDER BY timestamp DESC
    LIMIT 500;
    ```
  - This pattern behaves much like querying a relational table with `WHERE timestamp BETWEEN ... AND ...` on a timestamp index: RKL only touches the offsets that fall inside that window.
- If you just need “what’s happening now”, prefer:
  - `SELECT ... FROM topic ORDER BY timestamp DESC LIMIT n;`
  - This uses the optimized backward windowing from the end of the log and avoids scanning from the beginning.
