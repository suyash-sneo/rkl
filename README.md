# RKL

A TUI kafka tool that allows you to write SQL-like queries to read data from Kafka topics.

![RKL TUI Screenshot](/assets/rkl-screenshot.png?raw=true)

# Instructions

## Installation
Install latest (no sudo):
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

## Uninstall
Default location (`~/.local/bin`):
```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/uninstall.sh | bash
```

Uninstall from a custom location:
```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/uninstall.sh | RKL_INSTALL_DIR="$HOME/bin" bash
```

# Progress

## Query Parsing

Simple parser is built which can parse
1. SELECT can be key[, value]
2. FROM is the topic name
3. WHERE currently takes one equality condition for JSON field comparison.
4. ORDER BY takes timestamp only but can be followed by ASC/DESC
5. LIMIT takes a number

## TUI

Main TUI with: 
1. Envs window
2. Query editor (with highlighting for current and last-run query) plus SQL syntax highlighting
3. Output with table-like results
4. Side output-pane for full data under the cursor
5. Help footer

Env TUI with:
1. Left list of envs (basically hosts)
2. Right side for inputs like hots, private/public key, CA, etc
 3. Function key controls: F1 New, F2 Edit, F3 Delete, F4 Save, F5 Test (shows progress/errors at bottom), F6/F7 move between fields, Tab/Shift-Tab move, Up/Down select. Esc closes.

For testing locally with mTLS, see `local-test/README.md`.


# Query examples

These examples exercise the WHERE clause features (parentheses, AND/OR precedence, =, !=/<> and CONTAINS) against the realistic payloads produced by `local-test/producer.py` into the `random-data` topic. Each message looks roughly like:

```
{
  "meta": { "id": "<uuid>", "timestamp": <ms>, "service": "auth|orders|billing|catalog|search", "env": "prod|staging", "region": "us-east-1|eu-west-1|ap-south-1" },
  "request": { "method": "GET|POST|PUT|DELETE", "path": "/api/v1/..." },
  "response": { "status": <int>, "duration_ms": <int>, "size_bytes": <int>, "msg": "ok|...error..." },
  "user": { "id": "<uuid>", "role": "admin|customer|service", "country": "US|DE|IN|GB|BR" },
  "event": { "type": "login|purchase|logout|password_reset|view", "success": <bool> }
}
```

- Basic listing
  - `SELECT key, value FROM random-data LIMIT 5`

- JSON path equality/inequality
  - `SELECT key, value FROM random-data WHERE value->request->method = 'PUT'`
  - `SELECT key, value FROM random-data WHERE value->request->method != 'GET'`
  - `SELECT key, value FROM random-data WHERE value->response->status <> 200`
  - `SELECT key, value FROM random-data WHERE value->event->type = 'purchase' AND value->event->success = true`

- CONTAINS on key/value and nested fields (case-sensitive substring)
  - `SELECT key, value FROM random-data WHERE key CONTAINS 'auth-prod'`  -- keys look like `service-env-region:<user8>`
  - `SELECT key, value FROM random-data WHERE value CONTAINS 'error'`
  - `SELECT key, value FROM random-data WHERE value->response->msg CONTAINS 'error'`

- Parentheses and precedence (AND > OR)
  - `SELECT key, value FROM random-data WHERE (value->meta->service = 'orders' OR value->meta->service = 'billing') AND value->request->method <> 'GET'`
  - `SELECT key, value FROM random-data WHERE value->response->msg CONTAINS 'error' OR (value->event->type = 'purchase' AND value->response->status = 200)`

- ORDER/LIMIT examples
  - `SELECT key FROM random-data ORDER BY timestamp DESC LIMIT 20`
  - `SELECT key, value FROM random-data WHERE value->response->status >= 500 ORDER BY timestamp ASC LIMIT 10`  (note: ORDER BY timestamp only; comparison shown for illustration)

# Tests 

One simple test to validate parsing for a sample query.
