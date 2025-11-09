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


# Test queries:

```rkl
SELECT key, value FROM random-data LIMIT 5
```

```rkl
SELECT key, value FROM random-data WHERE value->data->field3 = false LIMIT 5
```

# Tests 

One simple test to validate parsing for a sample query.
