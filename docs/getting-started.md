# Getting started

RKL (pronounced "racle", like "oracle" with a silent "o") is a terminal UI for exploring Kafka topics with an SQL-like experience. It pairs a query editor, results table, JSON payload viewer, and environment manager so you can inspect data quickly without writing ad-hoc consumers.

## Quickstart

1. Install the binary with one of the scripts below (no sudo required).
2. Launch `rkl` to open the Home screen, use `Tab` to move into the query inputs, and press `Ctrl-Enter` to run your `SELECT`.
3. Switch environments with `F2` (or `:` → **Open Environments**), and use `LIST topics;` or the sample queries in the query language docs to explore data.

## Install

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

## Uninstall

Uninstall (default path `~/.local/bin`):

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/uninstall.sh | bash
```

Uninstall from a custom location:

```sh
curl -fsSL https://raw.githubusercontent.com/suyash-sneo/rkl/HEAD/scripts/uninstall.sh | RKL_INSTALL_DIR="$HOME/bin" bash
```
