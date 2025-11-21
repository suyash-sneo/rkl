# CLI usage

RKL ships with a one-shot CLI that shares the same query parser as the TUI.

Either run `rkl run --help` directly or set `RKL_MODE=cli` to make the CLI the default mode.

```sh
# Run a SELECT and print a table once
RKL_MODE=cli rkl run --broker localhost:9092 --query "SELECT key, value FROM random-data LIMIT 20;"

# Use --topic/--search when you just need a key/value grep
rkl run --broker localhost:9092 --topic random-data --search error --max-messages 50
```

CLI flags mirror the environment fields (including `--ssl-ca-pem`, `--ssl-certificate-pem`, and `--ssl-key-pem`) so you can reuse the same credentials outside of the TUI.

