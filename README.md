# RKL

RKL (pronounced "racle", like "oracle" with a silent "o") is a terminal UI for exploring Kafka topics with an SQL-like experience. It pairs a query editor, results table, JSON payload viewer, and environment manager so you can inspect data quickly without writing ad-hoc consumers.

![RKL TUI Screenshot](/assets/rkl-screenshot.png?raw=true)

## Feature highlights

- SQL-inspired query engine (`SELECT`, `WHERE`, `ORDER BY timestamp|poffset|poffset_ts`, `LIMIT`) with JSON-path filtering via `value->field->subfield`.
- Real-time results table with horizontal scrolling plus a right-side JSON pane for the focused record.
- Topic inspection with the `LIST topics;` command and an Info screen (F12) that caches broker metadata.
- Fuzzy topic autocomplete triggered after `FROM`, accepted with `Ctrl-Y` (or Right arrow), and navigated with `Ctrl-N`/`Ctrl-P`.
- Environment manager for hosts, credentials, and PEM-encoded CA/cert/key material with a built-in connectivity test.
- Dedicated CLI mode for one-shot queries (`rkl run ...`) when you need to script output or run inside CI.

## How to learn RKL

If you're new to RKL, start here and follow these docs in order:

1. **Getting started** – install, uninstall, and a first run
   - [Getting started & installation](docs/getting-started.md)
2. **Querying Kafka** – learn the language and timestamp semantics
   - [Query language](docs/query-language.md)
   - [High-performance timestamp queries](docs/timestamp-queries.md)
   - [Commands (e.g. `LIST topics;`)](docs/commands.md)
3. **Using the TUI** – editor behavior and navigation
   - [TUI controls (concise key reference)](docs/tui-controls.md)
   - [Query editor features (history, multi-line editing, parse status, quick re-run)](docs/query-editor.md)
   - [Autocomplete](docs/autocomplete.md)
   - [Environments & SSL](docs/environments-and-ssl.md)
4. **CLI mode** – scripting and non-interactive use
   - [CLI usage](docs/cli.md)
5. **Operating the tool** – building, debugging, and tuning
   - [Build from source](docs/build.md)
   - [Troubleshooting & performance tips](docs/troubleshooting-and-performance.md)
6. **Roadmap / ideas**
   - [Future query UI ideas](docs/future-query-ui-ideas.md)

Each document is self-contained, so you can also jump directly to the section that matches what you're trying to do (for example, "How do I filter by timestamp?" → timestamp queries; "What are the editor shortcuts?" → query editor features).

## Quickstart (5-minute version)

For a fast path:

1. Install RKL using the script in [Getting started](docs/getting-started.md).
2. Run `rkl` and:
   - Press `Tab` to focus the Query editor.
   - Paste a query from [Query language](docs/query-language.md) such as:

     ```sql
     SELECT key, value FROM random-data LIMIT 5;
     ```

   - Press `Ctrl-Enter` to run it.
3. Use the arrow keys to explore the results table, and `F10` to open the built-in help for a full keymap.

From there, use the documentation map above to deepen your understanding.

