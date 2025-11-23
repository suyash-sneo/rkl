# Query editor features

The query editor in RKL behaves more like a lightweight IDE than a single-line prompt.

## History (per-session and persistent)

- Press `Ctrl-R` in the editor to open the Query History popup.
- Use Up/Down (and PageUp/PageDown) to select a previous query.
- Press `Enter` to jump to that query if it already exists in the buffer, or append it as a new query at the end and move the cursor there.
- Press `Esc` to close the popup without changing the editor.
- History is saved under `~/.rkl/history/query-history.txt` so your queries are available in future runs (legacy `~/.rkl/envs/query-history.txt` is still read).

## Multi-line editing

- `Enter` inserts a newline; queries can span multiple lines.
- `Home` / `End` move within the current line.
- `Ctrl-Home` / `Ctrl-End` jump to the start or end of the buffer.
- `PageUp` / `PageDown` scroll the editor viewport.
- Word-wise movement and deletion are available with `Ctrl/Alt+Left/Right` and `Ctrl/Alt+Backspace/Delete` (subject to terminal support).
- The footer shows `Ln X, Col Y` while you edit, so it is easy to reason about longer statements.

These keys work naturally on macOS terminals (Terminal.app, iTerm2, kitty, etc.).

## Inline parse status

RKL continuously parses the current query under the cursor:

- When the query is valid, the footer shows `Parse: OK`.
- On syntax errors, the footer shows a `Parse error: <message>` snippet so you can fix issues before running.

Parse status is independent from run status in the Status panel; it never overwrites run results.

## Quick re-run of last query

When you have already run a `SELECT` successfully, you can re-run it without touching the editor:

- `Ctrl-Shift-Enter` or `Ctrl-Shift-R` re-runs the last successfully executed query.
- The re-run uses the exact text from the last run, regardless of where the cursor currently is.
- This is especially handy when tweaking environments or running the same query repeatedly.
