# Troubleshooting & performance

## Common issues

- **SSL or SASL handshake errors**: confirm the CA, certificate, and private key PEMs belong to the selected broker; use `F5 Test` inside the Environments screen to validate before running queries.
- **Metadata timeouts or empty topic lists**: verify the broker address, firewall rules, and authentication; run `LIST topics;` after pressing `F6` (Info screen) to refresh metadata.
- **Queries returning no rows**: remove `LIMIT`, double-check `WHERE` clauses (case-sensitive `CONTAINS`), and ensure the timestamp ordering matches your expectation.
- **CLI output wrapping oddly**: tweak `--max-cell-width` or supply `--no-color` when piping into other tools.

## Performance tips

- Think of `timestamp` as an indexed column in a SQL database:
  - Adding `WHERE timestamp >= ... AND timestamp < ...` is analogous to using an index on a `TIMESTAMP` column, and lets RKL seek directly into the desired time range instead of scanning the whole topic.
  - Adding `ORDER BY timestamp DESC LIMIT n` is similar to "give me the last N rows ordered by time" on an indexed table.
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
- If you just need "what's happening now", prefer:
  - `SELECT ... FROM topic ORDER BY timestamp DESC LIMIT n;`
  - This uses the optimized backward windowing from the end of the log and avoids scanning from the beginning.

