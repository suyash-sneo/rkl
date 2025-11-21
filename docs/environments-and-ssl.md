# Environments & SSL

RKL lets you manage multiple Kafka environments (hosts + credentials) from the TUI.

## Managing environments

- Press `F2` or hit `Enter` on the Host bar to open the Environments manager.
- The left list stores named hosts; the right pane contains fields for broker URL plus optional PEM fields for private key, certificate, and CA.
- Use `F1` to create, `F2` to edit, `F3` to delete, and `F4` to save environments.
- Use `Tab` / `Shift-Tab` to move between fields.

## SSL and connectivity

- Fields accept pasted PEM blobs.
- `F5` tests connectivity with the currently edited credentials before returning to the Home screen.
- For end-to-end TLS experiments (including mTLS), see `local-test/README.md` for a docker-compose scenario.

