# Environments & SSL

RKL lets you manage multiple Kafka environments (hosts + credentials) from the TUI, and it stores them as JSON files under your home directory so you can also edit them directly.

## Managing environments

- Press `F2` (or use `:` and pick **Open Environments**) to open the Environments manager.
- The left list stores named hosts; the right pane contains fields for broker URL plus optional PEM fields for private key, certificate, and CA.
- Slash leader shortcuts: `/n` new, `/d` delete, `/s` save, `/t` test, `/]` `/[` cycle environments. Function keys still work (`F1`, `F3`, `F4`, `F5`, `F6`, `F7`).
- Use `Tab` / `Shift-Tab` to move between fields.
- When the PEM editor is focused, use `/]` `/[` (or `Ctrl-Left/Right`) to switch PEM tabs.

## SSL and connectivity

- Fields accept pasted PEM blobs.
- Use `/t` (or `F5`) to test connectivity with the currently edited credentials; output streams into the Connection log panel.
- For end-to-end TLS experiments (including mTLS), see `local-test/README.md` for a docker-compose scenario.

## Where environments are stored

RKL persists environments as JSON files so they are available across runs and can be scripted or checked into dotfiles.

- The directory is `~/.rkl/configs/envs` (or `$HOME/.rkl/configs/envs`). Legacy configs under `~/.rkl/envs` are still loaded for compatibility.
- Each environment lives in its own `*.json` file.
- The **file name** does not need to match the environment name, but should end in `.json`.

On startup, RKL scans this directory, loads every JSON file that matches the format below, and shows them in the Environments list sorted by `name`.

## Editing environment JSON directly

Instead of using only the TUI, you can create or edit environment files directly under `~/.rkl/configs/envs`.

### JSON schema

Each environment file has this shape:

```jsonc
{
  "name": "Dev cluster",                // label shown in the TUI
  "host": "localhost:9092",            // Kafka bootstrap.servers
  "private_key_pem": null,              // optional PEM-encoded private key
  "public_key_pem": null,               // optional PEM-encoded certificate
  "ssl_ca_pem": null                    // optional PEM-encoded CA certificate
}
```

Notes:

- `name` (**required**) is a free-form string used as the display name in the Environments list.
- `host` (**required**) is passed directly to Kafka as `bootstrap.servers`.
- `private_key_pem`, `public_key_pem`, and `ssl_ca_pem` are all optional:
  - You can omit them entirely, or
  - Set them to `null`, or
  - Provide PEM content as a JSON string.

### PEM formatting

You can supply PEMs in two ways:

1. **Multi-line JSON strings** (easiest when editing by hand):

   ```json
   {
     "name": "TLS cluster",
     "host": "broker.example.com:9093",
     "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
     "public_key_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
     "ssl_ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"
   }
   ```

   Here `\n` are literal newline escapes inside a single JSON string. RKL automatically converts these into real newlines before passing them to Kafka.

2. **Literal newlines** (if your editor is comfortable with multi-line JSON strings):

   ```json
   {
     "name": "TLS cluster",
     "host": "broker.example.com:9093",
     "private_key_pem": "-----BEGIN PRIVATE KEY-----
... full key here ...
-----END PRIVATE KEY-----\n",
     "public_key_pem": "-----BEGIN CERTIFICATE-----
... full cert here ...
-----END CERTIFICATE-----\n",
     "ssl_ca_pem": "-----BEGIN CERTIFICATE-----
... full CA here ...
-----END CERTIFICATE-----\n"
   }
   ```

   When RKL later saves environments from the TUI, it rewrites PEMs into a single line with `\n` escapes to keep the JSON easier to parse.

### Creating a new environment via file

1. Create the directory if it does not exist:

   ```sh
   mkdir -p ~/.rkl/configs/envs
   ```

2. Add a file like `~/.rkl/configs/envs/dev.json` with the JSON schema above.
3. Start or restart `rkl` and open `F2` (Envs); the new environment should appear under the `name` you set.

You can mix TUI-edited environments and manually created JSON files; RKL will load all of them.
