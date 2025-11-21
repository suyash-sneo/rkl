# Build

To build RKL from source:

- `cargo build --release` produces the optimized binary in `target/release/rkl`.
- `cargo test` runs the test suite.
- `cargo clippy` (or `cargo clippy -- -D warnings`) keeps the parser and helper crates healthy.
- `cargo run --bin rkl` launches the binary from source; set `RKL_MODE` as needed for TUI vs CLI.

