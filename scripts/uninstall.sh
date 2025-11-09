#!/usr/bin/env bash
set -euo pipefail

# rkl uninstall script
# - Removes the rkl binary from the user install directory
# - Defaults to ~/.local/bin; override with RKL_INSTALL_DIR

BIN_NAME="rkl"
DEFAULT_INSTALL_DIR="$HOME/.local/bin"
INSTALL_DIR="${RKL_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"

log() {
  printf "[rkl-uninstall] %s\n" "$*"
}

warn() {
  printf "[rkl-uninstall] WARNING: %s\n" "$*" >&2
}

die() {
  printf "[rkl-uninstall] ERROR: %s\n" "$*" >&2
  exit 1
}

TARGET_PATH="$INSTALL_DIR/$BIN_NAME"

if [[ ! -e "$TARGET_PATH" ]]; then
  warn "No install found at $TARGET_PATH"
  # Try to show which rkl is active, if any
  if command -v "$BIN_NAME" >/dev/null 2>&1; then
    ACTIVE_PATH="$(command -v "$BIN_NAME")"
    warn "Another rkl exists on PATH at: $ACTIVE_PATH"
  fi
  exit 0
fi

if [[ -d "$TARGET_PATH" ]]; then
  die "$TARGET_PATH exists and is a directory; aborting."
fi

if [[ ! -w "$INSTALL_DIR" ]]; then
  die "Install dir '$INSTALL_DIR' is not writable. Set RKL_INSTALL_DIR to the directory where rkl was installed."
fi

log "Removing $TARGET_PATH"
rm -f "$TARGET_PATH"

# Attempt to remove the directory if empty (best effort)
rmdir "$INSTALL_DIR" 2>/dev/null || true

log "Uninstall complete."

