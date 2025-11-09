#!/usr/bin/env bash
set -euo pipefail

# rkl install script
# - Detects macOS Apple Silicon
# - Downloads the latest (or specified) release asset from GitHub
# - Verifies SHA256
# - Installs to a user-writable directory (default: ~/.local/bin)
# - Avoids quarantine by downloading via curl (Terminal)

OWNER_REPO="suyash-sneo/rkl"
BIN_NAME="rkl"
DEFAULT_INSTALL_DIR="$HOME/.local/bin"

log() {
  printf "[rkl-install] %s\n" "$*"
}

die() {
  printf "[rkl-install] ERROR: %s\n" "$*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "Required dependency '$1' not found in PATH"
}

# 1) Check platform
OS="$(uname -s)"
ARCH="$(uname -m)"
if [[ "$OS" != "Darwin" ]]; then
  die "This installer currently supports macOS only."
fi
if [[ "$ARCH" != "arm64" ]]; then
  die "This installer currently supports Apple Silicon (arm64) only."
fi

TARGET_TRIPLE="aarch64-apple-darwin"

# 2) Requirements
need curl
need shasum

# unzip/tar used depending on asset format
HAS_UNZIP=1
if ! command -v unzip >/dev/null 2>&1; then HAS_UNZIP=0; fi
HAS_TAR=1
if ! command -v tar >/dev/null 2>&1; then HAS_TAR=0; fi

# 3) Inputs
INSTALL_DIR="${RKL_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
VERSION_TAG="${RKL_VERSION:-}"

mkdir -p "$INSTALL_DIR"
if [[ ! -w "$INSTALL_DIR" ]]; then
  die "Install dir '$INSTALL_DIR' is not writable. Set RKL_INSTALL_DIR to a user-writable location."
fi

# 4) Determine version tag
if [[ -z "$VERSION_TAG" ]]; then
  log "Resolving latest release tag from GitHub..."
  # Use GitHub API (unauthenticated). Fall back to /releases/latest redirect if needed.
  if command -v python3 >/dev/null 2>&1; then
    VERSION_TAG=$(curl -fsSL "https://api.github.com/repos/${OWNER_REPO}/releases/latest" | python3 -c 'import sys, json; print(json.load(sys.stdin)["tag_name"])') || true
  fi
  if [[ -z "${VERSION_TAG:-}" ]]; then
    VERSION_TAG=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${OWNER_REPO}/releases/latest" | sed -E 's#.*/tag/([^/?]+).*#\1#') || true
  fi
  [[ -n "$VERSION_TAG" ]] || die "Failed to determine latest release tag. Set RKL_VERSION=vX.Y.Z and retry."
fi
log "Using version tag: $VERSION_TAG"

# 5) Prepare temp workspace
WORKDIR=$(mktemp -d 2>/dev/null || mktemp -d -t rkl)
trap 'rm -rf "$WORKDIR"' EXIT

ASSET_BASE="${BIN_NAME}-${VERSION_TAG}-${TARGET_TRIPLE}"
URL_BASE="https://github.com/${OWNER_REPO}/releases/download/${VERSION_TAG}"

# Prefer .zip, fall back to .tar.gz
ASSET_EXT="zip"
ASSET_URL="${URL_BASE}/${ASSET_BASE}.${ASSET_EXT}"
SHA_URL="${ASSET_URL}.sha256"

download_asset() {
  local asset_url=$1
  local output=$2
  curl -fL --retry 3 --retry-delay 1 -o "$output" "$asset_url"
}

log "Attempting to download zip asset..."
if ! download_asset "$ASSET_URL" "$WORKDIR/${ASSET_BASE}.zip" || ! download_asset "$SHA_URL" "$WORKDIR/${ASSET_BASE}.zip.sha256"; then
  log "Zip asset not found. Trying tar.gz..."
  ASSET_EXT="tar.gz"
  ASSET_URL="${URL_BASE}/${ASSET_BASE}.${ASSET_EXT}"
  SHA_URL="${ASSET_URL}.sha256"
  download_asset "$ASSET_URL" "$WORKDIR/${ASSET_BASE}.tar.gz" || die "Failed to download release asset: $ASSET_URL"
  download_asset "$SHA_URL" "$WORKDIR/${ASSET_BASE}.tar.gz.sha256" || die "Failed to download checksum: $SHA_URL"
fi

# 6) Verify checksum (normalize filename inside .sha256 if needed)
log "Verifying SHA256 checksum..."
if [[ "$ASSET_EXT" == "zip" ]]; then
  ASSET_FILE="$WORKDIR/${ASSET_BASE}.zip"
  SHA_FILE="$WORKDIR/${ASSET_BASE}.zip.sha256"
else
  ASSET_FILE="$WORKDIR/${ASSET_BASE}.tar.gz"
  SHA_FILE="$WORKDIR/${ASSET_BASE}.tar.gz.sha256"
fi

# Some releases may embed a path in the checksum file (e.g., 'dist/<file>'). Normalize to base name.
HASH=$(cut -d ' ' -f1 "$SHA_FILE" | tr -d '\n')
echo "$HASH  $(basename "$ASSET_FILE")" > "$SHA_FILE"
(cd "$WORKDIR" && shasum -a 256 -c "$(basename "$SHA_FILE")")

# 7) Unpack and locate binary
UNPACK_DIR="$WORKDIR/unpack"
mkdir -p "$UNPACK_DIR"
log "Unpacking..."
if [[ "$ASSET_EXT" == "zip" ]]; then
  [[ "$HAS_UNZIP" -eq 1 ]] || die "unzip not found; cannot extract zip."
  unzip -q "$ASSET_FILE" -d "$UNPACK_DIR"
else
  [[ "$HAS_TAR" -eq 1 ]] || die "tar not found; cannot extract tar.gz."
  tar -xzf "$ASSET_FILE" -C "$UNPACK_DIR"
fi

# Find the binary (handle possible parent dir in archive)
BIN_PATH_FOUND=""
while IFS= read -r -d '' f; do
  if [[ -f "$f" && -x "$f" ]]; then
    # Match exact name
    if [[ "$(basename "$f")" == "$BIN_NAME" ]]; then
      BIN_PATH_FOUND="$f"
      break
    fi
  fi
done < <(find "$UNPACK_DIR" -type f -name "$BIN_NAME" -print0)

[[ -n "$BIN_PATH_FOUND" ]] || die "Failed to locate '$BIN_NAME' in the extracted archive."

# 8) Install
log "Installing to: $INSTALL_DIR"
install -m 0755 "$BIN_PATH_FOUND" "$INSTALL_DIR/$BIN_NAME"

# 9) PATH hint
case ":$PATH:" in
  *":$INSTALL_DIR:"*) IN_PATH=1 ;;
  *) IN_PATH=0 ;;
esac

log "Installed: $INSTALL_DIR/$BIN_NAME"
if [[ $IN_PATH -eq 1 ]]; then
  log "You can now run: $BIN_NAME --help"
else
  log "Note: $INSTALL_DIR is not in your PATH. Add the following line to your shell profile:"
  if [[ -n "${ZSH_VERSION:-}" ]]; then SHELL_RC="$HOME/.zshrc"; else SHELL_RC="$HOME/.bashrc"; fi
  echo "    export PATH=\"$INSTALL_DIR:\$PATH\"" >&2
  log "Then restart your terminal or run: source $SHELL_RC"
fi

log "Done."
