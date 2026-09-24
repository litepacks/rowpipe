#!/usr/bin/env bash
set -e

# Rowpipe Universal One-Line Installer
REPO="litepacks/rowpipe"
INSTALL_DIR="/usr/local/bin"

if [ "$EUID" -ne 0 ] && [ ! -w "$INSTALL_DIR" ]; then
  INSTALL_DIR="$HOME/.local/bin"
  mkdir -p "$INSTALL_DIR"
fi

echo "==> Detecting operating system and architecture..."
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
  darwin)
    PLATFORM_OS="darwin"
    ;;
  linux)
    PLATFORM_OS="linux"
    ;;
  *)
    echo "Error: Unsupported operating system '$OS'. Please download binaries from GitHub Releases."
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64|amd64)
    PLATFORM_ARCH="x64"
    ;;
  arm64|aarch64)
    PLATFORM_ARCH="arm64"
    ;;
  *)
    echo "Error: Unsupported architecture '$ARCH'."
    exit 1
    ;;
esac

TARGET="${PLATFORM_OS}-${PLATFORM_ARCH}"
echo "==> Identified target: $TARGET"

# Get latest release tag
LATEST_TAG=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' || echo "")
if [ -z "$LATEST_TAG" ]; then
  LATEST_TAG="v2.10.2"
fi

TARBALL="rowpipe-${TARGET}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${LATEST_TAG}/${TARBALL}"

echo "==> Downloading Rowpipe ($LATEST_TAG) from $URL..."
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

if curl -sSL --fail "$URL" -o "$TMP_DIR/$TARBALL"; then
  tar -xzf "$TMP_DIR/$TARBALL" -C "$TMP_DIR"
  
  if [ -w "$INSTALL_DIR" ]; then
    cp "$TMP_DIR/rowpipe" "$INSTALL_DIR/rowpipe"
    chmod 0755 "$INSTALL_DIR/rowpipe"
  else
    echo "==> Elevating privileges with sudo to install into $INSTALL_DIR..."
    sudo cp "$TMP_DIR/rowpipe" "$INSTALL_DIR/rowpipe"
    sudo chmod 0755 "$INSTALL_DIR/rowpipe"
  fi

  echo "==> Successfully installed rowpipe into $INSTALL_DIR/rowpipe"
  "$INSTALL_DIR/rowpipe" --version
else
  echo "Error: Failed to download release tarball. Please check https://github.com/${REPO}/releases"
  exit 1
fi
