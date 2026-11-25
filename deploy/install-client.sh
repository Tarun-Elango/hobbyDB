#!/usr/bin/env bash
# Installs the kestrel client (kestrel-cli) from a GitHub release (Linux or macOS).
#
#   ./install-client.sh            # latest release
#   ./install-client.sh v0.1.0     # specific release
#
# Installs:
#   /usr/local/bin/kestrel-cli            the client binary (uses sudo only if needed)
#   /usr/local/bin/kestrel-cli-uninstall  removes everything this script added
#
# Override the location with the BIN_DIR=... env var.
# The server is installed separately with install-server.sh.
set -euo pipefail

REPO="Tarun-Elango/kestrelKV"
BIN_DIR="${BIN_DIR:-/usr/local/bin}"

die() { echo "error: $*" >&2; exit 1; }

case "$(uname -s)" in
  Linux) OS="linux" ;;
  Darwin) OS="darwin" ;;
  *) die "unsupported OS: $(uname -s)" ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) ARCH="amd64" ;;
  aarch64 | arm64) ARCH="arm64" ;;
  *) die "unsupported CPU: $(uname -m)" ;;
esac

for cmd in curl tar; do
  command -v "$cmd" >/dev/null || die "missing required command: $cmd"
done

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null; then shasum -a 256 "$1" | awk '{print $1}'
  else die "need sha256sum or shasum"
  fi
}

# Use sudo only when the bin dir isn't writable by the current user.
SUDO=""
if [[ ! -w "$BIN_DIR" ]]; then
  command -v sudo >/dev/null || die "$BIN_DIR is not writable and sudo is not available"
  SUDO="sudo"
fi

# HTTPS only (also on redirects), modern TLS, fail on HTTP errors.
fetch() { curl --proto '=https' --tlsv1.2 -fsSL --retry 3 "$@"; }

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  # /releases/latest redirects to /releases/tag/<version>
  latest_url="$(fetch -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest")"
  VERSION="${latest_url##*/}"
fi
[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || die "invalid version: '$VERSION'"

ASSET="kestrel-cli-${OS}-${ARCH}.tar.gz"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading kestrel-cli ${VERSION} (${OS}/${ARCH})..."
fetch -o "$TMP/$ASSET" "$BASE_URL/$ASSET" \
  || die "could not download $ASSET for ${VERSION} (releases before the client was published don't have it)"
fetch -o "$TMP/checksums.txt" "$BASE_URL/checksums.txt"

echo "Verifying checksum..."
expected="$(awk -v f="$ASSET" '$2 == f {print $1}' "$TMP/checksums.txt")"
[[ "$expected" =~ ^[0-9a-f]{64}$ ]] || die "no checksum for $ASSET in checksums.txt"
[[ "$(sha256 "$TMP/$ASSET")" == "$expected" ]] || die "checksum mismatch, aborting"

# Extract only the binary, ignoring ownership/permissions stored in the archive.
tar -xzf "$TMP/$ASSET" -C "$TMP" --no-same-owner --no-same-permissions kestrel-cli
[[ -f "$TMP/kestrel-cli" && ! -L "$TMP/kestrel-cli" ]] || die "archive did not contain the kestrel-cli binary"

$SUDO mkdir -p "$BIN_DIR"
$SUDO install -m 0755 "$TMP/kestrel-cli" "$BIN_DIR/kestrel-cli"

cat > "$TMP/kestrel-cli-uninstall" <<EOF
#!/usr/bin/env bash
# Removes what install-client.sh added.
set -euo pipefail
SUDO=""
[[ -w "$BIN_DIR" ]] || SUDO="sudo"
\$SUDO rm -f "$BIN_DIR/kestrel-cli" "$BIN_DIR/kestrel-cli-uninstall"
echo "kestrel-cli removed"
EOF
$SUDO install -m 0755 "$TMP/kestrel-cli-uninstall" "$BIN_DIR/kestrel-cli-uninstall"

cat <<EOF

kestrel-cli ${VERSION} installed to ${BIN_DIR}/kestrel-cli

Connect to a cluster (defaults to localhost:9000):
  kestrel-cli --9000 --9010 --9020
  kestrel-cli --host-a:9000,host-b:9000,host-c:9000

Uninstall:  kestrel-cli-uninstall
EOF
