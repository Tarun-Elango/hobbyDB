#!/usr/bin/env bash
# Installs the kestrel server binary (kestrelKV) from a GitHub release (Linux or macOS).
#
#   ./install.sh            # latest release
#   ./install.sh v0.1.0     # specific release
#
# Installs:
#   /usr/local/bin/kestrel            the server binary (uses sudo only if needed)
#   /usr/local/bin/kestrel-uninstall  removes everything this script added
#   ~/.kestrel                        data dir for the WAL (/var/lib/kestrel when run as root)
#
# Override locations with BIN_DIR=... and DATA_DIR=... env vars.
# It does not start the server.
set -euo pipefail

REPO="Tarun-Elango/kestrelKV"
BIN_DIR="${BIN_DIR:-/usr/local/bin}"
if [[ "$(id -u)" -eq 0 ]]; then
  DATA_DIR="${DATA_DIR:-/var/lib/kestrel}"
else
  DATA_DIR="${DATA_DIR:-$HOME/.kestrel}"
fi

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

ASSET="kestrel-${OS}-${ARCH}.tar.gz"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading kestrel ${VERSION} (${OS}/${ARCH})..."
fetch -o "$TMP/$ASSET" "$BASE_URL/$ASSET"
fetch -o "$TMP/checksums.txt" "$BASE_URL/checksums.txt"

echo "Verifying checksum..."
expected="$(awk -v f="$ASSET" '$2 == f {print $1}' "$TMP/checksums.txt")"
[[ "$expected" =~ ^[0-9a-f]{64}$ ]] || die "no checksum for $ASSET in checksums.txt"
[[ "$(sha256 "$TMP/$ASSET")" == "$expected" ]] || die "checksum mismatch, aborting"

# Extract only the binary, ignoring ownership/permissions stored in the archive.
tar -xzf "$TMP/$ASSET" -C "$TMP" --no-same-owner --no-same-permissions kestrel
[[ -f "$TMP/kestrel" && ! -L "$TMP/kestrel" ]] || die "archive did not contain the kestrel binary"

$SUDO mkdir -p "$BIN_DIR"
$SUDO install -m 0755 "$TMP/kestrel" "$BIN_DIR/kestrel"
mkdir -p "$DATA_DIR"
chmod 0700 "$DATA_DIR"

cat > "$TMP/kestrel-uninstall" <<EOF
#!/usr/bin/env bash
# Removes what install.sh added. Keeps WAL data unless --purge is given.
#   kestrel-uninstall           # remove binary, keep $DATA_DIR
#   kestrel-uninstall --purge   # also delete $DATA_DIR
set -euo pipefail
if pgrep -x kestrel >/dev/null 2>&1; then
  echo "error: kestrel is still running, stop it first" >&2; exit 1
fi
SUDO=""
[[ -w "$BIN_DIR" ]] || SUDO="sudo"
# Deleting the data dir needs write access to its parent (e.g. /var/lib after a root install).
DATA_SUDO=""
[[ -w "$(dirname "$DATA_DIR")" ]] || DATA_SUDO="sudo"
if [[ "\${1:-}" == "--purge" ]]; then
  \$DATA_SUDO rm -rf "$DATA_DIR"
  echo "deleted $DATA_DIR"
else
  echo "WAL data kept in $DATA_DIR (use --purge to delete it)"
fi
\$SUDO rm -f "$BIN_DIR/kestrel" "$BIN_DIR/kestrel-uninstall"
echo "kestrel removed"
EOF
$SUDO install -m 0755 "$TMP/kestrel-uninstall" "$BIN_DIR/kestrel-uninstall"

cat <<EOF

kestrel ${VERSION} installed to ${BIN_DIR}/kestrel
WAL directory: ${DATA_DIR}

Start it with your flags, for example:
  kestrel --node-id=node-a --wal=${DATA_DIR}/node-a.wal \\
    --client-addr=:9000 --replication-addr=:9001 --peers=...

Uninstall:  kestrel-uninstall          (keeps WAL data)
            kestrel-uninstall --purge  (deletes WAL data too)
EOF
