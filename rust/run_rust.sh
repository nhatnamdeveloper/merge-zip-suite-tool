#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   ./run_rust.sh --input <dir> --out <basename> [options]
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
echo "[cargo] building release..."
cargo build --release
BIN="$DIR/target/release/mergezip_rs"
exec "$BIN" "$@"
