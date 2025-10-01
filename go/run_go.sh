#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   ./run_go.sh -input <dir> -out <basename> [options]
# If binary not built yet, this will build it.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
BIN="./mergezip_go"
if [[ ! -x "$BIN" ]]; then
  echo "[build] go build -o mergezip_go main.go"
  go build -o mergezip_go ./main.go
fi
exec "$BIN" "$@"
