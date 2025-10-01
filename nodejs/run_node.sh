#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   ./run_node.sh --input <dir> --out <basename> [options]
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
if [[ ! -d node_modules ]]; then
  echo "[npm] installing dependencies..."
  npm install --silent
fi
exec node index.mjs "$@"
