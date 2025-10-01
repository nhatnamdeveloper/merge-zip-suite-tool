#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   ./run_python.sh <input_dir> <output_basename> [options]
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
exec python3 merge_zip.py "$@"
