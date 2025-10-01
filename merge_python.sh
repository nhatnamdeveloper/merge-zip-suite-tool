#!/usr/bin/env bash
# Quick wrapper to run the Python ZIP merger.
# Usage:
#   ./merge_python.sh <input_dir> <output_basename> [extra options]
# Example:
#   ./merge_python.sh /Users/agecodepc20/Downloads/Raw merged_python
#
# Notes:
# - Keeps original root paths inside ZIP (no extra nesting).
# - Shows per-zip & overall % with Elapsed/ETA.
# - Extra options are forwarded to python/merge_zip.py.

set -euo pipefail

INPUT_DIR="${1:-/Users/agecodepc20/Downloads/Raw}"
OUTPUT_BASENAME="${2:-merged_python}"

# Forward any extra options after the first 2 args
if (( $# >= 2 )); then
  shift 2
else
  shift $#
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/python/run_python.sh" "${INPUT_DIR}" "${OUTPUT_BASENAME}" "$@"
