#!/usr/bin/env bash
# Matched Linear Leios vote study. See docs/vote-diffusion-study.md.
set -euo pipefail
exec python3 "$(dirname "$0")/vote-diffusion-study.py" "$@"
