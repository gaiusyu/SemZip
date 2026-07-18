#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
exec /root/miniconda3/bin/python run_semzip_widthguard_or20_ipv4plain.py "$@"
