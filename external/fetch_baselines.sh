#!/bin/bash
# Fetch the third-party baselines at the pinned upstream commits used in the paper.
# No third-party source is redistributed in this repository; this script only clones/downloads it
# into external/upstream/ (git-ignored) and checks out the recorded commit.
#
# usage:  bash external/fetch_baselines.sh [delog] [loglite] [logshrink] [logreducer] [denum] [zstd]
#         (no argument = all).  The repository URLs are read from external/upstreams.conf (set them to the
#         repositories cited in the paper); an exported environment variable of the same name takes precedence.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; UP="$ROOT/external/upstream"; mkdir -p "$UP"
CONF="$ROOT/external/upstreams.conf"
if [ -f "$CONF" ]; then
  while IFS='=' read -r key value; do
    case "$key" in ''|\#*) continue ;; esac
    value="${value%\"}"; value="${value#\"}"
    if [ -z "${!key:-}" ]; then export "$key=$value"; fi
  done < "$CONF"
fi

pin() {  # name url commit
  local name="$1" url="$2" commit="$3"
  if [ -z "$url" ]; then echo "skip $name: repository URL not set (external/upstreams.conf)" >&2; return 0; fi
  [ -d "$UP/$name/.git" ] || git clone --quiet --no-checkout "$url" "$UP/$name"
  git -C "$UP/$name" fetch --quiet origin "$commit" 2>/dev/null || true
  git -C "$UP/$name" -c advice.detachedHead=false checkout --quiet "$commit"
  echo "$name $(git -C "$UP/$name" rev-parse HEAD)"
}

want() { [ $# -eq 0 ] && return 0; for a in "${ARGS[@]}"; do [ "$a" = "$1" ] && return 0; done; return 1; }
ARGS=("$@")
sel() { if [ ${#ARGS[@]} -eq 0 ]; then return 0; fi; want "$1"; }

sel delog      && pin DeLog      "${DELOG_REPO_URL:-}"      64a074f6b6559fbfcd809f201fc3540442151749
sel loglite    && pin LogLite    "${LOGLITE_REPO_URL:-}"    68f851ef673ac6fa45f26513df08613151624bd2
sel logshrink  && pin LogShrink  "${LOGSHRINK_REPO_URL:-}"  59ce49434eec06c709c7e16027a46214a6c37961
sel logreducer && pin LogReducer "${LOGREDUCER_REPO_URL:-}" 40005419022c02454eca7b027d76b99b3dfad543
sel denum      && pin Denum      "${DENUM_REPO_URL:-}"      a3a697564e378643461e27ab92ea44b098424c35

if sel zstd; then
  f="$UP/zstd-1.5.6.tar.gz"
  [ -f "$f" ] || curl -fsSL -o "$f" https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz
  echo "8c29e06cf42aacc1eafc4077ae2ec6c6fcb96a626157e0593d5e82a34fd403c1  $f" | sha256sum -c -
  [ -d "$UP/zstd-1.5.6" ] || tar -xzf "$f" -C "$UP"
  echo "build: make -C $UP/zstd-1.5.6  (binary: $UP/zstd-1.5.6/programs/zstd)"
fi

cat <<'EOF'
Next steps (see external/SETUP.md and evidence/r76/*/README.md):
  DeLog       build per external/SETUP.md; DeLog-generic = the same source with an empty regex_map (evidence/r76/codecs/README.md)
  LogLite-BL  apply external/loglite_wide_reserve.patch to the LogLite-B subtree, then build (external/SETUP.md)
  LogShrink   make in python_compression/parser; adapter: evidence/r76/baselines/logshrink/adapter/
  LogReducer  make in the repository root; driver: evidence/r76/baselines/logreducer/lr_run2.py
  Denum       g++ build command in evidence/r76/baselines/denum/README.md; adapter codec_baseline_denum*.py
EOF
