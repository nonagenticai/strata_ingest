#!/usr/bin/env bash
# Test entry point for CI and for local runs.
#
# The suite needs real `ffmpeg` + `ffprobe` (audio probe/segment tests and every test that drives
# Convert's audio branch). On Linux the static binaries are copied out of the public image
# (`mwader/static-ffmpeg`, pulled from Docker Hub) through the docker daemon (`docker create` +
# `docker cp`, no container is run) into a private temp dir that is put first on PATH.
#
# macOS: `docker cp` would yield Linux binaries, so the system ffmpeg/ffprobe (Homebrew) is
# used and must be on PATH. Never skips: missing ffmpeg fails the run loudly.
#
# Extra args are passed to `cargo test --locked` (e.g. `./scripts/ci-test.sh --test enrich`).
set -euo pipefail

GUARD="${CARGO_GUARD:-cargo}"
FFMPEG_IMAGE="${FFMPEG_IMAGE:-mwader/static-ffmpeg:7.1.1}"

if [ "$(uname -s)" = "Linux" ]; then
  bin_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/strata-ingest-ffmpeg.XXXXXX")"
  cid=""
  cleanup() {
    if [ -n "${cid}" ]; then docker rm "${cid}" >/dev/null 2>&1 || true; fi
    rm -rf "${bin_dir}"
  }
  trap cleanup EXIT
  if ! docker image inspect "${FFMPEG_IMAGE}" >/dev/null 2>&1; then
    docker pull -q "${FFMPEG_IMAGE}" >/dev/null
  fi
  cid="$(docker create "${FFMPEG_IMAGE}")"
  docker cp "${cid}:/ffmpeg" "${bin_dir}/ffmpeg"
  docker cp "${cid}:/ffprobe" "${bin_dir}/ffprobe"
  docker rm "${cid}" >/dev/null
  cid=""
  chmod 0755 "${bin_dir}/ffmpeg" "${bin_dir}/ffprobe"
  export PATH="${bin_dir}:${PATH}"
fi

for tool in ffmpeg ffprobe; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "::error::${tool} not found on PATH (macOS: brew install ffmpeg)" >&2
    exit 1
  fi
done
echo "using $(command -v ffmpeg): $(ffmpeg -version | sed -n 1p)"

"$GUARD" test --locked "$@"
