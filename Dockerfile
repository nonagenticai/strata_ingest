# strata-ingest, CPU-only Linux image: the API + worker pool. Conversion runs in the docling-serve
# CPU sidecar and ffmpeg (in this image); OCR, captions and transcription are optional HTTP
# providers from providers.toml.
#
# Base images come ONLY from the internal mirror. Same shape as strata_rag: the mirror has no glibc
# Rust image, so the builder is the mirrored debian:12-slim with a pinned toolchain from rustup,
# and builder + runtime share one glibc (aws-lc-sys is a native cmake build).
ARG RUST_TOOLCHAIN=1.99.1

FROM debian:12-slim AS build
ARG RUST_TOOLCHAIN
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake curl perl pkg-config \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal --default-toolchain "${RUST_TOOLCHAIN}" \
    && rustc --version
WORKDIR /src
COPY . .
# sqlx::migrate! embeds migrations/ into the binary, so the runtime image needs no SQL files.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
    && install -D -m 0755 target/release/strata-ingest /out/strata-ingest

FROM debian:12-slim AS runtime
# ffmpeg (5.1, with ffprobe): audio/video probing and segmenting; it is run with
# -protocol_whitelist / -format_whitelist. ca-certificates: https providers. curl: the HEALTHCHECK.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 strata-ingest \
    && useradd --system --uid 65532 --gid 65532 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin strata-ingest
COPY --from=build /out/strata-ingest /usr/local/bin/strata-ingest
# The pod runs with readOnlyRootFilesystem and an emptyDir at /tmp: scratch files (ffmpeg
# segments, docling uploads) go only to $TMPDIR. providers.toml is mounted at
# /etc/strata-ingest/providers.toml (STRATA_INGEST_PROVIDERS); none ships in the image, because
# the deployment region is a deployment decision. Startup fails naming any missing setting.
ENV STRATA_INGEST_BIND=0.0.0.0:9135 \
    TMPDIR=/tmp \
    RUST_LOG=strata_ingest=info
WORKDIR /tmp
USER 65532:65532
EXPOSE 9135
# /health answers 200 with a JSON component report even when a dependency is down, so this is a
# liveness check of the HTTP server; read the body for db/storage/docling/strata status.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fs http://127.0.0.1:9135/health -o /dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/strata-ingest"]
CMD ["serve"]
