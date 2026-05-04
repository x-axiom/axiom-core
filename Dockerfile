# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: builder
# ─────────────────────────────────────────────────────────────────────────────
FROM rust:1.82-slim AS builder

# RocksDB requires cmake + clang + libz + libsnappy.
# rusqlite uses the bundled feature (no system sqlite needed).
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        libclang-dev \
        libz-dev \
        libsnappy-dev \
        liblz4-dev \
        libzstd-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ── Dependency layer cache ────────────────────────────────────────────────────
COPY Cargo.toml Cargo.toml
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release --features local 2>/dev/null; rm -rf src

# ── Full source build ─────────────────────────────────────────────────────────
COPY src/ src/
RUN touch src/main.rs && cargo build --release --features local

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: runtime
# ─────────────────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# RocksDB links dynamically against these system libraries.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgcc-s1 \
        libsnappy1v5 \
        liblz4-1 \
        libzstd1 \
        libz1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user.
RUN groupadd -r axiom && useradd -r -g axiom -s /sbin/nologin axiom

# Copy the compiled binary.
COPY --from=builder /build/target/release/axiom-core /usr/local/bin/axiom-core

WORKDIR /app
RUN chown axiom:axiom /app

USER axiom

# REST API
EXPOSE 3000
# gRPC sync
EXPOSE 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:3000/health || exit 1

ENTRYPOINT ["/usr/local/bin/axiom-core"]
