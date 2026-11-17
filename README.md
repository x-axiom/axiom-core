# Axiom Core

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

[简体中文](README.zh-CN.md)

Axiom Core is a high-performance, versioned, content-addressed storage engine written in Rust.

It brings Git-like ideas to large binary assets: immutable versions, content-based deduplication, branch and tag references, and Merkle-tree diffing. It can run as an HTTP service or be embedded as a Rust library.

Axiom Core is designed as the engine for a **versioned, content-addressed object storage service** for large files such as videos, 3D models, datasets, container images, and design assets. It combines:

- **Git** version DAGs and immutable snapshot semantics
- **S3-compatible storage** for object-storage scale
- **IPFS/CAS** content addressing and chunk-level deduplication

**Long-term goal:** preserve complete change history while keeping storage costs close to raw object storage through content reuse and deduplication.

## Table of Contents

- [Why Axiom Core](#why-axiom-core)
- [Key Capabilities](#key-capabilities)
- [Architecture](#architecture)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Feature Flags](#feature-flags)
- [HTTP API Overview](#http-api-overview)
- [Authentication](#authentication)
- [Development](#development)
- [Contributing](#contributing)
- [Roadmap](#roadmap)
- [License](#license)

## Why Axiom Core

Axiom Core is designed for workloads where plain object storage is not enough and Git-style tooling does not scale well to large files.

- Content-addressed storage with BLAKE3 hashes
- Chunk-level deduplication for repeated content across versions
- Immutable version history with mutable branch and tag references
- Merkle-tree diffing for efficient change detection
- Swappable storage backends behind trait-based abstractions
- Support for both local single-node deployments and cloud-oriented extensions

### Comparison

| Capability | Axiom Core | Git LFS | DVC | Plain S3 |
| --- | --- | --- | --- | --- |
| Content-addressed storage | Yes, BLAKE3 | Partial | Partial | No |
| Built-in version graph | Yes | Via Git | Via Git | No |
| Server-side diff | Yes, Merkle-based | No | No | No |
| Chunk-level deduplication | Yes | No | No | No |
| Embeddable without HTTP | Yes | No | No | No |
| Multi-tenant cloud backend | Yes, with `cloud` + `fdb` | No | No | Partial |

## Key Capabilities

### Storage and Versioning

- FastCDC chunking for chunk-level deduplication
- RocksDB-backed content-addressed storage for chunks, trees, and nodes
- SQLite metadata storage for versions, refs, path indexes, and workspace metadata
- Immutable version nodes with branch and tag references
- BLAKE3 hashes used consistently across stored objects

### Developer Workflow

- Upload single files or whole directory snapshots
- Browse versions, inspect metadata, and download file contents
- Diff two versions, branches, or tags
- Materialize a version into a working directory with checkout support
- Compute working tree status against the current HEAD

### Cloud and Service Features

- gRPC push, pull, and clone flows behind the `cloud` feature
- Optional cloud storage backend using S3-compatible object storage and FoundationDB metadata; it requires both `cloud` and `fdb`
- Optional Redis/DragonflyDB cache for the cloud backend
- Workspace and member APIs, JWT authentication, optional GitHub OAuth, RBAC, remote refs, and sync-session support
- Usage and quota tracking, audit logging, recycle-bin retention, and garbage-collection support; availability depends on the selected backend and configuration

## Architecture

```text
HTTP API (axum)
    |
Service Layer
    |-- versioning, diff, checkout, working tree
    |-- workspaces, auth, quota, audit
    |-- sync/
    |-- gc/
    |
Store Traits (src/store/traits.rs)
    |-- ChunkStore
    |-- TreeStore
    |-- NodeStore
    |-- VersionRepo
    |-- RefRepo
    |-- PathIndexRepo
    |-- WorkspaceRepo
    |-- SyncStore
    |
Implementations
    |-- RocksDB CAS store
    |-- SQLite metadata store
    |-- S3-compatible CAS + FoundationDB metadata (cloud)
    |-- Optional Redis/DragonflyDB cache
    |-- In-memory store for tests
```

All storage access goes through traits defined in `src/store/traits.rs`. Service and API code should depend on those traits rather than concrete backend types. `AppState` wraps the selected backends and exposes them through shared trait objects.

## Repository Layout

```text
src/
  api/          axum router, request handlers, DTOs, error mapping
  auth/         JWT, RBAC, middleware
  audit/        security-relevant audit events and repositories
  gc/           recycle bin, retention, garbage collection, scheduler
  model/        domain types such as ChunkHash, VersionNode, Ref
  quota/        usage accounting and quota policy
  store/        storage traits and backend implementations
  sync/         sync protocol support, remote refs, session state
  tenant/       multi-tenant support for FoundationDB-backed flows
  telemetry/    structured logging and Prometheus metrics
  checkout.rs   materialize stored data onto the local filesystem
  chunker.rs    FastCDC content-defined chunking
  commit.rs     version creation, refs, history
  diff_engine.rs
  merkle.rs     Merkle tree build and traversal
  namespace.rs  directory tree construction
  working_tree.rs
tests/          integration tests
benches/        Criterion benchmarks
proto/          gRPC definitions for sync
```

## Getting Started

### Prerequisites

| Dependency | Required for | Notes |
| --- | --- | --- |
| Rust stable | All builds | Install from [rustup.rs](https://rustup.rs) |
| RocksDB | `local` feature | `brew install rocksdb` or your distro equivalent |
| SQLite CLI | Optional | Useful for inspecting metadata manually; not required for building |
| `protoc` | `cloud` feature | `brew install protobuf` or your distro equivalent |
| FoundationDB client | `fdb` feature | Required for the FoundationDB-backed cloud storage mode |
| S3-compatible bucket and credentials | Cloud storage mode | AWS S3 or a compatible service such as MinIO |
| Redis/DragonflyDB | Optional cloud cache | Configure with `AXIOM_CACHE_URL` |

### Build

```bash
# Default single-node build
cargo build

# Local + cloud features
cargo build --features full

# S3 + FoundationDB cloud storage (also enables local and gRPC features)
cargo build --features "full,fdb"
```

The `full` feature is `local` + `cloud`; it does not enable `fdb`.

### Run the Server

```bash
# Start with default local storage under .axiom/
cargo run --release

# Customize the data directory and listen address
cargo run --release -- --data-dir .axiom-dev --listen 127.0.0.1:3000

# Start the cloud backend (requires `--features "full,fdb"` and a config file)
cargo run --release --features "full,fdb" -- --mode cloud --config axiom.toml

# Show all CLI options
cargo run --release -- --help
```

By default, the server listens on `0.0.0.0:3000` and stores local data under `.axiom/`.

Cloud mode reads its S3 and FoundationDB settings from the TOML file or environment variables. A minimal `axiom.toml` is:

```toml
[cloud.s3]
bucket = "my-bucket"
region = "us-east-1"

[cloud.fdb]
tenant = "my-tenant"
workspace = "my-workspace"
```

### Upload Data

```bash
# Upload a single file
curl -X POST 'http://localhost:3000/api/v1/upload/file?path=hello.txt&message=first+commit' \
  --data-binary 'Hello, Axiom!'

# Upload a directory snapshot
curl -X POST http://localhost:3000/api/v1/upload/directory \
  -H 'Content-Type: application/json' \
  -d '{
    "files": [
      {"path": "src/main.rs", "content_base64": "Zm4gbWFpbigpIHt9"},
      {"path": "README.md", "content_base64": "IyBIZWxsbw=="}
    ],
    "message": "initial commit"
  }'
```

### Browse and Diff Versions

```bash
# List the root directory of the current main ref
curl http://localhost:3000/api/v1/version/main/ls

# Download a file from a stored version
curl http://localhost:3000/api/v1/version/main/file/hello.txt

# Inspect version history
curl http://localhost:3000/api/v1/versions/main/history

# Diff two refs
curl -X POST http://localhost:3000/api/v1/diff \
  -H 'Content-Type: application/json' \
  -d '{"old_version": "v1.0", "new_version": "main"}'
```

## Feature Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `local` | Yes | RocksDB CAS plus SQLite metadata for local and embedded usage |
| `cloud` | No | gRPC sync flows and dependencies for cloud integrations |
| `fdb` | No | FoundationDB metadata backend; cloud storage requires `cloud` + `fdb` |
| `full` | No | Convenience flag for `local` + `cloud`; it does not include `fdb` |

## HTTP API Overview

All `{ref}` parameters accept a version ID, branch name, or tag name. The complete OpenAPI 3.1 contract is available at `/openapi.json`; the built-in API page is at `/docs`.

| Area | Representative endpoints |
| --- | --- |
| Health and metrics | `GET /health`, `/healthz`, `/live`, `/ready`, `/metrics` |
| API documentation | `GET /docs`, `/openapi.json` |
| Authentication | `POST /auth/login`, `/auth/refresh`, `/auth/logout`; `GET /auth/session`, `/auth/jwks` |
| Upload | `POST /api/v1/upload/file`, `POST /api/v1/upload/directory` |
| Browse | `GET /api/v1/version/{ref}/ls`, `GET /api/v1/version/{ref}/file/{path}` |
| History | `GET /api/v1/versions/{ref}`, `GET /api/v1/versions/{ref}/history` |
| Path metadata | `GET /api/v1/versions/{ref}/path/{path}` |
| Workspaces and members | `GET/POST /api/v1/workspaces`, `GET /api/v1/workspaces/{workspace_id}/tree`, workspace member routes |
| Refs | `GET/POST /api/v1/refs`, `GET/PUT/DELETE /api/v1/refs/{name}`, `GET /api/v1/refs/{name}/resolve` |
| Diff | `POST /api/v1/diff` |
| Usage and audit | `GET /api/v1/usage`, `GET /api/v1/audit` |
| Objects | `GET /api/v1/objects/{hash}` |

Route implementations live under `src/api/routes/`. Protected API routes require the configured identity when authentication is enabled.

## Authentication

Local mode leaves HTTP authentication disabled by default. Set `AXIOM_AUTH_ENABLED=1` to attach the login and token service. When enabled, local mode defaults to JWT bearer-token verification; cloud mode defaults to trusted-gateway identity headers. `AXIOM_AUTH_MODE` can override this with `jwt`, `trusted-gateway`, or `disabled`.

For stable JWT signing across restarts, configure persistent key files. If either key is omitted, the process generates an ephemeral key pair:

```bash
AXIOM_AUTH_ENABLED=1 \
AXIOM_JWT_PRIVATE_KEY=/path/to/private.pem \
AXIOM_JWT_PUBLIC_KEY=/path/to/public.pem \
cargo run --release
```

GitHub OAuth login additionally requires `AXIOM_GITHUB_CLIENT_ID`, `AXIOM_GITHUB_CLIENT_SECRET`, and `AXIOM_GITHUB_REDIRECT_URI`. `AXIOM_AUTH_DEV_SECRET` enables development-only login and must not be used in production.

## Development

### Tests

```bash
# Run all default-feature tests
cargo test

# Run cloud-related tests
cargo test --features full

# Run a specific integration test file
cargo test --test commit_tests

# Run one test by name
cargo test version_history_is_paginated
```

### Benchmarks

```bash
cargo bench --bench poc_benchmark
cargo bench --bench cas-benchmark
cargo bench --bench working_tree_bench
```

### Project Conventions

- Keep storage access behind the traits in `src/store/traits.rs`
- Use `AppState::memory()` for fully in-memory tests
- Keep integration tests in `tests/` instead of `src/`
- Return `CasResult<T>` for fallible storage and service operations
- Treat `ChunkHash` and `VersionId` as the canonical object identifiers

## Contributing

Issues and pull requests are welcome.

Before opening a pull request:

- Make sure the relevant tests pass
- Add or update tests for behavior changes
- Avoid bypassing storage traits in new code
- Add a SQLite migration when schema changes require it
- Keep documentation in sync with user-facing behavior

Contributions intentionally submitted for inclusion are licensed under Apache-2.0 in accordance with Section 5 of [LICENSE](LICENSE), unless explicitly stated otherwise. Submit only material you have the right to contribute.

## Roadmap

1. First-class merge and conflict-resolution workflows
2. Dedicated developer and operator CLI tooling
3. Web management console
4. Billing integration and plan management

## License

Axiom Core is licensed under the [Apache License, Version 2.0](LICENSE) (`Apache-2.0`). Third-party dependencies and assets retain their respective licenses; consult their upstream license and notice files.
