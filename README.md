# Axiom Core

[简体中文](README.zh-CN.md)

Axiom Core is a high-performance, versioned, content-addressed storage engine written in Rust.

It brings Git-like ideas to large binary assets: immutable versions, content-based deduplication, branch and tag references, and Merkle-tree diffing. It can run as an HTTP service or be embedded directly in other applications.

## Table of Contents

- [Why Axiom Core](#why-axiom-core)
- [Key Capabilities](#key-capabilities)
- [Architecture](#architecture)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Feature Flags](#feature-flags)
- [HTTP API Overview](#http-api-overview)
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
| Multi-tenant SaaS path | Yes, with `fdb` | No | No | Partial |

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

### Distributed and SaaS Extensions

- gRPC sync support behind the `cloud` feature
- Multi-tenant and FoundationDB-backed flows behind the `fdb` feature
- JWT authentication, RBAC, remote refs, and sync session support
- Garbage collection, retention handling, and lifecycle management for advanced deployments

## Architecture

```text
HTTP API (axum)
    |
Service Layer
    |-- commit.rs
    |-- diff_engine.rs
    |-- namespace.rs
    |-- checkout.rs
    |-- working_tree.rs
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
    |-- In-memory store for tests
```

All storage access goes through traits defined in `src/store/traits.rs`. Service and API code should depend on those traits rather than concrete backend types. `AppState` wraps the selected backends and exposes them through shared trait objects.

## Repository Layout

```text
src/
  api/          axum router, request handlers, DTOs, error mapping
  auth/         JWT, RBAC, middleware
  gc/           recycle bin, retention, garbage collection, scheduler
  model/        domain types such as ChunkHash, VersionNode, Ref
  store/        storage traits and backend implementations
  sync/         sync protocol support, remote refs, session state
  tenant/       multi-tenant support for FoundationDB-backed flows
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
| FoundationDB client | `fdb` feature | Required only for FoundationDB-backed workflows |

### Build

```bash
# Default single-node build
cargo build

# Local + cloud features
cargo build --features full

# FoundationDB-backed extensions
cargo build --features fdb
```

### Run the Server

```bash
# Start with default local storage under .axiom/
cargo run --release

# Customize the data directory and listen address
cargo run --release -- --data-dir .axiom-dev --listen 127.0.0.1:3000

# Show all CLI options
cargo run --release -- --help
```

By default, the server listens on `0.0.0.0:3000` and stores local data under `.axiom/`.

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
| `cloud` | No | gRPC sync and cloud-oriented integrations |
| `fdb` | No | FoundationDB-backed tenant, auth, GC, and observability features |
| `full` | No | Convenience flag for `local` + `cloud` |

## HTTP API Overview

All `{ref}` parameters accept a version ID, branch name, or tag name.

| Area | Representative endpoints |
| --- | --- |
| Health | `GET /health` |
| Upload | `POST /api/v1/upload/file`, `POST /api/v1/upload/directory` |
| Browse | `GET /api/v1/version/{ref}/ls`, `GET /api/v1/version/{ref}/file/{path}` |
| History | `GET /api/v1/versions/{ref}`, `GET /api/v1/versions/{ref}/history` |
| Path metadata | `GET /api/v1/versions/{ref}/path/{path}` |
| Refs | `GET /api/v1/refs`, `POST /api/v1/refs`, `PUT /api/v1/refs/{name}` |
| Diff | `POST /api/v1/diff` |
| Objects | `GET /api/v1/objects/{hash}` |

Route implementations live under `src/api/routes/`.

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

## Roadmap

1. Merge commit support
2. Dedicated CLI tooling
3. End-to-end gRPC sync flows
4. Web management console
5. Billing and quota enforcement

## License

Licensed under the [Apache License 2.0](LICENSE).
