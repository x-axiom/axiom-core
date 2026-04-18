# Axiom

Axiom is a next-generation high-performance, versioned, content-addressed large-object storage engine.

# Axiom-core

This repository contains the core Rust prototype for Axiom — a versioned, content-addressed large-object storage engine. The v0.1 POC is complete, providing a fully functional single-node system with HTTP API, persistent storage, content-defined chunking, Merkle trees, version history, branching, tagging, diff, and streaming upload/download.

## Current Status — v0.1 POC Complete

The project provides:

- **BLAKE3 content addressing** — all chunks, tree nodes, and directory nodes are hash-addressed
- **Domain model** — chunk, tree, node, version, ref, and diff primitives
- **RocksDB CAS** (AXIOM-102) — persistent chunk, tree node, and directory node storage with column families, idempotent writes, and crash recovery
- **SQLite metadata** (AXIOM-103) — version, ref, and path index persistence with WAL mode and migration support
- **FastCDC chunking** (AXIOM-104) — content-defined chunking (16KB–256KB), deduplication, and reassembly
- **Merkle tree** (AXIOM-105) — multi-level tree with configurable fan-out (default 64), build and rehydrate operations
- **Directory namespace** (AXIOM-106) — `build_directory_tree()`, deterministic directory hashing, path resolution, SQLite path index
- **Commit service** (AXIOM-107) — version creation, branch/tag CRUD, history traversal, ref resolution
- **Diff engine** (AXIOM-108) — directory-level and chunk-level diff with Merkle hash short-circuit for unchanged subtrees
- **HTTP API** (AXIOM-109) — axum-based REST API with health, objects, versions, refs, diff, upload, and download routes
- **Streaming upload** (AXIOM-110) — single-file (raw body) and multi-file (JSON+base64) upload with dedup statistics
- **Streaming download** (AXIOM-111) — file download via Merkle rehydration and directory listing
- **Query APIs** (AXIOM-112) — paginated version history, ref-aware version/diff resolution, node metadata endpoint
- **E2E validation** (AXIOM-113) — 10 end-to-end tests, 6 benchmark scenarios, demo script, and known limitations documentation
- **Immutable tags**, branch management, and ref-based navigation across all read endpoints
- **158+ automated tests** covering all layers from domain model to HTTP API integration

## Quick Start

Requirements:

- Rust stable toolchain
- Cargo

### Start the Server

```bash
cargo run --release
```

The server listens on `http://localhost:3000`.

### Upload Files

```bash
# Single file
curl -X POST 'http://localhost:3000/api/v1/upload/file?path=hello.txt&message=first+commit' \
  --data-binary 'Hello, Axiom!'

# Directory (multiple files)
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

### Browse and Download

```bash
# List directory contents
curl http://localhost:3000/api/v1/version/main/ls

# Download a file
curl http://localhost:3000/api/v1/version/main/file/hello.txt

# View version history
curl http://localhost:3000/api/v1/versions/main/history
```

### Diff Versions

```bash
curl -X POST http://localhost:3000/api/v1/diff \
  -H 'Content-Type: application/json' \
  -d '{"old_version": "v1.0", "new_version": "main"}'
```

### Run Tests

```bash
cargo test
```

### Run Benchmarks

```bash
cargo bench --bench poc_benchmark
```

For the full demo walkthrough, see [docs/demo.md](docs/demo.md).

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/upload/file` | Single-file upload (raw body) |
| POST | `/api/v1/upload/directory` | Multi-file upload (JSON) |
| GET | `/api/v1/version/{ref}/file/{path}` | Download file content |
| GET | `/api/v1/version/{ref}/ls` | List root directory |
| GET | `/api/v1/version/{ref}/ls/{path}` | List subdirectory |
| GET | `/api/v1/versions/{ref}` | Get version by ID, branch, or tag |
| GET | `/api/v1/versions/{ref}/history` | Paginated version history |
| GET | `/api/v1/versions/{ref}/path/{path}` | Node metadata (hash, size, type) |
| POST | `/api/v1/versions` | Create version record |
| GET | `/api/v1/refs` | List refs (filterable by kind) |
| POST | `/api/v1/refs` | Create branch or tag |
| GET | `/api/v1/refs/{name}` | Get ref details |
| PUT | `/api/v1/refs/{name}` | Update branch target |
| DELETE | `/api/v1/refs/{name}` | Delete branch |
| GET | `/api/v1/refs/{name}/resolve` | Resolve ref to version |
| POST | `/api/v1/diff` | Diff two versions (by ID, branch, or tag) |
| GET | `/api/v1/objects/{hash}` | Get object info |

All `{ref}` parameters accept a version ID, branch name, or tag name.

## Architecture

```
┌─────────────────────────────────────────────┐
│              HTTP API (axum)                 │
│   upload · download · versions · refs · diff│
├─────────────────────────────────────────────┤
│           Service Layer                     │
│   CommitService · DiffEngine · Namespace    │
├──────────────────────┬──────────────────────┤
│   RocksDB CAS        │   SQLite Metadata    │
│   chunks · trees     │   versions · refs    │
│   nodes              │   path index         │
└──────────────────────┴──────────────────────┘
```

## Design Principles

- **Content-addressed** — chunks, trees, and nodes are all addressed by BLAKE3 hash
- **Immutable versions** — version nodes are never modified; branches provide mutable pointers
- **Trait-based storage** — all storage accessed through traits; implementations are swappable
- **Deduplication by default** — identical content is stored once across all versions

## Documentation

- [Demo script](docs/demo.md) — step-by-step POC walkthrough
- [Known limitations & v0.2 recommendations](docs/known-limitations.md)
- [Architecture notes](docs/architecture.md)

## Next Steps (v0.2)

See [docs/known-limitations.md](docs/known-limitations.md) for the full list. Key priorities:

1. Streaming upload with back-pressure for large files
2. Persistent storage benchmarks (RocksDB + SQLite)
3. Garbage collection for orphaned chunks
4. Merge commit support
5. CLI tool
