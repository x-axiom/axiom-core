# Axiom Core

A high-performance, versioned, content-addressed large-object storage engine written in Rust.

Axiom stores files the way Git stores code: every version is immutable, every byte is deduplicated by content hash, and branching/diffing are first-class operations. Unlike Git, it is built for large binary assets (datasets, model weights, media files) and exposes a REST API or embeds directly in a Tauri desktop app.

**Compared to alternatives:**
| | Axiom Core | Git LFS | DVC | Plain S3 |
|---|---|---|---|---|
| Content-addressed | ✅ BLAKE3 | ✅ | ✅ | ❌ |
| Built-in versioning | ✅ | via Git | via Git | ❌ |
| Server-side diff | ✅ Merkle | ❌ | ❌ | ❌ |
| Deduplication | ✅ chunk-level | ❌ | ❌ | ❌ |
| Embeddable (no HTTP) | ✅ Tauri IPC | ❌ | ❌ | ❌ |
| Multi-tenant / SaaS | ✅ (`fdb`) | ❌ | ❌ | ✅ |

## Features

**Storage & versioning**
- Chunk-level deduplication via FastCDC (16 KB – 256 KB chunks) — identical content stored once across all versions
- Merkle tree with configurable fan-out (default 64) — O(changed files) diff, not O(total files)
- Immutable version nodes + mutable branch/tag refs — same mental model as Git commits
- RocksDB-backed content store (chunks, trees, nodes) with crash recovery
- SQLite metadata store (versions, refs, path index) in WAL mode with schema migrations

**Developer workflow**
- `checkout_to_path` — materialise any version onto disk; Safe mode skips locally-modified files, Force overwrites all
- `compute_status` — compare working directory against HEAD (Added / Modified / Deleted / Untracked)
- Branch/tag CRUD, paginated history, ref-aware diff
- Streaming upload (single file or directory) and streaming download

**Operations**
- Soft-delete recycle bin with per-tier retention (Free: 7 d, Pro: 30 d, Custom)
- Mark-sweep garbage collection with grace-period safety (`fdb` feature)
- S3 intelligent-tiering lifecycle policies (`cloud` feature)
- Daily GC cron scheduler with FDB distributed lock and Prometheus metrics (`fdb` feature)

**Distributed / SaaS** (`fdb` feature)
- Multi-tenant model with FoundationDB-backed tenant repository
- JWT authentication, RBAC, axum middleware
- Reachable-object BFS sync, fast-forward detection, remote-tracking refs

**304+ automated tests** across all layers.

## Prerequisites

| Dependency | Required for | Install |
|---|---|---|
| Rust stable | all | [rustup.rs](https://rustup.rs) |
| RocksDB system libs | `local` (default) | `brew install rocksdb` / `apt install librocksdb-dev` |
| SQLite | `local` (default) | usually pre-installed |
| protoc | `cloud` feature | `brew install protobuf` / `apt install protobuf-compiler` |
| FoundationDB client | `fdb` feature | [apple/foundationdb releases](https://github.com/apple/foundationdb/releases) |

## Quick Start

### Start the HTTP server

```bash
cargo run --release
# → listening on http://localhost:3000
```

### Upload files

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

### Browse and download

```bash
curl http://localhost:3000/api/v1/version/main/ls            # list root
curl http://localhost:3000/api/v1/version/main/file/hello.txt  # download
curl http://localhost:3000/api/v1/versions/main/history      # history
```

### Diff two versions

```bash
curl -X POST http://localhost:3000/api/v1/diff \
  -H 'Content-Type: application/json' \
  -d '{"old_version": "v1.0", "new_version": "main"}'
```

## API Endpoints

All `{ref}` parameters accept a version ID, branch name, or tag name.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/upload/file` | Single-file upload (raw body) |
| POST | `/api/v1/upload/directory` | Multi-file upload (JSON + base64) |
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
| POST | `/api/v1/diff` | Diff two versions |
| GET | `/api/v1/objects/{hash}` | Get object info |

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `local` | ✅ | RocksDB CAS + SQLite metadata (single-node) |
| `cloud` | — | gRPC sync (`proto/sync.proto`), S3 lifecycle policies |
| `fdb` | — | FoundationDB: multi-tenant, reference counting, mark-sweep GC, Prometheus metrics |
| `full` | — | `local` + `cloud` |

The desktop app always builds with `--no-default-features --features local`.

## Architecture

```
┌─────────────────────────────────────────────┐
│              HTTP API (axum)                 │
│   upload · download · versions · refs · diff│
├─────────────────────────────────────────────┤
│           Service Layer                      │
│   CommitService · DiffEngine · Namespace    │
│   Checkout · WorkingTree · GC · Auth · Sync │
├──────────────────────┬──────────────────────┤
│   RocksDB CAS        │   SQLite Metadata    │
│   chunks · trees     │   versions · refs    │
│   nodes              │   path index         │
└──────────────────────┴──────────────────────┘
```

All storage goes through traits in `src/store/traits.rs` — backends are swappable without touching service code. `Arc<T>` blanket impls let you pass `Arc<dyn Trait>` wherever a concrete type was expected.

## Project Structure

```
src/
  api/          # axum router, DTOs, error mapping
    routes/     # one file per route group
  model/        # domain types (ChunkHash, VersionNode, Ref, DiffResult …)
  store/
    traits.rs   # all storage trait definitions
    rocksdb.rs  # CAS backend (chunks, trees, nodes)
    sqlite.rs   # metadata backend (versions, refs, path index, workspaces)
    memory.rs   # in-memory backend used in tests
  chunker.rs    # FastCDC content-defined chunking
  merkle.rs     # Merkle tree build / rehydrate
  namespace.rs  # directory tree construction
  commit.rs     # version creation, ref CRUD, history
  diff_engine.rs
  checkout.rs   # materialise version → local filesystem
  working_tree.rs  # compute local change status vs HEAD
  sync/         # reachable BFS, fast-forward, remote refs, session log
  auth/         # JWT, RBAC, axum middleware
  tenant/       # multi-tenant model + FDB repo  [fdb]
  gc/           # recycle bin, mark-sweep, lifecycle, scheduler
tests/          # integration tests (one file per module)
benches/        # criterion benchmarks
proto/          # gRPC definitions (sync.proto)  [cloud]
```

## Contributing

### Development setup

```bash
# Clone and build (default features)
git clone https://github.com/your-org/axiom
cd axiom/axiom-core
cargo build

# Build with all stable features
cargo build --features full

# Build with FDB features (requires FoundationDB client installed)
cargo build --features fdb
```

### Running tests

```bash
# Unit + integration tests (no external services needed)
cargo test

# Include cloud feature tests
cargo test --features full

# Run a specific test file
cargo test --test commit_tests

# Run a specific test by name
cargo test version_history_is_paginated
```

> **Note:** `--features fdb` tests require a running FoundationDB cluster. The `foundationdb-sys` build script also has a known upstream issue with the embedded-include path on some platforms — this does not affect `local` or `full` builds.

### Running benchmarks

```bash
cargo bench --bench poc_benchmark
cargo bench --bench cas-benchmark
```

### Code conventions

- **All storage through traits** — never call `RocksDbCasStore` or `SqliteMetadataStore` directly in service/API code; use the traits in `store/traits.rs`.
- **`Arc` blanket impls** — defined at the bottom of `store/traits.rs`; add one whenever you add a new trait so `Arc<dyn MyTrait>` works automatically.
- **Error type** — all fallible operations return `CasResult<T>` (alias for `Result<T, CasError>` from `error.rs`).
- **Content hashes** — always `ChunkHash` (BLAKE3); never raw `Vec<u8>` for hashes.
- **In-memory state for tests** — use `AppState::memory()` to get a fully in-memory state; no temp files or external processes needed.
- **Integration tests** — live in `tests/`, not inside `src/`. Each file covers one module layer.

### PR checklist

- [ ] `cargo test` passes with default features
- [ ] `cargo clippy` produces no new warnings
- [ ] New public API has at least one test
- [ ] Storage changes include a SQLite migration (`migrate_vN` in `store/sqlite.rs`) if the schema changed

## Design Principles

- **Content-addressed** — chunks, trees, and nodes are addressed by BLAKE3 hash; identical bytes are stored once
- **Immutable versions** — version nodes are never modified; branches are mutable pointers
- **Trait-based storage** — all storage accessed through traits; swap backends without touching service code
- **Deduplication by default** — same content shared across all versions and all workspaces

## Roadmap

1. Merge commit support
2. CLI tool (`axiom` binary)
3. gRPC sync end-to-end (`cloud` feature)
4. Web console (SaaS)
5. Billing and quota enforcement
