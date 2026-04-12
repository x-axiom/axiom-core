# Axiom

Axiom is a next-generation high-performance, versioned, content-addressed large-object storage engine.

# Axiom-core

This repository contains the core Rust prototype for Axiom. It currently focuses on the foundational capabilities of a versioned, content-addressed large-object storage engine. The project is in the v0.1 POC infrastructure phase and already includes a unified domain model, in-memory storage abstractions, and a minimal end-to-end demo flow of chunk -> tree root -> version -> ref.

## Current Status

The project currently provides:

- BLAKE3-based content hashing and version ID modeling
- Domain model decomposition for v0.1: chunk, tree, node, version, ref, and diff
- In-memory repository and store abstractions: `ChunkStore`, `TreeStore`, `NodeStore`, `VersionRepo`, and `RefRepo`
- **Persistent RocksDB CAS**: chunk, tree node, and directory node data are stored in separate column families on local disk, with idempotent writes and recovery after process restart
- Basic branch and tag reference semantics, with tags non-overwritable by default
- Compatibility with the transitional legacy `cas` and `version` modules
- 37 automated tests covering the in-memory implementation, RocksDB persistence, reopen recovery, and error paths

Not implemented yet: FastCDC, file-level Merkle builder, directory tree commit flow, SQLite metadata layer, HTTP API, and streaming upload/download.

## Quick Start

Requirements:

- Rust stable
- Cargo

Install dependencies and run the demo:

```bash
cargo run
```

Run tests:

```bash
cargo test
```

Run benchmarks:

```bash
cargo bench
```

## What The Demo Does

`src/main.rs` contains two demo flows:

**InMemory Demo** - verifies domain model connectivity:

1. Write raw bytes into `InMemoryChunkStore`
2. Compute an object root hash from a list of chunk hashes
3. Create a `VersionNode`
4. Create a branch ref named `main`
5. Resolve the ref to a version, then resolve the version to a root hash
6. Read the original data back from the chunk store

**RocksDB Persistence Demo** - verifies on-disk persistence:

1. Open a RocksDB instance at `.axiom/demo-cas`
2. Write a chunk and verify idempotency by getting the same hash on repeated writes
3. Write a tree node that references the chunk
4. Close the store to simulate process exit
5. Reopen the same path and verify that both the chunk and tree node are recovered

After execution, a `.axiom/demo-cas/` data directory is created in the project root. It is already ignored by `.gitignore`.

## Design Principles

- Assets are objects: content is represented through hashes and tree structures
- Versions are declarations: version nodes are immutable, while refs provide names and movement
- Storage is hash-addressed: chunks, trees, and nodes are all addressed by content hash
- Abstractions come before implementations: the HTTP layer and RocksDB / SQLite persistence implementations are decoupled through traits, and higher-level code does not depend directly on storage details

## Next Steps

The planned implementation order is:

1. ~~Persistent RocksDB CAS~~ completed (AXIOM-102)
2. SQLite metadata layer
3. FastCDC chunking
4. File-level Merkle tree
5. Directory tree, version commit, and refs end-to-end flow
6. Diff engine and axum API
