# Axiom Core

高性能、版本化、内容寻址的大对象存储引擎，Rust 实现。

Axiom 以存储代码的方式来存储文件：每个版本不可变，每个字节按内容哈希去重，分支和 Diff 是一等公民。与 Git 不同，它专为大型二进制资产（数据集、模型权重、媒体文件）设计，可通过 REST API 使用，也可以直接嵌入 Tauri 桌面应用。

**与同类方案对比：**
| | Axiom Core | Git LFS | DVC | 纯 S3 |
|---|---|---|---|---|
| 内容寻址 | ✅ BLAKE3 | ✅ | ✅ | ❌ |
| 内置版本管理 | ✅ | 依赖 Git | 依赖 Git | ❌ |
| 服务端 Diff | ✅ Merkle | ❌ | ❌ | ❌ |
| 去重 | ✅ chunk 级 | ❌ | ❌ | ❌ |
| 可嵌入（无 HTTP） | ✅ Tauri IPC | ❌ | ❌ | ❌ |
| 多租户 / SaaS | ✅ (`fdb`) | ❌ | ❌ | ✅ |

## 功能

**存储与版本管理**
- FastCDC chunk 级去重（16 KB – 256 KB）— 相同内容全局只存一份
- Merkle Tree（默认扇出 64）— diff 复杂度 O(变更文件数)，而非 O(总文件数)
- 不可变版本节点 + 可移动 branch/tag ref — 与 Git commit 相同的心智模型
- RocksDB 内容存储（chunk、tree、node），支持崩溃恢复
- SQLite 元数据存储（version、ref、path index），WAL 模式 + schema 迁移

**开发者工作流**
- `checkout_to_path` — 将任意版本物化到本地磁盘；Safe 模式跳过本地修改文件，Force 模式强制覆盖
- `compute_status` — 对比工作目录与 HEAD（Added / Modified / Deleted / Untracked）
- branch/tag CRUD、分页历史、ref 感知 diff
- 流式上传（单文件或目录）与流式下载

**运维**
- 软删除回收站，分级保留策略（Free: 7 天 / Pro: 30 天 / 自定义）
- 带优雅期保护的标记-清除 GC（`fdb` feature）
- S3 智能分层生命周期策略（`cloud` feature）
- 每日 GC cron 调度器，FDB 分布式锁 + Prometheus 指标（`fdb` feature）

**分布式 / SaaS**（`fdb` feature）
- 多租户模型，FoundationDB 存储后端
- JWT 认证、RBAC、axum 中间件
- 可达对象 BFS 同步、快进检测、远端追踪 ref

**304+ 自动化测试**，覆盖所有层次。

## 环境依赖

| 依赖 | 适用范围 | 安装方式 |
|---|---|---|
| Rust stable | 全部 | [rustup.rs](https://rustup.rs) |
| RocksDB 系统库 | `local`（默认） | `brew install rocksdb` / `apt install librocksdb-dev` |
| SQLite | `local`（默认） | 通常已预装 |
| protoc | `cloud` feature | `brew install protobuf` / `apt install protobuf-compiler` |
| FoundationDB 客户端 | `fdb` feature | [apple/foundationdb releases](https://github.com/apple/foundationdb/releases) |

## 快速开始

### 启动 HTTP 服务器

```bash
cargo run --release
# → 监听 http://localhost:3000
```

### 上传文件

```bash
# 单文件
curl -X POST 'http://localhost:3000/api/v1/upload/file?path=hello.txt&message=first+commit' \
  --data-binary 'Hello, Axiom!'

# 目录（多文件）
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

### 浏览与下载

```bash
curl http://localhost:3000/api/v1/version/main/ls               # 列出根目录
curl http://localhost:3000/api/v1/version/main/file/hello.txt  # 下载文件
curl http://localhost:3000/api/v1/versions/main/history        # 查看历史
```

### Diff 两个版本

```bash
curl -X POST http://localhost:3000/api/v1/diff \
  -H 'Content-Type: application/json' \
  -d '{"old_version": "v1.0", "new_version": "main"}'
```

## API 端点

所有 `{ref}` 参数均接受 version ID、branch 名称或 tag 名称。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| POST | `/api/v1/upload/file` | 单文件上传（raw body）|
| POST | `/api/v1/upload/directory` | 多文件上传（JSON + base64）|
| GET | `/api/v1/version/{ref}/file/{path}` | 下载文件内容 |
| GET | `/api/v1/version/{ref}/ls` | 列出根目录 |
| GET | `/api/v1/version/{ref}/ls/{path}` | 列出子目录 |
| GET | `/api/v1/versions/{ref}` | 按 ID、branch 或 tag 获取版本 |
| GET | `/api/v1/versions/{ref}/history` | 分页版本历史 |
| GET | `/api/v1/versions/{ref}/path/{path}` | Node 元数据（hash、size、type）|
| POST | `/api/v1/versions` | 创建版本记录 |
| GET | `/api/v1/refs` | 列出 ref（可按 kind 过滤）|
| POST | `/api/v1/refs` | 创建 branch 或 tag |
| GET | `/api/v1/refs/{name}` | 获取 ref 详情 |
| PUT | `/api/v1/refs/{name}` | 更新 branch 目标 |
| DELETE | `/api/v1/refs/{name}` | 删除 branch |
| GET | `/api/v1/refs/{name}/resolve` | 将 ref 解析为 version |
| POST | `/api/v1/diff` | Diff 两个版本 |
| GET | `/api/v1/objects/{hash}` | 获取对象信息 |

## Feature Flags

| Flag | 默认 | 说明 |
|------|------|------|
| `local` | ✅ | RocksDB CAS + SQLite 元数据（单节点）|
| `cloud` | — | gRPC 同步（`proto/sync.proto`）、S3 生命周期策略 |
| `fdb` | — | FoundationDB：多租户、引用计数、标记-清除 GC、Prometheus 指标 |
| `full` | — | `local` + `cloud` |

Desktop 应用始终以 `--no-default-features --features local` 构建。

## 架构

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

所有存储操作都通过 `src/store/traits.rs` 中定义的 trait 进行——后端可替换，不影响上层代码。`Arc<T>` blanket impl 定义在 `store/traits.rs` 末尾，使 `Arc<dyn MyTrait>` 直接可用。

## 项目结构

```
src/
  api/          # axum 路由、DTO、错误映射
    routes/     # 每个路由组一个文件
  model/        # 领域类型（ChunkHash、VersionNode、Ref、DiffResult …）
  store/
    traits.rs   # 所有存储 trait 定义
    rocksdb.rs  # CAS 后端（chunk、tree、node）
    sqlite.rs   # 元数据后端（version、ref、path index、workspace）
    memory.rs   # 测试用内存后端
  chunker.rs    # FastCDC 内容定义分块
  merkle.rs     # Merkle tree build / rehydrate
  namespace.rs  # 目录树构建
  commit.rs     # 版本创建、ref CRUD、历史遍历
  diff_engine.rs
  checkout.rs   # 版本 → 本地文件系统物化
  working_tree.rs  # 对比本地变更状态与 HEAD
  sync/         # 可达 BFS、快进检测、远端 ref、同步会话日志
  auth/         # JWT、RBAC、axum 中间件
  tenant/       # 多租户模型 + FDB repo  [fdb]
  gc/           # 回收站、标记-清除、lifecycle、调度器
tests/          # 集成测试（每个模块一个文件）
benches/        # criterion 基准测试
proto/          # gRPC 定义（sync.proto）  [cloud]
```

## 贡献指南

### 开发环境搭建

```bash
# 克隆并构建（默认 features）
git clone https://github.com/your-org/axiom
cd axiom/axiom-core
cargo build

# 包含所有稳定 feature
cargo build --features full

# 包含 FDB feature（需要本地安装 FoundationDB 客户端）
cargo build --features fdb
```

### 运行测试

```bash
# 单元 + 集成测试（无需外部服务）
cargo test

# 包含 cloud feature 测试
cargo test --features full

# 运行指定测试文件
cargo test --test commit_tests

# 按名称运行单个测试
cargo test version_history_is_paginated
```

> **注意：** `--features fdb` 的测试需要运行中的 FoundationDB 集群。`foundationdb-sys` 的构建脚本在部分平台上存在已知的上游 embedded-include 路径问题，不影响 `local` 或 `full` 构建。

### 运行基准测试

```bash
cargo bench --bench poc_benchmark
cargo bench --bench cas-benchmark
```

### 代码约定

- **存储必须通过 trait** — 服务层和 API 层不直接调用 `RocksDbCasStore` 或 `SqliteMetadataStore`，只使用 `store/traits.rs` 中的 trait。
- **`Arc` blanket impl** — 每添加一个新 trait，都在 `store/traits.rs` 末尾补充对应的 blanket impl，使 `Arc<dyn MyTrait>` 开箱即用。
- **错误类型** — 所有可能失败的操作返回 `CasResult<T>`（即 `Result<T, CasError>`，定义于 `error.rs`）。
- **内容哈希** — 始终使用 `ChunkHash`（BLAKE3），不直接用裸 `Vec<u8>` 表示哈希。
- **测试用内存 State** — 使用 `AppState::memory()` 获取完全内存态，无需临时文件或外部进程。
- **集成测试** — 放在 `tests/`，不放在 `src/` 内。每个文件覆盖一个模块层。

### PR 检查清单

- [ ] `cargo test` 在默认 features 下通过
- [ ] `cargo clippy` 无新增警告
- [ ] 新的公开 API 至少有一个测试用例
- [ ] schema 变更需在 `store/sqlite.rs` 中添加对应迁移函数（`migrate_vN`）

## 设计原则

- **内容寻址** — chunk、tree、node 均以 BLAKE3 哈希寻址；相同字节全局只存一份
- **版本不可变** — 版本节点永不修改，branch 是可移动的命名指针
- **Trait 存储抽象** — 所有存储通过 trait 访问，可替换后端而不改动上层代码
- **默认去重** — 相同内容跨版本、跨工作区共享存储

## 路线图

1. Merge commit 支持
2. CLI 工具（`axiom` 二进制）
3. gRPC 同步端到端打通（`cloud` feature）
4. Web 控制台（SaaS）
5. 计费与配额管理