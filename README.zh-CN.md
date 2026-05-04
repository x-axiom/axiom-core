# Axiom Core

[English](README.md)

Axiom Core 是一个使用 Rust 编写的高性能、版本化、内容寻址存储引擎。

它把 Git 的核心思想带到了大体积二进制资产场景：版本不可变、内容按哈希去重、分支和标签可引用历史节点、Merkle Tree 支持高效 Diff。它既可以作为 HTTP 服务运行，也可以直接嵌入其他应用。

## 目录

- [为什么选择 Axiom Core](#为什么选择-axiom-core)
- [核心能力](#核心能力)
- [架构](#架构)
- [仓库结构](#仓库结构)
- [快速开始](#快速开始)
- [功能开关](#功能开关)
- [HTTP API 概览](#http-api-概览)
- [开发说明](#开发说明)
- [贡献指南](#贡献指南)
- [路线图](#路线图)
- [许可证](#许可证)

## 为什么选择 Axiom Core

Axiom Core 适合这样一类场景：普通对象存储缺少版本语义，而 Git 一类工具又不适合直接管理大型二进制文件。

- 使用 BLAKE3 做内容寻址
- 基于 chunk 的去重，可跨版本复用重复内容
- 版本节点不可变，分支和标签作为可移动引用
- 基于 Merkle Tree 的高效变更检测和 Diff
- 通过 trait 抽象解耦上层逻辑与底层存储实现
- 同时覆盖本地单机模式和面向云的扩展能力

### 方案对比

| 能力 | Axiom Core | Git LFS | DVC | 纯 S3 |
| --- | --- | --- | --- | --- |
| 内容寻址存储 | 是，BLAKE3 | 部分支持 | 部分支持 | 否 |
| 内建版本图 | 是 | 依赖 Git | 依赖 Git | 否 |
| 服务端 Diff | 是，基于 Merkle | 否 | 否 | 否 |
| Chunk 级去重 | 是 | 否 | 否 | 否 |
| 无 HTTP 嵌入 | 是 | 否 | 否 | 否 |
| 多租户 SaaS 路径 | 是，依赖 `fdb` | 否 | 否 | 部分支持 |

## 核心能力

### 存储与版本管理

- 基于 FastCDC 的内容定义分块与 chunk 级去重
- 使用 RocksDB 保存 chunk、tree、node 等内容寻址对象
- 使用 SQLite 保存 version、ref、path index 和 workspace 元数据
- 版本节点不可变，分支和标签以引用的形式指向版本
- 统一使用 BLAKE3 哈希标识对象

### 开发者工作流

- 支持上传单文件或整目录快照
- 支持浏览版本树、查看元数据、下载文件内容
- 支持比较两个版本、分支或标签之间的差异
- 支持把任意版本 checkout 到本地工作目录
- 支持对比工作目录与当前 HEAD 的状态差异

### 分布式与 SaaS 扩展

- `cloud` feature 提供 gRPC 同步能力
- `fdb` feature 提供基于 FoundationDB 的多租户扩展路径
- 包含 JWT 认证、RBAC、远端 ref 与同步会话支持
- 面向复杂部署提供 GC、保留期和生命周期管理能力

## 架构

```text
HTTP API (axum)
    |
服务层
    |-- commit.rs
    |-- diff_engine.rs
    |-- namespace.rs
    |-- checkout.rs
    |-- working_tree.rs
    |-- sync/
    |-- gc/
    |
存储 Trait (src/store/traits.rs)
    |-- ChunkStore
    |-- TreeStore
    |-- NodeStore
    |-- VersionRepo
    |-- RefRepo
    |-- PathIndexRepo
    |-- WorkspaceRepo
    |-- SyncStore
    |
实现层
    |-- RocksDB CAS store
    |-- SQLite metadata store
    |-- 用于测试的内存存储
```

所有存储访问都通过 `src/store/traits.rs` 中定义的 trait 完成。服务层和 API 层应依赖 trait，而不是直接耦合具体后端类型。`AppState` 负责组装并暴露这些共享后端。

## 仓库结构

```text
src/
  api/          axum 路由、请求处理、DTO 和错误映射
  auth/         JWT、RBAC、中间件
  gc/           回收站、保留策略、垃圾回收、调度器
  model/        ChunkHash、VersionNode、Ref 等领域模型
  store/        存储 trait 与后端实现
  sync/         同步协议支持、远端 ref、会话状态
  tenant/       基于 FoundationDB 的多租户支持
  checkout.rs   将存储内容物化到本地文件系统
  chunker.rs    FastCDC 内容定义分块
  commit.rs     创建版本、管理 ref、遍历历史
  diff_engine.rs
  merkle.rs     构建和遍历 Merkle Tree
  namespace.rs  构建目录树命名空间
  working_tree.rs
tests/          集成测试
benches/        Criterion 基准测试
proto/          同步相关 gRPC 定义
```

## 快速开始

### 环境依赖

| 依赖 | 用途 | 说明 |
| --- | --- | --- |
| Rust stable | 所有构建 | 从 [rustup.rs](https://rustup.rs) 安装 |
| RocksDB | `local` feature | `brew install rocksdb` 或发行版对应包 |
| SQLite CLI | 可选 | 用于手动查看元数据，不是构建必需项 |
| `protoc` | `cloud` feature | `brew install protobuf` 或发行版对应包 |
| FoundationDB client | `fdb` feature | 仅在 FoundationDB 相关流程中需要 |

### 构建

```bash
# 默认单机构建
cargo build

# 启用本地和云相关 feature
cargo build --features full

# 启用 FoundationDB 扩展能力
cargo build --features fdb
```

### 启动服务

```bash
# 使用默认 .axiom/ 目录启动本地模式
cargo run --release

# 自定义数据目录和监听地址
cargo run --release -- --data-dir .axiom-dev --listen 127.0.0.1:3000

# 查看完整 CLI 选项
cargo run --release -- --help
```

服务默认监听 `0.0.0.0:3000`，本地数据默认写入 `.axiom/`。

### 上传数据

```bash
# 上传单文件
curl -X POST 'http://localhost:3000/api/v1/upload/file?path=hello.txt&message=first+commit' \
  --data-binary 'Hello, Axiom!'

# 上传目录快照
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

### 浏览与 Diff

```bash
# 列出 main 引用下的根目录
curl http://localhost:3000/api/v1/version/main/ls

# 下载指定版本中的文件
curl http://localhost:3000/api/v1/version/main/file/hello.txt

# 查看版本历史
curl http://localhost:3000/api/v1/versions/main/history

# 比较两个引用
curl -X POST http://localhost:3000/api/v1/diff \
  -H 'Content-Type: application/json' \
  -d '{"old_version": "v1.0", "new_version": "main"}'
```

## 功能开关

| Flag | 默认启用 | 作用 |
| --- | --- | --- |
| `local` | 是 | 用于本地或嵌入式场景的 RocksDB CAS 与 SQLite 元数据存储 |
| `cloud` | 否 | gRPC 同步和面向云的扩展能力 |
| `fdb` | 否 | 基于 FoundationDB 的租户、认证、GC 与可观测性扩展 |
| `full` | 否 | 便捷组合，等价于 `local` + `cloud` |

## HTTP API 概览

所有 `{ref}` 参数都接受 version ID、branch 名称或 tag 名称。

| 领域 | 代表性接口 |
| --- | --- |
| 健康检查 | `GET /health` |
| 上传 | `POST /api/v1/upload/file`、`POST /api/v1/upload/directory` |
| 浏览 | `GET /api/v1/version/{ref}/ls`、`GET /api/v1/version/{ref}/file/{path}` |
| 历史 | `GET /api/v1/versions/{ref}`、`GET /api/v1/versions/{ref}/history` |
| 路径元数据 | `GET /api/v1/versions/{ref}/path/{path}` |
| 引用管理 | `GET /api/v1/refs`、`POST /api/v1/refs`、`PUT /api/v1/refs/{name}` |
| Diff | `POST /api/v1/diff` |
| 对象查询 | `GET /api/v1/objects/{hash}` |

具体路由实现位于 `src/api/routes/`。

## 开发说明

### 测试

```bash
# 运行默认 feature 下的全部测试
cargo test

# 运行 cloud 相关测试
cargo test --features full

# 运行指定集成测试文件
cargo test --test commit_tests

# 按名称运行单个测试
cargo test version_history_is_paginated
```

### 基准测试

```bash
cargo bench --bench poc_benchmark
cargo bench --bench cas-benchmark
cargo bench --bench working_tree_bench
```

### 项目约定

- 存储访问统一收敛在 `src/store/traits.rs` 中定义的 trait 上
- 测试优先使用 `AppState::memory()` 构建全内存环境
- 集成测试放在 `tests/`，不要放入 `src/`
- 可能失败的存储和服务操作统一返回 `CasResult<T>`
- `ChunkHash` 与 `VersionId` 是对象标识的唯一权威类型

## 贡献指南

欢迎提交 issue 和 pull request。

提交 PR 前，建议至少完成以下检查：

- 确认相关测试通过
- 为行为变化补充或更新测试
- 新增代码不要绕过现有存储 trait
- 如果 schema 变化，补充对应的 SQLite migration
- 让文档与实际行为保持一致

## 路线图

1. 支持 merge commit
2. 提供独立 CLI 工具
3. 打通端到端 gRPC 同步流程
4. 提供 Web 管理控制台
5. 完善计费与配额能力

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 发布。
