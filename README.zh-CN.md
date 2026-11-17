# Axiom Core

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

[English](README.md)

Axiom Core 是一个使用 Rust 编写的高性能、版本化、内容寻址存储引擎。

它把 Git 的核心思想带到了大体积二进制资产场景：版本不可变、内容按哈希去重、分支和标签可引用历史节点、Merkle Tree 支持高效 Diff。它既可以作为 HTTP 服务运行，也可以作为 Rust 库嵌入其他应用。

Axiom Core 面向视频、3D 模型、数据集、容器镜像、设计资产等大文件，旨在作为 **版本化内容寻址对象存储服务** 的底层引擎。它融合了：

- **Git** 的版本 DAG 与不可变快照语义
- **兼容 S3 的存储后端** 所提供的对象存储能力
- **IPFS/CAS** 的内容寻址与 chunk 级去重

**长期目标：** 在保留完整变更历史的同时，通过内容复用和去重，将存储成本控制在接近原始对象存储的水平。

## 目录

- [Axiom Core](#axiom-core)
  - [目录](#目录)
  - [为什么选择 Axiom Core](#为什么选择-axiom-core)
    - [方案对比](#方案对比)
  - [核心能力](#核心能力)
    - [存储与版本管理](#存储与版本管理)
    - [开发者工作流](#开发者工作流)
    - [云端与服务能力](#云端与服务能力)
  - [架构](#架构)
  - [仓库结构](#仓库结构)
  - [快速开始](#快速开始)
    - [环境依赖](#环境依赖)
    - [构建](#构建)
    - [启动服务](#启动服务)
    - [上传数据](#上传数据)
    - [浏览与 Diff](#浏览与-diff)
  - [功能开关](#功能开关)
  - [HTTP API 概览](#http-api-概览)
  - [认证](#认证)
  - [开发说明](#开发说明)
    - [测试](#测试)
    - [基准测试](#基准测试)
    - [项目约定](#项目约定)
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
| 多租户云存储后端 | 是，依赖 `cloud` + `fdb` | 否 | 否 | 部分支持 |

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

### 云端与服务能力

- `cloud` feature 提供 gRPC push、pull 和 clone 同步流程
- 可选云存储后端使用兼容 S3 的对象存储保存内容、FoundationDB 保存元数据；需要同时启用 `cloud` 和 `fdb`
- 云后端可选 Redis/DragonflyDB 缓存
- 提供工作区和成员 API、JWT 认证、可选 GitHub OAuth、RBAC、远端 ref 与同步会话支持
- 提供用量与配额统计、审计日志、回收站保留期和垃圾回收能力；具体可用性取决于后端与配置

## 架构

```text
HTTP API (axum)
    |
服务层
    |-- 版本管理、Diff、checkout、工作目录
    |-- 工作区、认证、配额、审计
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
    |-- S3 兼容 CAS + FoundationDB 元数据（云模式）
    |-- 可选 Redis/DragonflyDB 缓存
    |-- 用于测试的内存存储
```

所有存储访问都通过 `src/store/traits.rs` 中定义的 trait 完成。服务层和 API 层应依赖 trait，而不是直接耦合具体后端类型。`AppState` 负责组装并暴露这些共享后端。

## 仓库结构

```text
src/
  api/          axum 路由、请求处理、DTO 和错误映射
  auth/         JWT、RBAC、中间件
  audit/        安全审计事件与存储
  gc/           回收站、保留策略、垃圾回收、调度器
  model/        ChunkHash、VersionNode、Ref 等领域模型
  quota/        用量统计与配额策略
  store/        存储 trait 与后端实现
  sync/         同步协议支持、远端 ref、会话状态
  tenant/       基于 FoundationDB 的多租户支持
  telemetry/    结构化日志与 Prometheus 指标
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
| FoundationDB client | `fdb` feature | FoundationDB 云存储模式需要 |
| S3 兼容 bucket 和凭据 | 云存储模式 | AWS S3 或 MinIO 等兼容服务 |
| Redis/DragonflyDB | 可选云缓存 | 通过 `AXIOM_CACHE_URL` 配置 |

### 构建

```bash
# 默认单机构建
cargo build

# 启用本地和云相关 feature
cargo build --features full

# S3 + FoundationDB 云存储（同时启用本地和 gRPC feature）
cargo build --features "full,fdb"
```

`full` 等价于 `local` + `cloud`，不会启用 `fdb`。

### 启动服务

```bash
# 使用默认 .axiom/ 目录启动本地模式
cargo run --release

# 自定义数据目录和监听地址
cargo run --release -- --data-dir .axiom-dev --listen 127.0.0.1:3000

# 云存储模式；需要 `--features "full,fdb"` 和云配置文件
cargo run --release --features "full,fdb" -- --mode cloud --config axiom.toml

# 查看完整 CLI 选项
cargo run --release -- --help
```

服务默认监听 `0.0.0.0:3000`，本地数据默认写入 `.axiom/`。

云模式从 TOML 文件或环境变量读取 S3 和 FoundationDB 配置。最小 `axiom.toml` 示例：

```toml
[cloud.s3]
bucket = "my-bucket"
region = "us-east-1"

[cloud.fdb]
tenant = "my-tenant"
workspace = "my-workspace"
```

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
| `cloud` | 否 | gRPC 同步流程和云集成所需依赖 |
| `fdb` | 否 | FoundationDB 元数据后端；云存储需要同时启用 `cloud` + `fdb` |
| `full` | 否 | 便捷组合，等价于 `local` + `cloud`，不包含 `fdb` |

## HTTP API 概览

所有 `{ref}` 参数都接受 version ID、branch 名称或 tag 名称。完整 OpenAPI 3.1 契约可通过 `/openapi.json` 获取，内置 API 页面位于 `/docs`。

| 领域 | 代表性接口 |
| --- | --- |
| 健康检查与指标 | `GET /health`、`/healthz`、`/live`、`/ready`、`/metrics` |
| API 文档 | `GET /docs`、`/openapi.json` |
| 认证 | `POST /auth/login`、`/auth/refresh`、`/auth/logout`；`GET /auth/session`、`/auth/jwks` |
| 上传 | `POST /api/v1/upload/file`、`POST /api/v1/upload/directory` |
| 浏览 | `GET /api/v1/version/{ref}/ls`、`GET /api/v1/version/{ref}/file/{path}` |
| 历史 | `GET /api/v1/versions/{ref}`、`GET /api/v1/versions/{ref}/history` |
| 路径元数据 | `GET /api/v1/versions/{ref}/path/{path}` |
| 工作区与成员 | `GET/POST /api/v1/workspaces`、`GET /api/v1/workspaces/{workspace_id}/tree`、工作区成员接口 |
| 引用管理 | `GET/POST /api/v1/refs`、`GET/PUT/DELETE /api/v1/refs/{name}`、`GET /api/v1/refs/{name}/resolve` |
| Diff | `POST /api/v1/diff` |
| 用量与审计 | `GET /api/v1/usage`、`GET /api/v1/audit` |
| 对象查询 | `GET /api/v1/objects/{hash}` |

具体路由实现位于 `src/api/routes/`。启用认证后，受保护的 API 路由要求请求包含配置的身份凭据。

## 认证

本地模式默认不启用 HTTP 认证。设置 `AXIOM_AUTH_ENABLED=1` 可挂载登录和令牌服务；启用后，本地模式默认使用 JWT Bearer Token 校验，云模式默认信任网关注入的身份请求头。可通过 `AXIOM_AUTH_MODE` 设置为 `jwt`、`trusted-gateway` 或 `disabled`。

要让 JWT 签名密钥在进程重启后保持稳定，请配置持久化密钥文件路径。缺少任一密钥时，进程会生成临时密钥对，重启后签发的令牌将无法继续验证：

```bash
AXIOM_AUTH_ENABLED=1 \
AXIOM_JWT_PRIVATE_KEY=/path/to/private.pem \
AXIOM_JWT_PUBLIC_KEY=/path/to/public.pem \
cargo run --release
```

GitHub OAuth 登录还需要配置 `AXIOM_GITHUB_CLIENT_ID`、`AXIOM_GITHUB_CLIENT_SECRET` 和 `AXIOM_GITHUB_REDIRECT_URI`。`AXIOM_AUTH_DEV_SECRET` 仅用于开发环境，禁止用于生产环境。

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

为纳入本项目而有意提交的贡献，除非另有明确说明，均依照 [LICENSE](LICENSE) 第 5 节按 Apache-2.0 条款授权。请仅提交你有权贡献的内容。

## 路线图

1. 提供完整的合并与冲突解决工作流
2. 提供面向开发者和运维人员的独立 CLI 工具
3. 提供 Web 管理控制台
4. 集成计费与套餐管理

## 许可证

Axiom Core 基于 [Apache License, Version 2.0](LICENSE) 发布（SPDX 标识：`Apache-2.0`）。第三方依赖和素材仍适用各自的许可证，请查阅其上游许可与声明文件。
