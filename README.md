# Axiom

Axiom是下一代高性能、版本化、内容寻址的大对象存储引擎

# Axiom-core

Axiom 的核心 Rust 原型仓库，当前聚焦于版本化、内容寻址的大对象存储引擎基础能力。项目处于 v0.1 POC 的基础设施阶段，已经完成统一领域模型、内存版存储抽象，以及一条最小可运行的 chunk → tree root → version → ref 演示链路。

## 当前状态

已经具备以下能力：

- 基于 BLAKE3 的内容哈希与版本 ID 建模
- 面向 v0.1 的领域模型拆分：chunk、tree、node、version、ref、diff
- 内存版 repository / store 抽象：ChunkStore、TreeStore、NodeStore、VersionRepo、RefRepo
- **RocksDB 持久化 CAS**：chunk、tree node、directory node 三类数据通过独立 Column Family 持久化到本地磁盘，支持幂等写入和跨进程重启恢复
- 分支与标签的基础引用语义，其中 tag 默认不可覆盖
- 兼容过渡期的旧版 `cas` / `version` 模块
- 37 个自动化测试覆盖内存实现、RocksDB 持久化、跨 reopen 恢复和错误路径

当前尚未实现：FastCDC、文件级 Merkle builder、目录树提交流程、SQLite 元数据层、HTTP API 与流式上传下载。

## 快速开始

环境要求：

- Rust stable
- Cargo

安装依赖并运行演示：

```bash
cargo run
```

执行测试：

```bash
cargo test
```

运行基准：

```bash
cargo bench
```

## 当前演示做了什么

`src/main.rs` 包含两段演示：

**InMemory Demo** — 验证领域模型连通性：

1. 写入一段原始字节到 `InMemoryChunkStore`
2. 对 chunk hash 列表计算对象根哈希
3. 创建 `VersionNode`
4. 创建名为 `main` 的 branch ref
5. 通过 ref 解析到 version，再解析到 root hash
6. 从 chunk store 取回原始数据

**RocksDB Persistence Demo** — 验证磁盘持久化能力：

1. 打开 `.axiom/demo-cas` 目录下的 RocksDB 实例
2. 写入 chunk 并验证幂等性（重复写入返回相同 hash）
3. 写入 tree node 引用该 chunk
4. 关闭 store，模拟进程退出
5. 重新打开同一路径，验证 chunk 和 tree node 均可恢复

运行后会在项目根目录生成 `.axiom/demo-cas/` 数据目录（已被 `.gitignore` 忽略）。

## 设计原则

- 资产即对象：内容通过哈希与树结构表达
- 版本即声明：版本节点不可变，refs 负责命名和移动
- 存储即哈希：chunk、tree、node 均以内容哈希寻址
- 抽象先于实现：HTTP 层与 RocksDB / SQLite 落盘实现通过 trait 解耦，上层代码不直接依赖存储细节

## 下一步

按当前计划，后续实现顺序为：

1. ~~RocksDB 持久化 CAS~~ ✅ 已完成（AXIOM-102）
2. SQLite 元数据层
3. FastCDC 分块
4. 文件级 Merkle Tree
5. 目录树、Version Commit 与 refs 闭环
6. Diff Engine 与 axum API
