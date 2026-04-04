# Axiom

Axiom是下一代高性能、版本化、内容寻址的大对象存储引擎

# Axiom-core

Axiom 的核心 Rust 原型仓库，当前聚焦于版本化、内容寻址的大对象存储引擎基础能力。项目处于 v0.1 POC 的基础设施阶段，已经完成统一领域模型、内存版存储抽象，以及一条最小可运行的 chunk → tree root → version → ref 演示链路。

## 当前状态

已经具备以下能力：

- 基于 BLAKE3 的内容哈希与版本 ID 建模
- 面向 v0.1 的领域模型拆分：chunk、tree、node、version、ref、diff
- 内存版 repository / store 抽象：ChunkStore、TreeStore、NodeStore、VersionRepo、RefRepo
- 分支与标签的基础引用语义，其中 tag 默认不可覆盖
- 兼容过渡期的旧版 `cas` / `version` 模块
- 单元测试覆盖基础对象构建、序列化和内存实现行为

当前尚未实现：FastCDC、文件级 Merkle builder、目录树提交流程、RocksDB、SQLite、HTTP API 与流式上传下载。

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

`src/main.rs` 展示了当前最小闭环：

1. 写入一段原始字节到 `InMemoryChunkStore`
2. 对 chunk hash 列表计算对象根哈希
3. 创建 `VersionNode`
4. 创建名为 `main` 的 branch ref
5. 通过 ref 解析到 version，再解析到 root hash
6. 从 chunk store 取回原始数据

这条链路验证的是模块边界和领域对象是否已经连通，而不是完整的文件存储产品能力。

## 设计原则

- 资产即对象：内容通过哈希与树结构表达
- 版本即声明：版本节点不可变，refs 负责命名和移动
- 存储即哈希：chunk、tree、node 均以内容哈希寻址
- 抽象先于实现：HTTP 层与未来的 RocksDB / SQLite 落盘实现通过 trait 解耦

## 下一步

按当前计划，后续实现顺序为：

1. RocksDB 持久化 CAS
2. SQLite 元数据层
3. FastCDC 分块
4. 文件级 Merkle Tree
5. 目录树、Version Commit 与 refs 闭环
6. Diff Engine 与 axum API

如果你要继续推进，建议优先从内容落盘和元数据事务边界开始，避免后续 FastCDC、refs、API 反向推翻接口。
