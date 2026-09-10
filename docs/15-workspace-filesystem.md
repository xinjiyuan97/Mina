# 跨环境 Workspace FS 契约与实现

> 状态：基础契约、Native、OPFS、S3 adapter 和文件 Tool 接入已实现。

## 1. 位置与边界

文件系统不属于 `agent-core`。Core 只定义通用 Tool 调用、结果、审批和事件，不知道路径、文件或目录。Workspace FS 位于 `agent-extension::workspace`：

```text
agent-core
    ▲
    │ implements ports
agent-extension
    ├── workspace/contract.rs   # WorkspaceFs 和稳定数据类型
    ├── workspace/native.rs     # 本地与服务端本地目录
    ├── workspace/opfs.rs       # 浏览器 Origin Private File System
    └── workspace/s3.rs         # 服务端 S3 bucket/prefix
    ▲
    │ composes
agent-harness
    ▲
    │ selects concrete adapter
apps
```

依赖规则固定为：

- `extension -> core`；
- `harness -> core + extension`；
- 禁止 `extension -> harness`，集成测试也遵守该规则；
- Provider 配置属于 Extension，Harness 只负责解析和选择配置；
- ADF 定义锁定是纯领域逻辑，属于 Core；QuickJS 执行属于 Harness。

## 2. 契约

`WorkspaceFs` 暴露最小存储原语：

- `stat`：读取文件或目录元数据；
- `read`：有上限的全量或 range 读取；
- `write`：`create_new`、`truncate`、`append`；
- `list`：带 cursor 和 limit 的直接子项分页；
- `create_dir`；
- `remove`；
- `rename`。

路径统一使用 `WorkspacePath`，只接受 workspace-relative 路径，在进入具体后端前规范化 `/` 与 `\\` 并拒绝绝对路径、盘符、NUL 和 `..`。错误使用稳定的 `WorkspaceErrorKind + code + safe_message + retryable`，不把本地 I/O、DOMException 或 AWS SDK 错误直接泄露给模型。

`edit`、`apply_patch`、文本搜索不是 FS 原语，而是基于这些原语实现的上层 Tool。这样 OPFS 或 S3 不需要复制编辑语义。

## 3. 三类环境

### Native：本地和服务端本地目录

`NativeWorkspaceFs` 同时供 CLI、桌面端和服务端本地存储使用：

- 构造时固定 canonical root；
- 拒绝路径穿越和解析后越界；
- 修改操作不跟随最终符号链接；
- 支持 range read、append 和原子 rename；
- revision 由文件长度和修改时间生成。

本地与服务端不维护两份实现，差别只在应用传入的 root 和外围权限策略。

### OPFS：浏览器

`OpfsWorkspaceFs` 通过 `navigator.storage.getDirectory()` 直接访问 OPFS：

- 数据受浏览器 origin 隔离并持久化；
- 支持目录、range read、truncate 和 append；
- 优先使用 `FileSystemHandle.move`，浏览器不支持时文件 rename 降级为 copy/delete；
- 目录 rename 在缺少 `move` 时返回 `Unsupported`；
- DOMException 被转换为统一 Workspace 错误。

OPFS adapter 通过 `workspace-opfs` feature 只在 `wasm32` 编译，不进入原生端产物。

### S3：服务端对象存储

`S3WorkspaceFs` 使用一个 bucket 和可选 prefix 作为 workspace：

- 应用注入已配置的 `aws_sdk_s3::Client`；凭证、endpoint、代理和重试策略不进入 Extension 契约；
- 目录使用 prefix/marker 语义；
- 支持 HTTP Range 读取；
- append 明确返回 `Unsupported`；
- rename 是 copy/delete，`atomic_rename=false`；
- S3 workspace 标记为 `shared=true`，可供多实例服务端访问。

S3 SDK 固定在兼容仓库 Rust 1.92 MSRV 的版本，Cargo resolver 会优先选择 MSRV 兼容的传递依赖。

## 4. Tool 接入

以下实现已经改为注入 `Arc<dyn WorkspaceFs>`：

- `read`；
- `write`；
- `edit`；
- `list_directory`；
- `apply_patch`；
- 默认 workspace text search backend。

`BuiltinToolCatalog::with_file_workspace` 可让服务端把默认 Native adapter 替换为 S3。文件 Tool 的密钥路径、`.git`、`.env`、私钥保护仍属于 Tool/Host policy，不硬编码进通用 FS，因此普通应用也可以直接复用 FS adapter。

Terminal 不走通用 FS。进程执行需要真实 cwd、进程隔离和 OS 文件描述符，继续使用显式的 `NativePathWorkspace + ProcessSandbox`。S3/OPFS 文件可以被 Agent 读写，但不能伪装成本地命令工作目录。Office checker 目前也仍是 native-path-only，后续应改为读取 bytes/archive 后再接通通用 FS。

## 5. Feature

```toml
agent-extension = { path = "crates/extension", features = ["workspace-native"] }
agent-extension = { path = "crates/extension", features = ["workspace-opfs"] }
agent-extension = { path = "crates/extension", features = ["workspace-s3"] }
```

`builtin-tools` 自动启用 `workspace-native`。服务端若要使用 S3，需要额外启用 `workspace-s3`，构造 S3 client 后通过 Catalog 注入。

## 6. 后续边界

- 为写入和 edit 增加基于 revision 的 compare-and-swap，避免多 Agent 覆盖；
- 为 S3 rename 增加操作日志或事务补偿，支持崩溃恢复；
- 为 OPFS 增加浏览器集成测试，而不仅是 WASM 编译测试；
- 将 Office checker 改为 bytes-first，使其可以运行在 S3/OPFS；
- 如要让完整 AgentLoop 在浏览器内直接执行异步 Tool，Core 的异步 port 还需要完成 `wasm32` 下非 `Send` future 的系统性适配；目前浏览器 app 采用 reducer + Worker host，OPFS adapter 本身已经可独立使用。
