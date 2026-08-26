# 工具包与跨语言静态链接契约

> 状态：内置工具包和 Tool Contract v1 已实现；安全的 Rust 核心不直接加载裸 FFI 指针。

## 1. 包边界

```text
apps/server
  ├─→ agent-core::harness/tool
  └─→ agent-extension::tool ─→ agent-core

external C/C++/Swift static library
  └─→ contracts/tool/v1/mina_tool.h + JSON Schema
```

- `agent-core::harness/tool`：Agent Loop、稳定 DTO、Tool trait/registry、校验、取消、超时、审批和事件；
- `agent-extension::tool`：具体工具实现及宿主选择工具的 builder；
- `contracts/tool/v1`：其他语言使用的 C ABI 和 JSON Schema；
- `apps/server`：唯一装配点，选择 workspace 和允许启用的高风险能力。

依赖必须保持单向。Harness 不依赖具体工具，外部契约也不依赖 Tokio、Axum 或模型 provider。

## 2. 内置工具

| 工具 | 默认风险 | 宿主能力边界 |
|---|---|---|
| `get_current_time` | Low | UTC clock |
| `read` | Low | workspace 内、UTF-8、最大 1 MiB、拒绝受保护路径 |
| `list_directory` | Low | workspace 内、单层、最多 500 项 |
| `search` | Backend 决定 | 默认 workspace text backend 为 Low；外部网络 backend 至少 Medium |
| `write` | Medium | workspace 内创建/覆盖，父目录必须存在，拒绝 symlink 和受保护路径 |
| `edit` | Medium | 精确文本替换，默认要求唯一匹配，拒绝 symlink 和受保护路径 |
| `shell_command/exec_command/write_stdin` | High | 显式 opt-in、经 `ProcessSandbox`、受管会话、输出上限 |

`BuiltinToolCatalog::new(workspace)` 默认装载时钟、文件和 workspace search 工具。宿主调用 `enable_terminal_tools()` 后启用 `shell_command/exec_command/write_stdin/apply_patch`；这一步只代表能力可用，不代表调用获批。Agent Loop 对 Medium/High 风险仍触发用户审批。宿主可以通过 `with_search_backend` 和 `with_process_sandbox` 替换具体实现。

风险声明是默认提示，不是权限证明。未来参数级 `ToolPolicy` 可以把同一工具的不同调用重新分类，宿主限制永远可以比插件声明更严格。

## 3. 为什么 ABI 使用 JSON

直接跨语言暴露 Rust `String`、enum layout、trait object 或 `Future` 会绑定 Rust 编译器和 allocator。V1 只固定少量 C layout：借用字节、带释放回调的返回字节和插件 vtable。业务数据通过三个 JSON Schema 传输：

```text
ToolPluginManifest
ToolInvokeRequest
ToolInvokeResult
```

manifest 可以包含多个工具。调用通过 `tool_name` 路由；`run_id` 和 `call_id` 用于关联审计、取消和结果。ABI 返回码只表示边界是否工作，正常工具失败必须使用 `status = error` 的结果 envelope。

## 4. 版本和兼容性

- `protocol_version = 1` 是 JSON 语义版本；
- `abi_version = 1` 是 C layout 版本；
- vtable 包含 `struct_size`，允许兼容版本在尾部追加字段；
- V1 字段不会改变含义或复用；破坏性变化发布 v2；
- 宿主必须拒绝未知 ABI/协议版本、重复工具名、非法 Schema 和空标识符。

`agent-core::tool` 的测试会把 Rust 序列化结果交给发布的 JSON Schema 校验，防止 Rust DTO 和外部文件发生漂移。

## 5. 静态链接与安全隔离

静态库没有通用的运行时发现机制，因此每个外部库导出应用自定义名称的入口函数，最终应用显式注册其 `MinaToolPluginV1`。自定义符号也避免多个静态库发生符号冲突。

FFI 必然涉及裸指针和外部 allocator，而当前 Rust workspace 全局 `unsafe_code = forbid`。因此 V1 先稳定契约，不在 Harness 中放宽该规则。后续 host loader 应作为独立、极小、可审计的 adapter crate：

1. 验证指针、`struct_size`、ABI version 和 UTF-8；
2. 复制 manifest/result 后立即调用插件的释放函数；
3. 在 blocking pool 执行同步 `invoke`；
4. 将 run cancellation 转成并发 `cancel(call_id)`；
5. 捕获 ABI 错误并映射成安全 `ToolError`；
6. 不允许 panic、异常或宿主密钥穿越边界。

未来的动态库、子进程、WASM 或远程 MCP adapter 可以复用同一 JSON envelope，但各自定义传输和身份验证；它们不应改变 Agent Loop。
