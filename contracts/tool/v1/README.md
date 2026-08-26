# Mina Tool Contract v1

这个目录是独立于 Rust trait、Tokio 和 Gateway transport 的工具接入契约。Rust、C、C++、Swift、Kotlin/Native 等实现只要遵守同一份 JSON envelope 和 C ABI，就可以在最终程序中静态链接。

## 文件

- `mina_tool.h`：稳定 C ABI v1；
- `manifest.schema.json`：插件和工具声明；
- `invoke-request.schema.json`：单次调用输入；
- `invoke-result.schema.json`：成功或工具级错误输出；
- Rust 对应类型位于 `crates/core/src/tool/contract.rs`，公开路径为 `agent_core::tool`。

## 边界

模型只能看到 manifest 中的工具名称、描述和参数 Schema。`risk_level` 是工具作者提供的默认值，宿主仍必须在执行前做参数校验、动态策略判断和用户审批，不能把插件声明当成授权。

ABI 只传输 UTF-8 JSON 字节，不传输 Rust 的 `String`、trait object、future 或 allocator。这样可以避免编译器版本和语言运行时进入公共边界。

## 静态链接流程

1. 外部库实现一个自定义名称的入口函数，返回 `MinaToolPluginV1`；
2. 最终应用把外部静态库链接进可执行文件；
3. 嵌入层显式调用入口函数并注册返回的 vtable；
4. 宿主解析并校验 `manifest_json`，然后把工具加入 registry；
5. 调用时宿主序列化 `ToolInvokeRequest`，在阻塞执行池调用 `invoke`；
6. 插件返回 `ToolInvokeResult` JSON，宿主读取后必须调用插件提供的 `free`；
7. run 被取消时，宿主可以并发调用可选的 `cancel` 回调。

静态库没有可靠的运行时自动发现机制，所以入口符号由应用命名并显式注册。这样也避免多个静态插件都导出同名符号造成链接冲突。

## 内存与并发规则

- `manifest_json` 由插件持有，在 `drop` 返回前始终有效；
- `request_json` 只在 `invoke` 调用期间有效，插件需要长期使用时必须复制；
- `result_json_out` 的内存由插件分配，并提供匹配的 `free`；
- `invoke`、`cancel` 可能并发发生，插件上下文必须线程安全；
- panic、异常和语言运行时对象不得穿越 ABI；
- ABI 状态只表示边界是否正常。超时、拒绝、上游失败等业务错误必须写入 `ToolInvokeResult::error`。

## V1 限制

V1 的 `invoke` 是同步 ABI。Mina 宿主应在独立阻塞池执行它，外部插件通过 `cancel` 响应取消。原生异步回调、流式工具输出和进程外传输会在后续协议版本扩展，不改变 v1 结构。
