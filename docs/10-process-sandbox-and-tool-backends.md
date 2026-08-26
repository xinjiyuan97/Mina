# 进程沙箱与工具后端

> 状态：`ProcessSandbox` 契约位于 `agent-core::sandbox`，Host adapter 位于 `agent-extension::sandbox`。一次性执行与受管长会话、`shell_command/exec_command/write_stdin/apply_patch` 已实现。强隔离 adapter 尚未实现，当前 UI 会明确显示 `host_process / isolation=none`。

## 1. 结论

所有 terminal process tools 必须位于沙箱端口之后。固定 cwd、清空环境和限制输出只能减少误用，不能阻止进程读取宿主文件、访问网络、创建子进程或消耗资源，因此不能称为安全沙箱。

推荐分层：

```text
AgentLoop -> shell_command/exec_command/write_stdin
          -> ProcessSandbox contract
                                  ├─ HostProcessSandbox       开发环境，非强隔离
                                  ├─ Bubblewrap/Nsjail        Linux 单机默认候选
                                  ├─ OCI + gVisor             共享服务
                                  └─ Firecracker microVM      多租户高风险任务
```

AgentLoop 只理解工具调用、风险与审批，不依赖 bubblewrap、Docker 或虚拟机。具体 adapter 只在 Server composition root 选择。

## 2. 可选的进程级隔离

| 方案 | 适用平台 | 隔离能力 | 成本与建议 |
|---|---|---|---|
| Linux namespaces + seccomp + cgroups | Linux | 文件系统、网络、syscall、PID、资源 | 基础机制，不建议自行从零组合 |
| bubblewrap | Linux | user/mount/PID/network namespace | 桌面或单机首选，体积小，策略直观 |
| nsjail | Linux | namespaces、seccomp、rlimit/cgroups | 服务端任务，策略能力更完整 |
| Landlock | Linux | 进程自我限制文件系统访问 | 适合嵌入式补强，不能单独覆盖网络和完整 syscall |
| systemd-run | Linux | transient unit、cgroup、部分 namespace | systemd 环境易部署，但不是跨平台方案 |
| Docker/Podman OCI | Linux/macOS VM | mount、network、user、cgroup | 生态成熟，启动和镜像成本较高 |
| gVisor | Linux | 用户态内核加强 syscall 隔离 | 多租户容器，比普通 OCI 更安全、兼容性需验收 |
| Firecracker/Kata | Linux | microVM/虚拟机边界 | 高风险多租户首选，调度和镜像成本最高 |
| Windows Job Object + restricted token/AppContainer | Windows | 进程树、资源、权限与部分文件/网络 | Windows 原生 adapter 组合 |
| macOS Seatbelt/sandbox-exec | macOS | profile 驱动的进程限制 | `sandbox-exec` 已废弃，只适合开发期 best-effort，不作为长期公共 API |
| Lima/Virtualization.framework VM | macOS | Linux VM 边界 | macOS 上运行不可信终端的可靠方案 |
| WASI/WebAssembly | 跨平台 | capability-based 文件、网络和资源 | 只适用于可编译到 WASI 的程序，不是任意终端替代品 |

macOS App Sandbox 面向签名应用，不适合动态约束任意 CLI；Endpoint Security 是监控/控制接口，也不是完整沙箱。Mina 在 macOS 开发时可继续使用 Host adapter + 审批，真正的不可信任务应放到 Linux VM、OCI VM 或远端 sandbox worker。

## 3. ProcessSandbox 契约

当前端口包含：

- `ProcessSandboxDescriptor`：identity、kind、版本、隔离强度及文件系统/网络/资源声明；
- `ProcessSandboxRequest`：显式 program、args、workspace root、取消信号和可选 wall-time；
- `ProcessSandboxOutput`：exit code、stdout/stderr 和截断标记；
- `ProcessSandbox::execute`：一次性执行入口；
- `ProcessSandboxSessionRequest` / `ProcessSandbox::start`：启动受管进程并等待首批输出，进程未退出时返回 session id；
- `ProcessSandboxWriteRequest` / `ProcessSandbox::write`：写 stdin、空写轮询、关闭 stdin 或终止会话；
- `ProcessSandboxSessionOutput`：本次读取到的有界合并输出、截断标记、exit code、duration 和可选 session id。

`HostProcessSandbox` 使用每会话 supervisor task 独占 `Child` 和 stdin，通过有界 channel 接收 stdout/stderr；adapter clone 共享 session store，因此 `exec_command` 与 `write_stdin` 必须注入同一实例。stdin close 会释放 pipe handle；取消、超时和显式 terminate 会终止独立进程组并等待直接子进程退出；已结束条目在报告最终状态后移除。会话不存在、已结束、stdin 已关闭、超时、取消、spawn/I/O 失败都映射为不含原始宿主错误的稳定 code/message。

Host adapter 只继承启动时捕获的 `PATH`，其他环境变量全部清空；输出 reader 使用有界 channel，每次响应也按 budget 截断。它没有 mount/network/syscall/resource 隔离，无法阻止命令读取 workspace 之外的宿主路径，因此 descriptor 必须继续声明 `isolation=none`、`filesystem_isolated=false`、`network_isolated=false`、`resource_limited=false`。审批降低误用风险，但不是隔离边界。

强隔离 adapter 必须满足：

1. workspace 以最小权限挂载，默认只读；只有批准的写目录可写；
2. 默认断网，网络按 hostname/port capability 单独开放；
3. 使用非 root uid，禁止提权、新 namespace 和危险 syscall；
4. 限制 CPU、内存、PID、文件大小、磁盘、wall time 和输出；
5. 只注入 allow-list 环境变量，不继承 API Key；
6. 取消时终止整个进程树；
7. descriptor 与策略版本进入 Run manifest，不能把 Host adapter 冒充强隔离；
8. sandbox 启动失败必须 fail closed，不能自动降级到宿主执行。

## 4. 文件工具

| 工具 | 语义 | 风险 |
|---|---|---|
| `read` | 读取 workspace 内 UTF-8 文件，最大 1 MiB | Low |
| `write` | 创建或完整覆盖文件；父目录必须存在 | Medium |
| `edit` | 精确 old/new 文本替换；默认要求唯一匹配 | Medium |
| `apply_patch` | 解析 Codex-style patch 后添加、更新或删除 UTF-8 文件；函数参数为 `{patch}` | Medium |

四个写工具共享 Workspace path policy：拒绝绝对路径、`..`、逃逸 symlink、`.git`、`config/mina.toml`、`.env*`、`.pem` 和 `.key`。`write/edit/apply_patch` 不写入 symlink 目标。`apply_patch` 先解析全部操作并解析所有目标路径，再提交文件变化；它不调用 shell。Medium 工具仍必须经过已有用户审批状态机。

## 5. Terminal tools

| 工具 | 语义 | 风险 |
|---|---|---|
| `shell_command` | `/bin/sh -c` 一次性执行，支持 workspace-relative workdir 与 1–300000 ms timeout | High |
| `exec_command` | plain-pipe shell 会话；首批等待后返回输出或 session id，可选总 timeout | High |
| `write_stdin` | 写字符、轮询、close stdin 或 terminate；返回本次新增的有界输出 | High |

`exec_command` 参考 Codex unified exec 的 `cmd/workdir/shell/yield_time_ms/max_output_tokens` 形状，但 Mina 当前不提供 PTY，输出 budget 用保守的 4 bytes/token 上限换算并受 adapter hard cap 约束。`write_stdin` 额外声明 `close_stdin` 与 `terminate`，让非 PTY 会话无需依赖控制字符表达生命周期。definition 的字段、默认值、范围与 runtime serde 校验保持一致。

这些工具只在 `BuiltinToolCatalog::enable_terminal_tools()` 后出现。`request_permissions` 不在 catalog 中：Mina 没有 Codex attached-environment permission profile 的 turn/session grant contract，terminal 权限统一由 AgentLoop 的 `ApprovalPort` 处理，High risk 不可由模型参数降级。

## 6. SearchBackend

`search` 是稳定 Tool，搜索来源由 `SearchBackend` 决定：

```text
search Tool -> SearchBackend
               ├─ WorkspaceSearchBackend  当前默认，不联网
               ├─ HTTP search adapter      Brave/Tavily/SearXNG 等
               ├─ MCP search adapter
               └─ enterprise index adapter
```

统一结果是 `title + uri + snippet + score + metadata`。Backend descriptor 声明是否访问外部网络；本地 backend 默认为 Low，外部网络 backend 至少应声明 Medium，因为 query 会离开本机并可能携带敏感上下文。供应商认证、限流、重试、缓存和响应规范化属于 adapter，不进入 AgentLoop。

默认 `WorkspaceSearchBackend`：

- 扫描 workspace UTF-8 文本，不启动外部进程；
- 跳过 symlink、`target`、`node_modules`、`.next` 和受保护路径；
- 单文件最大 1 MiB、最多扫描 10,000 个文件、最多返回 100 条；
- 支持 Run cancellation；
- 返回相对路径、行号和截断后的命中行。

## 7. 下一步实现顺序

1. Linux `BubblewrapProcessSandbox`：默认断网、只读 root、workspace 分区写权限、PID namespace、rlimit/cgroup，并完整实现 execute/start/write 会话契约；
2. 把 sandbox 选择、网络策略和资源预算加入 TOML；
3. 将 sandbox descriptor/预算写入 execution manifest 和 tool events；
4. 实现一个 HTTP SearchBackend adapter，并加入 query egress policy；
5. 为 macOS 开发提供 Lima worker，而不是依赖已废弃的 `sandbox-exec`；
6. 多租户时增加 gVisor 或 Firecracker worker，并用 lease/cancellation 与 Flow Runtime 对接。
