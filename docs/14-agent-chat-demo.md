# agent-chat/1 Demo Server

当前独立 HTTP 示例使用 `Harness<GraphAgent>` → `GraphEngine` → Context / Model / Gate / Tools → `AgentChatAdapter` → SSE。默认 demo.yaml 装配 Context、Model、Gate、ToolPolicy 和 Tools；不再使用 AgentLoop 或固定流水线 Coordinator。

Demo 使用 MemoryFabric，尚未提供跨进程恢复。CLI 和正式 server 也已接入静态图；正式 server 使用持久化 Fabric、AgentEngine 和 RunStore。

## 启动

已有 `config/agent-chat-demo.toml` 可继续使用；没有时复制示例并编辑：

```sh
cp config/agent-chat-demo.example.toml config/agent-chat-demo.toml
export OPENAI_API_KEY='your-api-key'
cargo run -p mina-server --example agent_chat_demo -- config/agent-chat-demo.toml
```

配置项：`graph`（可选 YAML/JSON 路径，相对启动目录）、`listen`、`system_prompt`、`model.model`、`model.max_output_tokens`、
`model.provider.base_url`、`model.provider.api_key`。API key 支持字符串或 `{ env = "OPENAI_API_KEY" }`。
Base URL 包含 API 前缀（例如 `/v1/`），不包含 `/chat/completions`；`protocol` 可为
`chat-completions` 或 `responses`。本地配置已被 Git 忽略，示例使用占位值。
默认加载 [config/agents/demo.yaml](../config/agents/demo.yaml)，示例与本地 TOML 通过 `graph = "config/agents/demo.yaml"` 显式指定。图从磁盘读取，不再按工具配置在代码中选择内置模板。节点、订阅、16 轮预算、工具超时和审批阈值均由 YAML 决定，修改后重启服务生效。宿主仅注入 primary 模型、workspace 工具集，以及 TOML 的 system_prompt、model.model 和 max_output_tokens。未启用 workspace 或生图时，工具集可以为空。

默认监听 `127.0.0.1:8081`，没有鉴权，供本地开发使用。浏览器跨域通过前端开发代理接入。

## 对话

```sh
curl -N http://127.0.0.1:8081/chat \
  -H 'Content-Type: application/json' \
  -d '{"protocol":"agent-chat/1","conversationId":"demo","turnId":"turn-1","runId":"run-1","input":[{"type":"message","inputId":"input-1","parts":[{"type":"text","text":"你好"}]}]}'
```

下一轮复用 conversationId，更换 turnId/runId/inputId，只发送新增消息。服务按会话保存
成功完成（stop）的用户消息、附件引用和助手回复；失败、取消和断连未完成的轮次不加入历史。
不同会话隔离，同一会话运行中返回 `409 conversation_busy`。历史保存在内存，重启清空；
不做上下文压缩，同时受上游模型上下文窗口及静态图事件大小限制。客户端 messages 快照不会覆盖服务端历史。

- `GET /health`：健康检查。
- `POST /chat`：server-hello 开头的 SSE；eventId 是字符串且与 SSE id 一致；失败以唯一 error 结束。
- `DELETE /turns/{turnId}`：幂等取消；仍连接的流将收到取消终态。断连立即取消，无宽限期。
- 同一 turnId 只接收一次，重复返回 409；不是完整 inputId 重放实现。
- `Last-Event-ID` 返回 409 resume_expired，未知协议版本返回 426。
- 整个 JSON 请求体上限 **50 MiB（52,428,800 字节）**，超过返回 413。运行超时 300 秒。

## 附件

在 parts 中加入 ChatUIComponent 默认 inline 上传格式，可与 text 混合或单独发送：

```json
{"type":"file","url":"data:image/png;base64,...","mediaType":"image/png","name":"photo.png"}
```

最多 10 个附件，单个解码后最多 10 MiB；请求总大小包含 base64 膨胀。支持 PNG/JPEG/GIF/WebP、
PDF、UTF-8 text/plain、text/csv、text/markdown。校验 base64、文件签名或 UTF-8；签名校验
不等于完整文件解码。无效输入返回 400 validation，未知 part/input 类型由 JSON 解析拒绝。
上游模型必须支持对应附件类型。

仅支持 data URL，不支持远程 URL、浏览器 blob URL 或协议 ref/inline envelope。
文件保存在 `data/agent-chat-demo-blobs`，停止服务后可手动清理，demo 不自动删除。

## 文件与终端工具

配置工作目录后，demo 复用 Extension 的 `BuiltinToolCatalog` 注册工具。相对路径以启动目录为基准；目录不存在时自动创建。示例配置为：

```toml
[workspace]
root = "data/agent-chat-demo-workspace"
terminal = true
```

| 工具 | 用途 |
| --- | --- |
| `list_directory` | 列出目录的直接子项，省略 path 表示根目录 |
| `read` | 读取 UTF-8 文件，单文件上限 1 MiB |
| `write` | 创建或覆盖文件，父目录需已存在 |
| `edit` | 精确替换文件内容 |
| `search` | 在工作目录中搜索文本 |
| `get_current_time` | 查询当前时间 |
| `shell_command` | 执行命令并返回 stdout、stderr 和退出状态 |
| `exec_command` / `write_stdin` | 启动命令，继续写入或读取运行中的会话；不支持 PTY |
| `apply_patch` | 应用文件补丁 |

`terminal = false` 或省略该字段时，不注册 shell_command、exec_command、write_stdin 和 apply_patch。删除整个 workspace 配置可关闭此工具组。工具与生图配置彼此独立，不需要图片服务密钥就能测试文件和终端。

demo.yaml 将 ToolPolicy 和 Tools 的 review_level 显式设为 100，直接执行工具，不等待人工审批；工具本身的风险等级保持不变。demo 没有审批 UI，请仅在本地使用。文件工具校验工作目录边界；终端使用 `HostProcessSandbox`，固定 cwd、清空继承环境并限制输出，但不提供系统级隔离，shell 命令仍可能访问工作目录外的文件。

可直接在聊天中测试：

> 列出当前目录，创建 hello.txt，内容为 hello mina；读取确认，然后通过终端追加一行 terminal works，最后再次读取并告诉我结果。

测试文件可在配置的 root 中查看。所有 demo 会话共用此工作目录，文件在服务重启后保留，不会自动清理。若显式指定 graph，需选择含 agent.tools 的工具图；不应继续使用 text.yaml。

本地回归使用模拟模型发出真实工具调用，验证目录列表、写入、读取、shell_command、exec_command 和再次读取的完整图循环，并检查文件工具拒绝越界读取。write_stdin 由 Extension 的终端工具测试覆盖。

## 图片生成

示例配置已包含 `[image_generation]`。将 `model` 改为你的服务支持的 GPT Image 兼容模型，并设置 provider 的地址与密钥。生图与对话模型可使用不同服务。未配置该节时不暴露生图工具；启用时图必须包含 `agent.tools`，纯文本图会在启动时报错。

Agent 可调用 `generate_image({"prompt":"一只水彩风格的蓝色小鸟"})`。工具调用 `POST /v1/images/generations`，固定生成一张 PNG，size/quality 为 auto。模型只能提供提示词，不能覆盖密钥、服务地址或生图模型。当前不支持图片编辑、遮罩或仅返回 URL 的旧版图片服务。

图片保存到 `data/agent-chat-demo-blobs`，工具结果只包含 blob_id、url、media_type 和 size_bytes，二进制和 base64 不进入模型对话或 checkpoint。模型可用 `![描述](/images/{id})` 展示结果；`GET /images/{id}` 返回保存的 PNG。前端开发代理除 `/chat`、`/turns` 外，还需将 `/images/*` 转发到 demo，否则图片链接会指向前端服务。此接口与 demo 一样没有鉴权，仅用于本地开发。

工具默认超时 180 秒，允许配置 1–240000 毫秒，demo.yaml 的工具节点预算为 241 秒，换用自定义图时需自行协调；整轮仍受 300 秒总预算约束。取消会中止本地请求，但不能保证上游已开始的生图停止计费。生成不是幂等操作，不会自动重试。JSON 响应上限 24 MiB，解码图片上限 16 MiB，检查 PNG 签名；不做完整图像解码。错误响应正文不回传。

接口依据：[OpenAI 图片生成指南](https://developers.openai.com/api/docs/guides/image-generation)。本地 mock 测试覆盖模型选择工具 → 图片生成 → 保存 → 模型返回 Markdown → HTTP 读取图片，以及错误脱敏、参数限制、响应大小限制、超时和取消；不代表真实服务可用性或浏览器渲染已验证。

## 范围与验证

默认纯文本图没有工具；配置 `workspace` 后注册文件/终端工具，配置 `image_generation` 后额外注册 `generate_image`。没有权限审批、A2UI、续传、认证、跨进程恢复或历史持久化。最多登记 10,000 个 turn
及会话，满后返回 503，重启清空。上游错误内容不直接回传，避免泄露配置。

```sh
cargo test -p mina-server --example agent_chat_demo
cargo clippy -p mina-server --example agent_chat_demo -- -D warnings
```

测试用本地模拟模型验证 Operator SSE、多轮上下文与附件、会话隔离和并发锁、重复请求、
错误终态、取消端点及 50 MiB HTTP 边界；不需要真实 API key。

## 流式输出排查

GraphAgent 通过 GraphHost 追加公开输出日志，再按序拉取给 SSE，不等待模型整轮结束。公开片段以追加日志保存，模型完成后的 stop.*、节点状态和 token 用量由 GraphEngine 提交。Demo 的日志存储仍是 MemoryFabric。`message-end.usage` 使用 Run 累计值。

服务发送 `Cache-Control: no-cache, no-transform` 和 `X-Accel-Buffering: no`；其他代理仍需自行关闭缓冲/压缩。

逐片回归测试会暂停上游，直到 HTTP 客户端收到当前片段才允许继续，防止误把“整段响应完成后
一次读取”当成流式验证：

```sh
cargo test -p mina-server --example agent_chat_demo streams_each_chunk_before_upstream_can_finish -- --nocapture
```

该测试只覆盖本地模拟上游到 demo HTTP 客户端，不涵盖真实模型服务、反向代理或浏览器渲染。

诊断日志默认追加到 `data/agent-chat-demo-debug.log`，启动时打印日志位置。
默认启用 demo debug 和 Core info，可通过 RUST_LOG 调整。
日志不记录消息正文、文件内容、API key 或完整模型请求。
`request_accepted` 将客户端 runId/turnId 与内部 run_id 关联；`upstream_event` 记录
上游事件类型、字节数、间隔和累计时间；`upstream_wait` 每 5 秒显示无事件等待；
`sse_yield` 表示事件交给 HTTP 响应流（不代表浏览器已接收），`request_terminal` 表示终态。
比较这些时间点可区分上游等待与内部投递延迟。日志不轮转，长期运行需自行清理。

### 2026-10-24 卡顿排查

真实附件复现中，SSE 推理片段间隔逐渐增大到约 20/24/34/39 秒，最终约 120 秒触发
model_timeout。进程采样 CPU 约 98%，热点落在 MemoryFabric 全状态复制，以及 10 ms
控制轮询反复执行的读取/订阅中。早先仅 3 片的测试不足以覆盖随事件记录增长出现的退化。

修复：只读及先完整校验再无失败修改的命令直接执行；事务快照共享不可变检查点（Arc），
仍保留失败回滚；控制轮询跳过已错过的 tick。逐片测试扩展到 96 片，约 3.9 秒完成。
此时间为本地模拟结果，不代表真实模型的生成速度。

如果环境已经设置 RUST_LOG=warn，会覆盖默认诊断级别；明确启动为：

```sh
RUST_LOG=agent_chat_demo=debug,agent_core=info cargo run -p mina-server --example agent_chat_demo -- config/agent-chat-demo.toml
```


## 内存流与检查点

生成中的片段先写公开日志再推送，算子返回时原子提交状态、内部事件和用量。Demo 使用内存存储，进程中断会丢失全部内存记录；demo 不承诺跨重启续传。断开响应流会取消 Run 并停止其模型 worker，取消状态写入内存 checkpoint。

图格式、状态边界与 Gate 协议见 [静态图指南](design/agent/static-agent-graph.md)。

## 自定义图节点

修改 [demo.yaml](../config/agents/demo.yaml) 可调整现有节点和订阅。增加自定义类型时，需要先在 Rust 宿主注册，再传给 GraphAgent；当前 demo 默认仅注册内置节点。完整步骤与可复制示例见 [自定义节点指南](design/agent/custom-graph-nodes.md)。

## 共享上下文准备与 Skill

可选配置：

```toml
[context]
skill_directory = "skills"
```

启用后，demo 使用 Core 的 ContextPreparation 和 GraphAgent::with_context_preparation，与正式 Server 的持久化接入共用 Skill 选择、指令编译、工具授权和上下文预算实现。目录中的 Skill 使用 skill.toml + SKILL.md 格式。匹配的 Skill 可以收紧工具列表，不会扩大宿主已注册权限。

demo 的历史来自当前内存会话，作为 prior_messages 进入预算流程；正式 Server 可以读取 SessionStore。demo 配置为最多 8 个 Skill、65536 token 上下文预算、4096 Skill token、16000 历史 token，输出预留沿用模型设置；Memory 检索关闭。没有 context 配置时保持原有直接图运行行为。Skill 准备失败不会调用模型。
