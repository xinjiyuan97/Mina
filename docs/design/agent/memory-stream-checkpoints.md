# 内存流与持久化检查点

> 历史设计说明：本文涉及的固定流水线已移除。当前实现、持久化事件与恢复 API 以[静态图指南](static-agent-graph.md)和[单 Agent API](operator-only-api.md)为准。

状态：默认 Operator/tools 路径已实现。本文替代逐片段持久化作为默认方案；追加日志与独立批次仍用于显式兼容模式。

## 默认行为

模型生成期间，Core 的 LiveStreams 在内存维护 reducer、尚未提交的公开事件和本轮 journal。每个片段直接推进内存状态并唤醒客户端，不写输出 batch、输出 checkpoint、公开事件日志或 journal blob。片段顺序和 invocation 身份必须连续匹配；每轮继续受 MAX_OUTPUT_BYTES / MAX_DELTAS 上限约束。

生成中的状态使用每个 run 独立的锁，注册表锁只用于短暂查找。模型一轮结束（包括正常的模型失败）时，先追加本次尚未提交的 journal 片段，再通过带租约校验的原子事务提交输出水位、reducer 检查点和新增公开事件；随后沿原有模型结果与 seal 流程完成本轮。事务失败不能提前确认模型 delivery。日志事件序号在内存展示和提交后保持一致。

取消和运行超时在消费控制事件前提交内存前缀，再通过既有 coordinator 持久化终止状态。工具调度、授权、审批、等待及外部副作用的可靠提交顺序保持不变。内存片段不能自行调度新的 effect；工具调用仍在模型完成后的可靠边界启动。

默认在模型轮次结束、取消/超时等上述边界提交生成状态；未提交公开事件序列化大小累计达到 1 MiB 时，也触发中间检查点，避免结束时一次提交超过存储限制。没有按时间自动触发检查点。控制通道的订阅、租约和回执仍按原机制持久化，它们不属于 token 流状态。

## 读取与恢复

- 便利流与 AgentEngine 的实时转发通过 runtime 读取已提交日志和内存尾部。
- events_after 在当前进程可读取内存尾部；外部 RunStore 的投影只包含已提交的事件。
- 进程中断前已经展示但未提交的片段可以丢失。这是明确接受的行为；预览事件序号不代表已经持久化。
- 重启恢复以已保存的检查点为准；原调用标记中断，不盲目重新调用模型，也不保证原连接的 token 续传。客户端应以恢复后的运行状态重新加载历史，不能把崩溃前的预览游标视为持久确认。
- 结束前已经提交的 journal 可沿现有模型恢复路径核对，防止重复应用已提交输出。
- release_run 会释放内存状态；正常轮次结束也会移除对应内存尾部。

## 后端边界和兼容性

本机制在 Core 实现，不依赖 SQLite。外部持久化仍通过 OperatorFabric / BlobStore / RunStore 接口注入；SQLite 的具体实现继续位于 Extension。

`ToolsPipelineRuntime::new` 默认使用内存流。显式调用 `with_durable_streaming()` 可启用以前的逐批持久化路径，其原有回放和批次恢复测试继续保留。

SQLite 已改为按记录增量持久化，详见 [SQLite 增量存储](sqlite-incremental-storage.md)。当前不实现历史 GC。默认路径由内存流端单独持有本轮 journal；模型端只维护发送计数和输出限额，结束时接收 journal 的所有权并核对片段数量，不复制片段。显式 durable 兼容模式仍保留模型端 journal。reducer、公开事件和序列化缓冲仍有各自的内存开销，不能据此认为所有内容都只保存一份。检查点、日志提交和 blob 的现有大小限制仍然有效。

## 重连接口

`ToolsPipelineRuntime::resume_events(tenant, run_id, cursor, limit)` 和 `AgentEngine::resume_events(run_id, cursor, limit)` 返回 `StreamPage`，单页上限为 128。

- `events`：本页事件；`cursor.after` 是已读取的位置。
- `cursor.durable_through`：本页游标范围内已经持久化的位置，不代表客户端尚未读取的完整存储水位。
- `cursor.stream_id`：内存流实例标识；用于识别重启、释放或切换模型轮次后失效的预览。
- `reset`：为 true 时，调用方必须先清空已显示历史，再应用本页，并继续使用返回的 cursor 分页。重载从序号 0 开始，不会偷偷忽略游标差异。

客户端应将整份 cursor 与已显示事件一起保存，首次读取传 None。同一内存流内可以继续读取预览；预览对应的流实例消失后采用保守的全量重载，即使该预览后来恰好已被提交。已经确认持久化的游标可以跨实例继续使用。不同 run 或 tenant 的游标会被拒绝。

仅比较事件序号不够：恢复后的终止事件可能复用已丢失预览的序号。本接口用流实例身份避免漏读这类事件。旧 `events_after` 数字游标接口保留兼容，不具备这项重连保证。

这是 Core/Engine 的恢复能力。demo 的 HTTP 协议目前仍明确返回 `409 resume_expired` 拒绝 Last-Event-ID，并在断开时取消执行；没有将 demo 宣称为支持续传。

## 回滚只到最后成功的 checkpoint

生成期间原地推进内存状态，不为每个增量创建回滚副本。提交 checkpoint 时直接序列化状态和待提交事件，不先克隆整个 EffectRun。journal 只编码上次 checkpoint 之后的片段，摘要增量累计。每个流使用独立提交锁串行提交；序列化完成后释放预览读取锁，存储 IO 期间预览仍可读取。只有原子事务成功后才推进内存中的已提交水位和版本。

blob 写入或 checkpoint 事务失败时，丢弃该内存状态及 journal，读取回到最后成功的 checkpoint。失败提交不会确认模型 delivery。后续租约恢复读取已提交的 journal 前缀，将原模型调用标记为中断；不重新调用模型，也不把未提交尾部补写成成功输出。客户端旧预览通过 resume_events 的 reset 重载。

本策略没有取消 Fabric 事务的原子性保护，也不撤销已发生的工具副作用；工具仍依赖持久回执、幂等和租约。失败提交产生的孤立 blob 仍需后续 GC 清理。


## 运行观测

`ToolsPipelineRuntime::metrics().await` 返回不含内容的指标快照：Fabric 操作次数/错误/耗时、fencing 次数（含租约和 checkpoint 版本冲突）、领取数/空领取数/事件等待时间、checkpoint 次数/失败/耗时/新增 journal 字节、delta 发送次数/失败/等待时间，以及当前活跃流、journal 字节和未提交事件字节。

计数为同一个 runtime 及其 clone 的累计值；时长单位在字段名中标注。checkpoint 和发送等待计时器在 future 被取消时也记录失败。Fabric 操作计数覆盖完成的调用。事件等待按调用方逻辑时钟计算，包含重试延迟；并非单纯数据库排队耗时。缓冲 gauge 逐个读取各流，不是所有流的原子快照。指标不包含 prompt、输出内容或凭据。
