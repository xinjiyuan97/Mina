# 单 Agent 请求前压缩

`agent.compactor@1` 与 `agent.context@3` 配合，在每一次业务模型请求之前检查完整输入预算。默认图保持原样，通过配置显式启用。可运行配置见 [tools-compaction.yaml](../../../config/agents/tools-compaction.yaml)；其中窗口大小是示例，必须改为实际模型容量。

```text
Start / Gate.completed → Context → Compactor → Context → Model
                            请求准备       写回消息后请求模型
```

## 接入

文件图将 Context 版本设为 3，添加 Compactor，并增加两个订阅：

- Context → Compactor：`context.prepare.requested.v1`。
- Compactor → Context：`context.prepared.v1`。

原 Context → Model 的 `model.requested.v1` / `context.failed.v1` 订阅不变。终态仍由 Model 发布。Context v1/v2 不变。Compactor 使用标准 Model 资源绑定，可以与业务模型共用资源，也可以绑定独立摘要模型。宿主策略对摘要模型同样执行允许列表、输出上限和超时约束。

Rust 默认工具图可以设置 `GraphAgentConfig.compaction`，或在绑定前对具有 `context`、`llm` 节点的默认图调用：

```rust,ignore
use agent_core::agent::CompactorConfig;

let mut compaction = CompactorConfig::new("primary", 128_000, 4_096, 128_000);
compaction.model_name = Some("host-approved-model-name".into());
definition.enable_compaction(compaction)?;
```

四个参数依次是摘要模型资源名、业务模型窗口、业务模型输出预留、摘要模型窗口。辅助方法为尚未设置输出上限的业务模型设置预留值。构建图时要求下游业务模型具有显式输出上限，且不能超过预留。

## 预算与内容

`B = context_window_tokens - reserved_output_tokens - safety_margin_tokens`。预计输入超过 `80% × B` 时尝试压缩，目标为 `60% × B`。可配置触发比例、目标比例和最近保留块数（默认 2）。估算包含序列化消息、工具定义、消息格式开销及保守余量，复用现有 HeuristicTokenEstimator，并非精确的供应商 tokenizer。

系统消息、当前 Run 的用户请求、带附件的消息及最近完整交互块保留。assistant 的一批工具调用及全部 tool results 组成不可拆分块，其余消息各自为块。即使未触发压缩，也检查工具历史是否完整；未结束、重复或孤立的工具结果不会送入业务模型。

旧历史（包括上一份摘要）生成一份滚动摘要，记录目标、约束、已完成工作、依据、失败尝试、待办和必要产物引用。摘要以带有“不可信派生上下文”标记的 user 消息注入，不变成 system 指令。工具定义与授权快照不由摘要模型生成或修改。原始日志不删除；当前消息检查点会替换成压缩结果。后续请求不会重新追加旧历史。

附件不能仅按 blob 引用估算；有附件时必须配置 `attachment_tokens`（每个附件的保守 token 预留），否则返回 `attachment_budget_required`。这个估计需由宿主根据实际媒体输入确定。首版不支持任意消息的自定义保留标记，也不压缩附件。

## 有界调用与降级

每次准备最多调用一次摘要模型，禁用工具，限制摘要输入窗口、输出 token、流输出字节及调用时长。摘要模型的输出还要经过业务请求预算复查。不会在超大历史上递归调用摘要模型。

摘要调用失败、超时、提前截断、输入放不下或摘要未达到目标时：

1. 原请求仍在 B 内：保留原文继续，不在本轮重试。
2. 原请求超过 B：默认失败，代码为 `context_budget_exceeded`。
3. 显式配置 `fallback: truncate`：允许删除符合压缩条件的完整旧历史块；必须保留部分和最近块仍不删除，且结果必须在 B 内。

若必须保留部分本身超预算，即使允许裁剪也失败。运行取消立即中止等待，沿用 GraphEngine 的取消和总截止时间。摘要的实际 token 用量通过 GraphOutput 计入 Run 总用量，不增加业务 round / max_steps。

## 持久化与恢复

Context 持久化待准备请求，包含 Run/节点/round 派生的 prepare_id、原始请求指纹、消息、当前用户索引、工具定义和授权快照。round 是单调上下文代次。Compactor 返回关联 ID、源指纹、消息、索引以及压缩前后估算值和被替换消息索引；失败结果带 code。

Context 只接受当前待处理请求对应的结果，校验当前用户请求原文，然后在同一图事务内写回消息、清除 pending 并发布模型请求。重复、过期结果和已消费工具批次不重复推进 round。摘要内容不作为业务模型输出投影到用户回答。

成功提交的摘要和事件沿用 Fabric 事务恢复。Compactor 声明 `replay_safe=false`，中断的未提交摘要调用沿用现有 `interrupted_operator` 恢复语义，不自动再次计费调用。原始事实日志仍保留完整历史，因此上下文压缩不等于存储回收。

## 验证

`builtins::compactor::tests` 验证直通、摘要与用量、预算溢出、显式裁剪、超时、取消及工具批次完整性。Context 测试验证待处理状态恢复、过期/重复结果和重复工具批次。`static_agent_graph` 集成测试验证完整工具循环中的消息回写、后续请求复用、终态恢复和配置预算校验。
