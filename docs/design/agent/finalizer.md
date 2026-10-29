# Finalizer 算子

`agent.finalizer@1` 为轮次耗尽后的最终答复提供一次独立、禁用工具的模型调用。
默认保持耗尽即失败；Context 配置 `on_max_steps: finalize` 时启用。
`max_steps` 仍然计正常模型轮次，finalize 额外调用一次，不增加工具执行机会。

```rust
use agent_core::agent::{GraphAgentConfig, OnMaxSteps};

let mut config = GraphAgentConfig::new("model-name", "system prompt");
config.max_steps = 8;
config.on_max_steps = OnMaxSteps::Finalize;
config.finalizer.max_output_tokens = 2048;
config.finalizer.timeout_ms = 30_000;
let definition = config.definition()?;
```

已有的标准 Context/Model 图可在绑定前调用
`definition.enable_finalization(FinalizerConfig::default())`。
配置文件示例见 `config/agents/tools-finalize.yaml`。

## 事件与状态

最后一轮 Gate 收齐工具结果后，Context 保存完整工具批次及图片引用，
设置 `finalizing`，添加收尾指令。启用 Compactor 时，仍执行
`context.prepare.requested.v1 → context.prepared.v1`，持久化处理后的消息后，
发布 `finalize.requested.v1`；否则直接发布该请求。
收尾提示词在压缩前计入上下文预算，压缩保留此系统消息。
Compactor 的 `reserved_output_tokens` 必须同时覆盖 Model 和 Finalizer 的输出上限。

Finalizer 使用独立配置的 `max_output_tokens` 和 `timeout_ms`，强制清空 tools，
复用 Model 的流读取、输出容量检查、错误分类与用量统计。
若模型仍返回工具调用，返回 `tools_disabled`，不执行工具、不重试收尾。
正常 Model 的 `stop.completed.v1`、`stop.failed.v1`、`stop.cancelled.v1`
也转交 Finalizer，生命周期终止出口统一为 Finalizer；转发不再次调用模型或累计用量。

收尾结果保留 `termination_reason: max_steps_exceeded` 和 `finalized: true`。
其中 `finalized` 表示经过收尾阶段，最终是否成功由终止事件类型决定。
Completed 表示最终答复生成成功，不表示原任务全部完成；提示词要求明确说明未完成事项。
失败、输出截断或超时仍按失败终止，不再进入工具循环。

## 恢复与限制

Context 持久化 `finalizing` 和准备请求关联信息，忽略重复工具完成事件及过期准备结果。
Finalizer 持久化 `done`，完成后的重复请求不再调用模型。
算子标记 `replay_safe: false`：模型调用中断且未提交结果时，沿用引擎的
`interrupted_operator` 处理，不盲目重放外部调用。

收尾仍受全局截止时间、取消及事件容量限制；全局超时或取消后不保证再有一次模型调用。
此版本仅对正常轮次耗尽触发收尾，不为模型错误、取消或超时自动触发收尾。
