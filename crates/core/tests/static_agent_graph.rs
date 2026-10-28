use agent_core::{
    agent::{
        AgentBuilder, AgentDefinition, AgentGraph, AgentResources, GraphContext, GraphEvent,
        GraphLifecycle, GraphLimits, GraphOperator, GraphOutput, GraphStatus, OperatorDefinition,
        OperatorDescriptor, OperatorRegistry,
        graph::{GraphFuture, ResourceKind},
        memory_fabric::MemoryFabric,
    },
    contracts::*,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

fn test_usage(input_tokens: u64, output_tokens: u64) -> TokenUsage {
    TokenUsage {
        input_tokens,
        output_tokens,
        total_tokens: input_tokens + output_tokens,
        source: TokenUsageSource::ProviderReported,
    }
}
struct Text(Arc<AtomicUsize>);
impl ModelPort for Text {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures_util::stream::iter([
            ModelEvent::Usage {
                usage: test_usage(1, 1),
            },
            ModelEvent::TextDelta {
                delta: "hello".into(),
            },
            ModelEvent::Usage {
                usage: test_usage(10, 5),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}
async fn terminal(
    agent: &agent_core::agent::ConfiguredAgent,
    run: RunId,
) -> agent_core::agent::GraphRun {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = agent.engine.get_run("local", run).await.unwrap();
            if state.is_terminal() {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
fn text_definition() -> AgentDefinition {
    AgentDefinition::from_yaml(include_str!("../../../config/agents/text.yaml")).unwrap()
}
fn text_resources(calls: Arc<AtomicUsize>) -> AgentResources {
    AgentResources::new(Arc::new(MemoryFabric::default())).model(
        "primary",
        "provider-v1",
        Arc::new(Text(calls)),
    )
}
#[tokio::test]
async fn yaml_json_and_code_build_the_same_agent_and_isolate_runs() {
    let yaml = text_definition();
    let code = AgentGraph::new("text-assistant")
        .trigger(
            "start",
            agent_core::agent::TriggerDefinition::manual("agent.start.v1"),
        )
        .operator(
            "context",
            OperatorDefinition::new(
                "agent.context",
                1,
                json!({"system_prompt":"你是一个简洁、准确的助手。","max_steps":8}),
            ),
        )
        .operator(
            "llm",
            OperatorDefinition::new(
                "agent.model",
                1,
                json!({"model":"primary","timeout_ms":60000}),
            ),
        )
        .subscribe("start-agent", "start", "context", ["agent.start.v1"])
        .subscribe("request-model", "context", "llm", ["model.requested.v1"])
        .subscribe("context-failure", "context", "llm", ["context.failed.v1"])
        .lifecycle(GraphLifecycle::model("llm"))
        .limits(GraphLimits::new(900000, 10000))
        .finish()
        .unwrap();
    assert_eq!(yaml, code);
    assert_eq!(
        yaml,
        AgentDefinition::from_json(&code.to_json().unwrap()).unwrap()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), text_resources(calls.clone()))
        .build(code)
        .unwrap();
    let first = agent.run("one").await.unwrap();
    let second = agent.run("two").await.unwrap();
    for run in [first.run_id, second.run_id] {
        let state = terminal(&agent, run).await;
        assert_eq!(state.status, GraphStatus::Completed);
        assert_eq!(state.usage, Some(test_usage(10, 5)));
        let model_steps: Vec<_> = state
            .step_usage
            .iter()
            .filter(|s| s.instance == "llm")
            .collect();
        assert_eq!(model_steps.len(), 1);
        assert_eq!(model_steps[0].usage, state.usage);
        assert_eq!(
            state.step_usage.last().unwrap().cumulative_usage,
            state.usage
        );
        assert_eq!(state.output.as_ref().unwrap()["output"], "hello");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let events = agent
        .engine
        .events_after("local", first.run_id, 0, 128)
        .await
        .unwrap();
    assert_eq!(events[0].from, "start");
    assert_eq!(events.last().unwrap().kind, "engine.terminal.v1");
    assert_eq!(
        events.last().unwrap().payload["usage"],
        json!(test_usage(10, 5))
    );
    assert!(
        agent
            .engine
            .events_after("local", first.run_id, events.len() as u64, 128)
            .await
            .unwrap()
            .is_empty()
    );
    agent.engine.stop_workers().await;
}
#[test]
fn parser_rejects_duplicate_keys_tags_unknown_fields_and_old_format() {
    assert!(AgentDefinition::from_yaml("schema_version: 1\nschema_version: 1").is_err());
    assert!(AgentDefinition::from_json(r#"{"schema_version":1,"schema_version":1}"#).is_err());
    assert!(AgentDefinition::from_yaml("!custom tagged").is_err());
    assert!(AgentDefinition::from_yaml("1: value").is_err());
    assert!(AgentDefinition::from_json(r#"{"version":1,"run":{},"entry":"run.started"}"#).is_err());
    let mut value = serde_json::to_value(text_definition()).unwrap();
    value["subscriptinos"] = json!([]);
    assert!(AgentDefinition::from_json(&value.to_string()).is_err());
}
#[test]
fn invalid_graphs_fail_before_execution() {
    let resources = text_resources(Arc::new(AtomicUsize::new(0)));
    let registry = OperatorRegistry::builtins();
    let baseline = text_definition();
    baseline.validate(&registry, &resources).unwrap();
    let mut d = baseline.clone();
    d.subscriptions[1].from = "missing".into();
    assert!(
        d.validate(&registry, &resources)
            .unwrap_err()
            .to_string()
            .contains("subscriptions[1].from")
    );
    let mut d = baseline.clone();
    d.subscriptions[1].events = vec!["model.failed.v1".into()];
    assert!(d.validate(&registry, &resources).is_err());
    let mut d = baseline.clone();
    let mut duplicate = d.subscriptions[1].clone();
    duplicate.id = "another-id".into();
    d.subscriptions.push(duplicate);
    assert!(d.validate(&registry, &resources).is_err());
    let mut d = baseline.clone();
    d.operators.get_mut("llm").unwrap().version = 2;
    assert!(d.validate(&registry, &resources).is_err());
    let mut d = baseline.clone();
    d.operators.get_mut("llm").unwrap().config["model"] = json!("missing");
    assert!(d.validate(&registry, &resources).is_err());
    let mut d = baseline.clone();
    d.operators
        .insert("unreachable".into(), d.operators["llm"].clone());
    assert!(d.validate(&registry, &resources).is_err());
    let mut d = baseline.clone();
    d.limits.max_deliveries = 0;
    assert!(d.validate(&registry, &resources).is_err());
    let mut d = baseline;
    d.operators.get_mut("llm").unwrap().config["typo"] = json!(true);
    assert!(d.validate(&registry, &resources).is_err());
}
struct Echo;
impl GraphOperator for Echo {
    fn handle(&self, c: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            Ok(GraphOutput::new(
                Value::Null,
                vec![GraphEvent::new(
                    "echo.done.v1",
                    json!({"input":c.event.payload}),
                )],
            ))
        })
    }
}
fn echo_descriptor() -> OperatorDescriptor {
    OperatorDescriptor::new("test.echo", 1)
        .input("echo.start.v1", json!({"type":"string"}))
        .output("echo.done.v1", json!({"type":"object"}))
        .output("echo.failed.v1", json!({"type":"object"}))
        .output("echo.cancelled.v1", json!({"type":"object"}))
}
fn echo_definition() -> AgentDefinition {
    AgentGraph::new("custom-only")
        .trigger(
            "start",
            agent_core::agent::TriggerDefinition::manual("echo.start.v1"),
        )
        .operator("echo", OperatorDefinition::new("test.echo", 1, json!({})))
        .subscribe("start", "start", "echo", ["echo.start.v1"])
        .lifecycle(agent_core::agent::GraphLifecycle {
            start: agent_core::agent::GraphStart {
                trigger: "start".into(),
            },
            terminal: agent_core::agent::GraphTerminal {
                from: "echo".into(),
                completed: "echo.done.v1".into(),
                failed: "echo.failed.v1".into(),
                cancelled: "echo.cancelled.v1".into(),
            },
        })
        .limits(GraphLimits::new(10000, 100))
        .finish()
        .unwrap()
}
#[tokio::test]
async fn arbitrary_custom_graph_needs_no_builtin_roles_and_loads_from_file() {
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(Echo)))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("echo.json");
    std::fs::write(&path, echo_definition().to_json().unwrap()).unwrap();
    let agent = AgentBuilder::new(
        registry,
        AgentResources::new(Arc::new(MemoryFabric::default())),
    )
    .from_file(path)
    .unwrap();
    let run = agent.run("custom").await.unwrap();
    assert_eq!(
        terminal(&agent, run.run_id).await.output,
        Some(json!({"input":"custom"}))
    );
    agent.engine.stop_workers().await;
}
struct RoundModel(Arc<AtomicUsize>);
impl ModelPort for RoundModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        let events = if n == 0 {
            assert_eq!(request.tools[0].name, "echo");
            vec![
                ModelEvent::ToolCallStarted {
                    call_id: "c1".into(),
                    name: "echo".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "c1".into(),
                    delta: "{\"text\":\"value\"}".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]
        } else {
            assert_eq!(request.messages.last().unwrap().content, "value");
            vec![
                ModelEvent::TextDelta {
                    delta: "finished".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]
        };
        let mut events = events;
        events.insert(
            events.len() - 1,
            ModelEvent::Usage {
                usage: test_usage(10, 5),
            },
        );
        Box::pin(futures_util::stream::iter(events))
    }
}
struct EchoTool(Arc<AtomicUsize>);
impl ToolPort for EchoTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "echo",
            "echo",
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
        )]
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolOutput::text(
                request.arguments["text"].as_str().unwrap(),
            ))
        })
    }
}
#[tokio::test]
async fn configured_tool_loop_validates_and_returns_to_model() {
    let model_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let resources = AgentResources::new(Arc::new(MemoryFabric::default()))
        .model("primary", "m1", Arc::new(RoundModel(model_calls.clone())))
        .toolset("workspace", "t1", Arc::new(EchoTool(tool_calls.clone())));
    let definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap();
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(definition)
        .unwrap();
    let run = agent.run("tool").await.unwrap();
    assert_eq!(
        terminal(&agent, run.run_id).await.output.unwrap()["output"],
        "finished"
    );
    let state = terminal(&agent, run.run_id).await;
    assert_eq!(state.usage, Some(test_usage(20, 10)));
    let steps: Vec<_> = state
        .step_usage
        .iter()
        .filter(|s| s.usage.is_some())
        .collect();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].cumulative_usage, Some(test_usage(10, 5)));
    assert_eq!(steps[1].cumulative_usage, Some(test_usage(20, 10)));
    assert_eq!(model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    agent.engine.stop_workers().await;
}
#[tokio::test]
async fn sqlite_recovery_pins_definition_and_resource_contract_without_model_reexecution() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("graph.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let fabric = Arc::new(SqliteEventStore::open(&path, "static").await.unwrap());
    let resources =
        AgentResources::new(fabric).model("primary", "r1", Arc::new(Text(calls.clone())));
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(text_definition())
        .unwrap();
    let run = agent.run("saved").await.unwrap();
    terminal(&agent, run.run_id).await;
    agent.engine.stop_workers().await;
    drop(agent);
    let resources = AgentResources::new(Arc::new(
        SqliteEventStore::open(&path, "static").await.unwrap(),
    ))
    .model("primary", "r1", Arc::new(Text(calls.clone())));
    let mut changed = text_definition();
    changed.name = "new-default".into();
    changed.operators.get_mut("context").unwrap().config["system_prompt"] = json!("new prompt");
    let reopened = AgentBuilder::new(OperatorRegistry::builtins(), resources.clone())
        .build(changed)
        .unwrap();
    let recovered = reopened.engine.recover("local", run.run_id).await.unwrap();
    assert_eq!(recovered.definition.name, "text-assistant");
    assert_eq!(recovered.usage, Some(test_usage(10, 5)));
    assert_eq!(
        recovered
            .step_usage
            .iter()
            .filter(|s| s.usage.is_some())
            .count(),
        1
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    reopened.engine.stop_workers().await;
    let changed_resource = resources.model("primary", "r2", Arc::new(Text(calls.clone())));
    let mismatched = AgentBuilder::new(OperatorRegistry::builtins(), changed_resource)
        .build(text_definition())
        .unwrap();
    assert!(
        mismatched
            .engine
            .recover("local", run.run_id)
            .await
            .is_err()
    );
}
struct Pending(Arc<AtomicUsize>);
impl ModelPort for Pending {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures_util::stream::pending())
    }
}
#[tokio::test]
async fn cancellation_and_budget_are_engine_controls_without_subscriptions() {
    let calls = Arc::new(AtomicUsize::new(0));
    let resources = AgentResources::new(Arc::new(MemoryFabric::default())).model(
        "primary",
        "r1",
        Arc::new(Pending(calls.clone())),
    );
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(text_definition())
        .unwrap();
    let run = agent.run("cancel").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.cancel("local", run.run_id).await.unwrap();
    assert_eq!(
        terminal(&agent, run.run_id).await.status,
        GraphStatus::Cancelled
    );
    agent.engine.stop_workers().await;
    let mut definition = text_definition();
    definition.limits.max_deliveries = 1;
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), text_resources(calls.clone()))
        .build(definition)
        .unwrap();
    let run = agent.run("budget").await.unwrap();
    assert_eq!(
        terminal(&agent, run.run_id).await.error.as_deref(),
        Some("delivery_budget_exhausted")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    agent.engine.stop_workers().await;
}
#[test]
fn registry_rejects_ambiguous_event_schemas() {
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(Echo)))
        .unwrap();
    let other = OperatorDescriptor::new("other", 1).input("echo.done.v1", json!({"type":"string"}));
    assert!(registry.register(other, |_, _| Ok(Arc::new(Echo))).is_err());
    let mut descriptor = OperatorDescriptor::new("external", 1);
    descriptor.config_schema = json!({"$ref":"https://invalid/schema"});
    assert!(
        registry
            .register(descriptor, |_, _| Ok(Arc::new(Echo)))
            .is_err()
    );
    let _ = ResourceKind::Model;
}

struct Observer(Arc<std::sync::Mutex<Vec<String>>>);
impl GraphOperator for Observer {
    fn handle(&self, context: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            self.0.lock().unwrap().push(context.event.from);
            Ok(GraphOutput::new(json!({"seen":true}), vec![]))
        })
    }
}
#[tokio::test]
async fn subscriptions_filter_publisher_and_terminal_observers_drain() {
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(Echo)))
        .unwrap();
    let captured = seen.clone();
    registry
        .register(
            OperatorDescriptor::new("test.observer", 1)
                .input("echo.done.v1", json!({"type":"object"})),
            move |_, _| Ok(Arc::new(Observer(captured.clone()))),
        )
        .unwrap();
    let mut definition = echo_definition();
    let echo = definition.operators.remove("echo").unwrap();
    definition.operators.insert("a_other".into(), echo.clone());
    definition.operators.insert("z_main".into(), echo);
    definition.operators.insert(
        "observer".into(),
        OperatorDefinition::new("test.observer", 1, json!({})),
    );
    definition.lifecycle.terminal.from = "z_main".into();
    definition.subscriptions.clear();
    for name in ["a_other", "z_main"] {
        definition
            .subscriptions
            .push(agent_core::agent::OperatorSubscription {
                id: format!("start-{name}"),
                subscriber: name.into(),
                from: "start".into(),
                events: vec!["echo.start.v1".into()],
            });
    }
    definition
        .subscriptions
        .push(agent_core::agent::OperatorSubscription {
            id: "observe-main".into(),
            subscriber: "observer".into(),
            from: "z_main".into(),
            events: vec!["echo.done.v1".into()],
        });
    let agent = AgentBuilder::new(
        registry,
        AgentResources::new(Arc::new(MemoryFabric::default())),
    )
    .build(definition)
    .unwrap();
    let run = agent.run("x").await.unwrap();
    terminal(&agent, run.run_id).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while seen.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec!["z_main"]);
    agent.engine.stop_workers().await;
}
struct Loop;
impl GraphOperator for Loop {
    fn handle(&self, c: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let n = c.state.as_u64().unwrap_or(0) + 1;
            Ok(GraphOutput::new(
                json!(n),
                vec![GraphEvent::new("loop.tick.v1", json!({}))],
            ))
        })
    }
}
#[tokio::test]
async fn cyclic_custom_graph_stops_at_persisted_delivery_budget() {
    let mut registry = OperatorRegistry::default();
    let mut descriptor = echo_descriptor();
    descriptor.kind = "test.loop".into();
    descriptor
        .inputs
        .insert("loop.tick.v1".into(), json!({"type":"object"}));
    descriptor
        .outputs
        .insert("loop.tick.v1".into(), json!({"type":"object"}));
    descriptor.replay_safe = true;
    registry
        .register(descriptor, |_, _| Ok(Arc::new(Loop)))
        .unwrap();
    let mut definition = echo_definition();
    definition.operators.get_mut("echo").unwrap().kind = "test.loop".into();
    definition.limits.max_deliveries = 4;
    definition
        .subscriptions
        .push(agent_core::agent::OperatorSubscription {
            id: "loop".into(),
            from: "echo".into(),
            subscriber: "echo".into(),
            events: vec!["loop.tick.v1".into()],
        });
    let agent = AgentBuilder::new(
        registry,
        AgentResources::new(Arc::new(MemoryFabric::default())),
    )
    .build(definition)
    .unwrap();
    let run = agent.run("loop").await.unwrap();
    let state = terminal(&agent, run.run_id).await;
    assert_eq!(state.deliveries, 4);
    assert_eq!(state.error.as_deref(), Some("delivery_budget_exhausted"));
    agent.engine.stop_workers().await;
}
#[tokio::test]
async fn interrupted_model_is_not_reexecuted_after_recovery_and_timeout_is_durable() {
    let calls = Arc::new(AtomicUsize::new(0));
    let resources = AgentResources::new(Arc::new(MemoryFabric::default())).model(
        "primary",
        "r1",
        Arc::new(Pending(calls.clone())),
    );
    let mut definition = text_definition();
    definition.limits.timeout_ms = 250;
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources.clone())
        .build(definition.clone())
        .unwrap();
    let run = agent.run("pending").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.stop_workers().await;
    drop(agent);
    let recovered = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(definition)
        .unwrap();
    recovered.engine.recover("local", run.run_id).await.unwrap();
    let state = terminal(&recovered, run.run_id).await;
    assert_eq!(state.error.as_deref(), Some("run_timeout"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    recovered.engine.stop_workers().await;
}
struct InvalidOutput;
impl GraphOperator for InvalidOutput {
    fn handle(&self, _: GraphContext) -> GraphFuture<'_> {
        Box::pin(async {
            Ok(GraphOutput::new(
                Value::Null,
                vec![GraphEvent::new(
                    "model.requested.v1",
                    json!({"from":"brain"}),
                )],
            ))
        })
    }
}
#[tokio::test]
async fn operator_cannot_publish_undeclared_events_or_impersonate_another_node() {
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(InvalidOutput)))
        .unwrap();
    let agent = AgentBuilder::new(
        registry,
        AgentResources::new(Arc::new(MemoryFabric::default())),
    )
    .build(echo_definition())
    .unwrap();
    let run = agent.run("x").await.unwrap();
    let state = terminal(&agent, run.run_id).await;
    assert_eq!(state.error.as_deref(), Some("undeclared_output"));
    let events = agent
        .engine
        .events_after("local", run.run_id, 0, 128)
        .await
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| e.from == "brain" || e.kind == "model.requested.v1")
    );
    agent.engine.stop_workers().await;
}

struct OversizedOutput;
impl GraphOperator for OversizedOutput {
    fn handle(&self, context: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let output = match context.event.payload.as_str() {
                Some("state") => GraphOutput::new(json!("x".repeat(4 * 1024 * 1024)), vec![]),
                Some("batch") => GraphOutput::new(
                    Value::Null,
                    (0..129)
                        .map(|_| GraphEvent::new("echo.done.v1", json!({})))
                        .collect(),
                ),
                _ => GraphOutput::new(
                    Value::Null,
                    vec![GraphEvent::new(
                        "echo.done.v1",
                        json!({"text":"x".repeat(65536)}),
                    )],
                ),
            };
            Ok(output)
        })
    }
}
#[tokio::test]
async fn oversized_input_is_rejected_and_output_fails_without_waiting_for_deadline() {
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(OversizedOutput)))
        .unwrap();
    let agent = AgentBuilder::new(
        registry,
        AgentResources::new(Arc::new(MemoryFabric::default())),
    )
    .build(echo_definition())
    .unwrap();
    assert!(agent.run("x".repeat(65536)).await.is_err());
    for input in ["small", "state", "batch"] {
        let run = agent.run(input).await.unwrap();
        let result = terminal(&agent, run.run_id).await;
        assert_eq!(result.status, GraphStatus::Failed);
        assert_eq!(result.error.as_deref(), Some("output_capacity_exceeded"));
    }
    agent.engine.stop_workers().await;
}

struct UsageThenIncomplete {
    timeout: bool,
}
impl ModelPort for UsageThenIncomplete {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        use futures_util::StreamExt;
        let usage = futures_util::stream::iter([ModelEvent::Usage {
            usage: test_usage(7, 2),
        }]);
        if self.timeout {
            Box::pin(usage.chain(futures_util::stream::pending()))
        } else {
            Box::pin(usage)
        }
    }
}
#[tokio::test]
async fn failed_or_timed_out_model_preserves_reported_usage_in_checkpoint() {
    for timeout in [false, true] {
        let mut definition = text_definition();
        definition.operators.get_mut("llm").unwrap().config["timeout_ms"] = json!(20);
        let agent = AgentBuilder::new(
            OperatorRegistry::builtins(),
            AgentResources::new(Arc::new(MemoryFabric::default())).model(
                "primary",
                "v1",
                Arc::new(UsageThenIncomplete { timeout }),
            ),
        )
        .build(definition)
        .unwrap();
        let run = agent.run("test").await.unwrap();
        let state = terminal(&agent, run.run_id).await;
        assert_eq!(state.status, GraphStatus::Failed);
        assert_eq!(state.usage, Some(test_usage(7, 2)));
        assert_eq!(
            state
                .step_usage
                .iter()
                .filter(|s| s.usage.is_some())
                .count(),
            1
        );
        agent.engine.stop_workers().await;
    }
}

#[tokio::test]
async fn split_context_step_limit_stops_before_another_model_call() {
    let mut definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap();
    definition.operators.get_mut("context").unwrap().config["max_steps"] = json!(1);
    let model_calls = Arc::new(AtomicUsize::new(0));
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        AgentResources::new(Arc::new(MemoryFabric::default()))
            .model("primary", "v1", Arc::new(RoundModel(model_calls.clone())))
            .toolset(
                "workspace",
                "v1",
                Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
            ),
    )
    .build(definition)
    .unwrap();
    let run = agent.run("test").await.unwrap();
    let result = terminal(&agent, run.run_id).await;
    assert_eq!(result.status, GraphStatus::Failed);
    assert_eq!(result.output.unwrap()["code"], "max_steps_exceeded");
    assert_eq!(model_calls.load(Ordering::SeqCst), 1);
    assert!(
        OperatorRegistry::builtins()
            .describe("agent.coordinator", 1)
            .is_none()
    );
    agent.engine.stop_workers().await;
}

struct ApprovalBatch;
impl GraphOperator for ApprovalBatch {
    fn handle(&self, _: GraphContext) -> GraphFuture<'_> {
        Box::pin(async {
            Ok(GraphOutput::new(
                Value::Null,
                vec![GraphEvent::new(
                    "gate.open.v1",
                    json!({"batch_id":"approval","context":{"change":"demo"},"items":[{"id":"owner","payload":{"review":"demo"}}]}),
                )],
            ))
        })
    }
}
struct ApprovalResult;
impl GraphOperator for ApprovalResult {
    fn handle(&self, ctx: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            Ok(GraphOutput::new(
                Value::Null,
                vec![GraphEvent::new(
                    "gate.result.v1",
                    json!({"batch_id":ctx.event.payload["batch_id"],"id":ctx.event.payload["id"],"payload":{"approved":true}}),
                )],
            ))
        })
    }
}
struct ApprovalDone;
impl GraphOperator for ApprovalDone {
    fn handle(&self, ctx: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            Ok(GraphOutput::new(
                Value::Null,
                vec![GraphEvent::new("echo.done.v1", ctx.event.payload)],
            ))
        })
    }
}
#[tokio::test]
async fn generic_gate_runs_a_custom_approval_graph_without_model_or_tool_protocols() {
    let mut registry = OperatorRegistry::builtins();
    assert!(registry.describe("agent.result_router", 1).is_none());
    assert!(registry.describe("agent.tool_join", 1).is_none());
    let gate = registry.describe("agent.gate", 1).unwrap().clone();
    registry
        .register(
            OperatorDescriptor::new("test.batch", 1)
                .input("echo.start.v1", json!({"type":"string"}))
                .output("gate.open.v1", gate.inputs["gate.open.v1"].clone()),
            |_, _| Ok(Arc::new(ApprovalBatch)),
        )
        .unwrap();
    registry
        .register(
            OperatorDescriptor::new("test.approval", 1)
                .input("gate.dispatch.v1", gate.outputs["gate.dispatch.v1"].clone())
                .output("gate.result.v1", gate.inputs["gate.result.v1"].clone()),
            |_, _| Ok(Arc::new(ApprovalResult)),
        )
        .unwrap();
    let mut done = echo_descriptor();
    done.kind = "test.done".into();
    done.inputs.clear();
    done.inputs.insert(
        "gate.completed.v1".into(),
        gate.outputs["gate.completed.v1"].clone(),
    );
    registry
        .register(done, |_, _| Ok(Arc::new(ApprovalDone)))
        .unwrap();
    let definition = AgentGraph::new("approvals")
        .trigger(
            "start",
            agent_core::agent::TriggerDefinition::manual("echo.start.v1"),
        )
        .operator("begin", OperatorDefinition::new("test.batch", 1, json!({})))
        .operator("gate", OperatorDefinition::new("agent.gate", 1, json!({})))
        .operator(
            "review",
            OperatorDefinition::new("test.approval", 1, json!({})),
        )
        .operator("echo", OperatorDefinition::new("test.done", 1, json!({})))
        .subscribe("start", "start", "begin", ["echo.start.v1"])
        .subscribe("open", "begin", "gate", ["gate.open.v1"])
        .subscribe("dispatch", "gate", "review", ["gate.dispatch.v1"])
        .subscribe("result", "review", "gate", ["gate.result.v1"])
        .subscribe("done", "gate", "echo", ["gate.completed.v1"])
        .lifecycle(echo_definition().lifecycle)
        .limits(GraphLimits::new(5000, 100))
        .finish()
        .unwrap();
    let agent = AgentBuilder::new(
        registry,
        AgentResources::new(Arc::new(MemoryFabric::default())),
    )
    .build(definition)
    .unwrap();
    let run = agent.run("review").await.unwrap();
    let result = terminal(&agent, run.run_id).await;
    assert_eq!(result.status, GraphStatus::Completed);
    assert_eq!(
        result.output.unwrap()["results"][0]["payload"]["approved"],
        true
    );
    assert!(result.usage.is_none());
    agent.engine.stop_workers().await;
}

struct FinishWith(FinishReason);
impl ModelPort for FinishWith {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(futures_util::stream::iter([
            ModelEvent::TextDelta {
                delta: "partial".into(),
            },
            ModelEvent::Usage {
                usage: test_usage(3, 1),
            },
            ModelEvent::Completed {
                finish_reason: self.0,
            },
        ]))
    }
}
#[tokio::test]
async fn model_emits_stop_failures_for_truncation_filter_and_missing_tool_calls() {
    for reason in [
        FinishReason::Length,
        FinishReason::ContentFilter,
        FinishReason::ToolCall,
    ] {
        let agent = AgentBuilder::new(
            OperatorRegistry::builtins(),
            AgentResources::new(Arc::new(MemoryFabric::default())).model(
                "primary",
                "v1",
                Arc::new(FinishWith(reason)),
            ),
        )
        .build(text_definition())
        .unwrap();
        let run = agent.run("test").await.unwrap();
        let result = terminal(&agent, run.run_id).await;
        assert_eq!(result.status, GraphStatus::Failed);
        assert_eq!(result.usage, Some(test_usage(3, 1)));
        let events = agent
            .engine
            .events_after("local", run.run_id, 0, 128)
            .await
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.from == "llm" && event.kind == "stop.failed.v1")
        );
        assert!(!events.iter().any(|event| event.kind == "stop.completed.v1"));
        agent.engine.stop_workers().await;
    }
}

struct PreviewThenPending;
impl ModelPort for PreviewThenPending {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        use futures_util::StreamExt;
        Box::pin(
            futures_util::stream::iter([ModelEvent::TextDelta {
                delta: "live".into(),
            }])
            .chain(futures_util::stream::pending()),
        )
    }
}
#[tokio::test]
async fn graph_chat_stream_drop_cancels_persisted_run_and_provider() {
    use agent_core::agent::GraphAgent;
    use futures_util::StreamExt;
    let resources = AgentResources::new(Arc::new(MemoryFabric::default())).model(
        "primary",
        "v1",
        Arc::new(PreviewThenPending),
    );
    let graph = GraphAgent::new(
        text_definition(),
        OperatorRegistry::builtins(),
        resources.clone(),
    )
    .unwrap();
    let observer = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(text_definition())
        .unwrap();
    let run_id = RunId::new();
    let mut events = graph.run(RunRequest {
        run_id,
        input: "test".into(),
        attachments: vec![],
        prior_messages: vec![],
        allowed_tools: None,
        allow_run_adf: false,
        max_steps: None,
        cancellation: RunCancellation::new(),
    });
    let first = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first, AgentEvent::OutputDelta { delta, .. } if delta == "live"));
    assert_eq!(
        observer
            .engine
            .get_run("local", run_id)
            .await
            .unwrap()
            .status,
        GraphStatus::Running
    );
    drop(events);
    assert_eq!(
        terminal(&observer, run_id).await.status,
        GraphStatus::Cancelled
    );
}

struct ReviewedEcho(Arc<AtomicUsize>);
impl ToolPort for ReviewedEcho {
    fn definitions(&self) -> Vec<ToolDefinition> {
        EchoTool(self.0.clone())
            .definitions()
            .into_iter()
            .map(|d| d.with_risk_level(ToolRiskLevel::High))
            .collect()
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        EchoTool(self.0.clone()).call(request)
    }
}
async fn approval_event(agent: &agent_core::agent::ConfiguredAgent, run: RunId) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = agent
                .engine
                .events_after("local", run, 0, 128)
                .await
                .unwrap();
            if let Some(event) = events.iter().find(|e| e.kind == "approval.requested.v1") {
                return event.payload.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn durable_approval_survives_reopen_and_rejects_changed_binding_and_duplicate_delivery() {
    use agent_core::{
        agent::GraphIngress, contracts::events::EventId, contracts::fabric::FabricError,
    };
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("approval.db");
    let models = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap();
    let resources = |fabric: Arc<dyn agent_core::contracts::fabric::OperatorFabric>| {
        AgentResources::new(fabric)
            .model("primary", "v1", Arc::new(RoundModel(models.clone())))
            .toolset("workspace", "v1", Arc::new(ReviewedEcho(calls.clone())))
    };
    let fabric = Arc::new(SqliteEventStore::open(&path, "approval").await.unwrap());
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(fabric.clone()))
        .build(definition.clone())
        .unwrap();
    let run = agent.run("approve").await.unwrap().run_id;
    let request = approval_event(&agent, run).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    agent.engine.stop_workers().await;
    drop(agent);
    drop(fabric);
    let fabric = Arc::new(SqliteEventStore::open(&path, "approval").await.unwrap());
    let ingress = GraphIngress::new(fabric.clone());
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(fabric))
        .build(definition)
        .unwrap();
    agent.engine.recover("local", run).await.unwrap();
    let id = EventId::new();
    let decision = GraphEvent::new(
        "approval.decided.v1",
        json!({"approval_id":request["approval_id"],"binding_digest":request["binding_digest"],"actor":"test","resolution":{"decision":"allow-once"}}),
    );
    let mut stale = decision.clone();
    stale.payload["binding_digest"] = json!(format!("sha256:{}", "0".repeat(64)));
    ingress
        .send("local", run, EventId::new(), stale)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        ingress.send("other", run, id, decision.clone()).await,
        Err(FabricError::NotFound)
    ));
    assert!(
        ingress
            .send("local", run, id, decision.clone())
            .await
            .unwrap()
    );
    assert!(
        !ingress
            .send("local", run, id, decision.clone())
            .await
            .unwrap()
    );
    let mut conflict = decision;
    conflict.payload["actor"] = json!("changed");
    assert!(matches!(
        ingress.send("local", run, id, conflict).await,
        Err(FabricError::Conflict)
    ));
    assert_eq!(terminal(&agent, run).await.status, GraphStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 2);
    agent.engine.stop_workers().await;
}
struct Allow;
impl ApprovalPort for Allow {
    fn request(&self, _: ApprovalRequest) -> ApprovalFuture {
        Box::pin(async { Ok(ApprovalResolution::allow_once()) })
    }
}
#[tokio::test]
async fn graph_stream_approval_precedes_execution_and_continues_model() {
    use agent_core::agent::GraphAgent;
    use futures_util::StreamExt;
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = GraphAgent::new(
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap(),
        OperatorRegistry::builtins(),
        AgentResources::new(Arc::new(MemoryFabric::default()))
            .model(
                "primary",
                "v1",
                Arc::new(RoundModel(Arc::new(AtomicUsize::new(0)))),
            )
            .toolset("workspace", "v1", Arc::new(ReviewedEcho(calls.clone()))),
    )
    .unwrap()
    .with_approval_port(Arc::new(Allow));
    let events: Vec<_> = tokio::time::timeout(
        Duration::from_secs(5),
        agent_core::agent::Harness::new(agent)
            .stream("approve")
            .unwrap()
            .collect(),
    )
    .await
    .unwrap();
    let requested = events
        .iter()
        .position(|e| matches!(e.kind, RunEventKind::ApprovalRequested { .. }))
        .unwrap();
    let resolved = events
        .iter()
        .position(|e| matches!(e.kind, RunEventKind::ApprovalResolved { .. }))
        .unwrap();
    let executed = events
        .iter()
        .position(|e| matches!(e.kind, RunEventKind::ToolExecutionStarted { .. }))
        .unwrap();
    assert!(requested < resolved && resolved < executed);
    assert!(matches!(
        events.last().unwrap().kind,
        RunEventKind::RunCompleted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct ErrorAwareModel(AtomicUsize);
impl ModelPort for ErrorAwareModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            return RoundModel(Arc::new(AtomicUsize::new(0))).stream(request);
        }
        let content: Value =
            serde_json::from_str(&request.messages.last().unwrap().content).unwrap();
        assert_eq!(content["ok"], false);
        assert_eq!(content["error"]["code"], "tool_rejected");
        Box::pin(futures_util::stream::iter([ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        }]))
    }
}
#[tokio::test]
async fn denied_and_expired_approvals_return_results_without_executing_tools() {
    use agent_core::{agent::GraphIngress, contracts::events::EventId};
    for expire in [false, true] {
        let fabric = Arc::new(MemoryFabric::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut definition =
            AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap();
        if expire {
            definition.operators.get_mut("policy").unwrap().config["approval_timeout_ms"] =
                json!(50);
        }
        let agent = AgentBuilder::new(
            OperatorRegistry::builtins(),
            AgentResources::new(fabric.clone())
                .model(
                    "primary",
                    "v1",
                    Arc::new(ErrorAwareModel(AtomicUsize::new(0))),
                )
                .toolset("workspace", "v1", Arc::new(ReviewedEcho(calls.clone()))),
        )
        .build(definition)
        .unwrap();
        let run = agent.run("reject").await.unwrap().run_id;
        let request = approval_event(&agent, run).await;
        if expire {
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        let decision = if expire {
            GraphEvent::new(
                "approval.expire.v1",
                json!({"approval_id":request["approval_id"]}),
            )
        } else {
            GraphEvent::new(
                "approval.decided.v1",
                json!({"approval_id":request["approval_id"],"binding_digest":request["binding_digest"],"actor":"reviewer","resolution":{"decision":"deny","reason":"not allowed"}}),
            )
        };
        GraphIngress::new(fabric)
            .send("local", run, EventId::new(), decision)
            .await
            .unwrap();
        assert_eq!(terminal(&agent, run).await.status, GraphStatus::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        agent.engine.stop_workers().await;
    }
}
struct RetryEcho(Arc<AtomicUsize>);
impl ToolPort for RetryEcho {
    fn definitions(&self) -> Vec<ToolDefinition> {
        EchoTool(self.0.clone())
            .definitions()
            .into_iter()
            .map(|d| {
                d.with_execution_policy(
                    ToolExecutionPolicy::read_only().with_retry(ToolRetryPolicy::bounded(2, 1, 10)),
                )
            })
            .collect()
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if attempt == 0 {
                return Err(ToolError::new("temporarily_unavailable", "retry", true));
            }
            Ok(ToolOutput::text(
                request.arguments["text"].as_str().unwrap(),
            ))
        })
    }
}
#[tokio::test]
async fn tool_executor_retries_only_with_validated_idempotent_policy() {
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        AgentResources::new(Arc::new(MemoryFabric::default()))
            .model(
                "primary",
                "v1",
                Arc::new(RoundModel(Arc::new(AtomicUsize::new(0)))),
            )
            .toolset("workspace", "v1", Arc::new(RetryEcho(calls.clone()))),
    )
    .build(AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap())
    .unwrap();
    let run = agent.run("retry").await.unwrap().run_id;
    assert_eq!(terminal(&agent, run).await.status, GraphStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.engine.stop_workers().await;
}

struct WaitingEcho(Arc<AtomicUsize>);
impl ToolPort for WaitingEcho {
    fn definitions(&self) -> Vec<ToolDefinition> {
        EchoTool(self.0.clone())
            .definitions()
            .into_iter()
            .map(|d| {
                d.with_execution_policy(
                    ToolExecutionPolicy::read_only().with_completion(ToolCompletion::MaySuspend),
                )
            })
            .collect()
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        use agent_core::contracts::events::*;
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolOutput::suspend(
                vec![WaitSpec {
                    wait_key: "answer".into(),
                    subscription: CreateSubscription {
                        subscription_id: SubscriptionId::stable(
                            "graph-wait-test",
                            &request.run_id.to_string(),
                        ),
                        owner: SubscriptionOwner::Run {
                            run_id: request.run_id,
                        },
                        scope: SubscriptionScope::Run {
                            run_id: request.run_id,
                        },
                        filter: EventFilter {
                            topics: vec!["test.answer".into()],
                            sources: vec![EventSource::Gateway],
                            correlation_id: Some(request.run_id.to_string()),
                            ..Default::default()
                        },
                        delivery: DeliveryTarget::WakeRun {
                            run_id: request.run_id,
                            wait_key: "answer".into(),
                        },
                        mode: SubscriptionMode::Once,
                        start_position: StartPosition::Beginning,
                        expires_at_ms: None,
                        max_deliveries: Some(1),
                        created_at_ms: 0,
                    },
                }],
                vec![],
            ))
        })
    }
}
struct WaitingModel(Arc<AtomicUsize>);
impl ModelPort for WaitingModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            return RoundModel(Arc::new(AtomicUsize::new(0))).stream(request);
        }
        let result: Value =
            serde_json::from_str(&request.messages.last().unwrap().content).unwrap();
        assert_eq!(result["events"][0]["payload"]["answer"], 42);
        Box::pin(futures_util::stream::iter([ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        }]))
    }
}
fn wait_definition() -> AgentDefinition {
    AgentDefinition::from_yaml(include_str!("../../../config/agents/durable-tools.yaml")).unwrap()
}

#[tokio::test]
async fn graph_wait_recovers_and_rejects_forged_delivery_without_reexecuting_tool() {
    use agent_core::{agent::graph::GraphWaitBridge, contracts::events::*};
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wait.db");
    let tools = Arc::new(AtomicUsize::new(0));
    let models = Arc::new(AtomicUsize::new(0));
    let resources = |store: Arc<SqliteEventStore>, bridge: Arc<GraphWaitBridge>| {
        AgentResources::new(store)
            .model("primary", "v1", Arc::new(WaitingModel(models.clone())))
            .toolset("workspace", "v1", Arc::new(WaitingEcho(tools.clone())))
            .wait_service("durable", "v1", bridge)
    };
    let store = Arc::new(SqliteEventStore::open(&path, "wait").await.unwrap());
    let bridge = Arc::new(GraphWaitBridge::new(
        store.clone(),
        store.clone(),
        store.clone(),
    ));
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        resources(store.clone(), bridge.clone()),
    )
    .build(wait_definition())
    .unwrap();
    let run = agent.run("wait").await.unwrap().run_id;
    let sub_id = SubscriptionId::stable("graph-wait-test", &run.to_string());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store.get_subscription(sub_id).await.unwrap().is_some()
                && agent
                    .engine
                    .get_run("local", run)
                    .await
                    .unwrap()
                    .step_usage
                    .iter()
                    .any(|s| s.instance == "wait")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.stop_workers().await;
    drop(agent);
    drop(bridge);
    drop(store);
    let store = Arc::new(SqliteEventStore::open(&path, "wait").await.unwrap());
    let bridge = Arc::new(GraphWaitBridge::new(
        store.clone(),
        store.clone(),
        store.clone(),
    ));
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        resources(store.clone(), bridge.clone()),
    )
    .build(wait_definition())
    .unwrap();
    agent.engine.recover_run("local", run).await.unwrap();
    // No new trigger: recovery must stay at the durable wait and not repeat effects.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        agent.engine.get_run("local", run).await.unwrap().status,
        GraphStatus::Running
    );
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 1);
    let published = store
        .publish(PublishEvent {
            event_id: EventId::new(),
            topic: "test.answer".into(),
            event_type: "answer".into(),
            schema_version: 1,
            source: EventSource::Gateway,
            subject: Some(format!("run/{run}")),
            correlation_id: Some(run.to_string()),
            causation_id: None,
            occurred_at_ms: 1,
            recorded_at_ms: 1,
            payload: json!({"answer":42}),
        })
        .await
        .unwrap();
    let delivery = store
        .claim_deliveries(ClaimDeliveries {
            worker_id: "test".into(),
            now_ms: 2,
            lease_until_ms: 1000,
            limit: 10,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut forged = published.event.clone();
    forged.event.payload = json!({"answer":99});
    assert!(bridge.deliver(delivery.clone(), forged).await.is_err());
    bridge
        .deliver(delivery.clone(), published.event.clone())
        .await
        .unwrap();
    bridge.deliver(delivery, published.event).await.unwrap();
    assert_eq!(terminal(&agent, run).await.status, GraphStatus::Completed);
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 2);
    agent.engine.stop_workers().await;
}

struct ParallelGraphModel(AtomicUsize);
impl ModelPort for ParallelGraphModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut events = vec![];
            for id in ["slow", "fast"] {
                events.push(ModelEvent::ToolCallStarted {
                    call_id: id.into(),
                    name: "echo".into(),
                });
                events.push(ModelEvent::ToolCallArgumentsDelta {
                    call_id: id.into(),
                    delta: json!({"text":id}).to_string(),
                });
            }
            events.push(ModelEvent::Completed {
                finish_reason: FinishReason::ToolCall,
            });
            Box::pin(futures_util::stream::iter(events))
        } else {
            let messages = &request.messages;
            assert_eq!(messages[messages.len() - 2].content, "slow");
            assert_eq!(messages[messages.len() - 1].content, "fast");
            Box::pin(futures_util::stream::iter([ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            }]))
        }
    }
}
struct ParallelGraphTool {
    exclusive: bool,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}
impl ToolPort for ParallelGraphTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        EchoTool(self.active.clone())
            .definitions()
            .into_iter()
            .map(|d| {
                d.with_execution_policy(ToolExecutionPolicy::read_only().with_concurrency(
                    if self.exclusive {
                        ToolConcurrency::Exclusive
                    } else {
                        ToolConcurrency::ParallelSafe
                    },
                ))
            })
            .collect()
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let active = self.active.clone();
        let maximum = self.maximum.clone();
        Box::pin(async move {
            maximum.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(if request.call_id == "slow" {
                40
            } else {
                5
            }))
            .await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(ToolOutput::text(
                request.arguments["text"].as_str().unwrap(),
            ))
        })
    }
}
#[tokio::test]
async fn graph_parallel_tools_overlap_only_when_enabled_and_gate_preserves_order() {
    for (parallel, exclusive) in [(false, false), (true, false), (true, true)] {
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut config = agent_core::agent::graph::GraphAgentConfig::new("test", "test");
        config.parallel_tools = parallel;
        let agent = AgentBuilder::new(
            OperatorRegistry::builtins(),
            AgentResources::new(Arc::new(MemoryFabric::default()))
                .model(
                    "primary",
                    "v1",
                    Arc::new(ParallelGraphModel(AtomicUsize::new(0))),
                )
                .toolset(
                    "workspace",
                    "v1",
                    Arc::new(ParallelGraphTool {
                        exclusive,
                        active: Arc::new(AtomicUsize::new(0)),
                        maximum: maximum.clone(),
                    }),
                ),
        )
        .build(config.definition().unwrap())
        .unwrap();
        let run = agent.run("parallel").await.unwrap().run_id;
        assert_eq!(terminal(&agent, run).await.status, GraphStatus::Completed);
        assert_eq!(
            maximum.load(Ordering::SeqCst),
            if parallel && !exclusive { 2 } else { 1 }
        );
        agent.engine.stop_workers().await;
    }
}
#[tokio::test]
async fn cancelling_graph_cleans_persisted_external_subscriptions() {
    use agent_core::{agent::graph::GraphWaitBridge, contracts::events::*};
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteEventStore::open(dir.path().join("cancel.db"), "cancel")
            .await
            .unwrap(),
    );
    let bridge = Arc::new(GraphWaitBridge::new(
        store.clone(),
        store.clone(),
        store.clone(),
    ));
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        AgentResources::new(store.clone())
            .model(
                "primary",
                "v1",
                Arc::new(WaitingModel(Arc::new(AtomicUsize::new(0)))),
            )
            .toolset(
                "workspace",
                "v1",
                Arc::new(WaitingEcho(Arc::new(AtomicUsize::new(0)))),
            )
            .wait_service("durable", "v1", bridge),
    )
    .build(wait_definition())
    .unwrap();
    let run = agent.run("cancel").await.unwrap().run_id;
    let sub_id = SubscriptionId::stable("graph-wait-test", &run.to_string());
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.get_subscription(sub_id).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.cancel("local", run).await.unwrap();
    agent.engine.stop_workers().await;
    assert_eq!(
        store
            .get_subscription(sub_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        SubscriptionStatus::Cancelled
    );
}

struct PrivateEcho(Arc<AtomicUsize>);
impl ToolPort for PrivateEcho {
    fn definitions(&self) -> Vec<ToolDefinition> {
        EchoTool(self.0.clone()).definitions()
    }
    fn argument_visibility(&self, _: &str) -> ToolArgumentVisibility {
        ToolArgumentVisibility::DigestOnly
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        EchoTool(self.0.clone()).call(request)
    }
}
#[tokio::test]
async fn graph_stream_redacts_private_arguments_without_changing_tool_input() {
    use futures_util::StreamExt;
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = agent_core::agent::GraphAgent::new(
        agent_core::agent::GraphAgentConfig::new("test", "test")
            .definition()
            .unwrap(),
        OperatorRegistry::builtins(),
        AgentResources::new(Arc::new(MemoryFabric::default()))
            .model(
                "primary",
                "v1",
                Arc::new(RoundModel(Arc::new(AtomicUsize::new(0)))),
            )
            .toolset("workspace", "v1", Arc::new(PrivateEcho(calls.clone()))),
    )
    .unwrap();
    let events: Vec<_> = agent_core::agent::Harness::new(agent)
        .stream("private")
        .unwrap()
        .collect()
        .await;
    let args: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ToolCallArgumentsDelta { delta, .. } => {
                Some(serde_json::from_str::<Value>(delta).unwrap())
            }
            RunEventKind::ToolExecutionStarted { arguments, .. } => Some(arguments.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(args.len(), 2);
    assert!(
        args.iter()
            .all(|v| v["redacted"] == true && !v.to_string().contains("value"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        events.last().unwrap().kind,
        RunEventKind::RunCompleted { .. }
    ));
}

#[tokio::test]
async fn durable_host_replays_identical_events_after_sqlite_reopen_and_projects_once() {
    use agent_core::agent::GraphHost;
    use agent_extension::store::{SqliteEventStore, SqliteRunStore};
    use futures_util::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("host.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let run = RunId::new();
    let resources = |fabric: Arc<dyn agent_core::contracts::fabric::OperatorFabric>| {
        AgentResources::new(fabric).model("primary", "v1", Arc::new(Text(calls.clone())))
    };
    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = GraphHost::new(
        "tenant-a",
        text_definition(),
        OperatorRegistry::builtins(),
        resources(fabric.clone()),
    )
    .unwrap();
    host.start(run, json!("hello")).await.unwrap();
    let live: Vec<_> = tokio::time::timeout(
        Duration::from_secs(5),
        host.subscribe(run, 0).collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .into_iter()
    .map(Result::unwrap)
    .collect();
    assert_eq!(live.first().unwrap().kind, RunEventKind::RunStarted);
    assert!(live.last().unwrap().kind.is_terminal());
    assert_eq!(live.iter().filter(|e| e.kind.is_terminal()).count(), 1);
    assert!(
        live.iter().any(
            |e| matches!(&e.kind, RunEventKind::OutputDelta { delta, .. } if delta == "hello")
        )
    );
    host.engine.stop_workers().await;
    drop(host);
    drop(fabric);

    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = GraphHost::new(
        "tenant-a",
        text_definition(),
        OperatorRegistry::builtins(),
        resources(fabric.clone()),
    )
    .unwrap();
    host.recover(run).await.unwrap();
    for after in 0..=live.len() as u64 {
        assert_eq!(
            host.events_after(run, after, 128).await.unwrap(),
            live.iter()
                .filter(|e| e.seq > after)
                .cloned()
                .collect::<Vec<_>>()
        );
    }
    let other = GraphHost::new(
        "tenant-b",
        text_definition(),
        OperatorRegistry::builtins(),
        resources(fabric),
    )
    .unwrap();
    assert_eq!(
        other.events_after(run, 0, 128).await.unwrap_err(),
        agent_core::contracts::fabric::FabricError::NotFound
    );
    let runs = SqliteRunStore::open(&path).await.unwrap();
    runs.create_run(RunSnapshot::new(run, "hello", 1))
        .await
        .unwrap();
    let (a, b) = tokio::join!(host.project_into(run, &runs), host.project_into(run, &runs));
    a.unwrap();
    b.unwrap();
    host.project_into(run, &runs).await.unwrap();
    assert_eq!(runs.events_after(run, 0, 128).await.unwrap(), live);
    assert_eq!(runs.get_run(run).await.unwrap().unwrap().output, "hello");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    host.engine.stop_workers().await;
}

#[tokio::test]
async fn durable_host_keeps_partial_output_when_subscriber_disconnects_and_reopens() {
    use agent_core::agent::GraphHost;
    use agent_extension::store::SqliteEventStore;
    use futures_util::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.db");
    let make = |fabric: Arc<dyn agent_core::contracts::fabric::OperatorFabric>| {
        GraphHost::new(
            "local",
            text_definition(),
            OperatorRegistry::builtins(),
            AgentResources::new(fabric).model("primary", "v1", Arc::new(PreviewThenPending)),
        )
        .unwrap()
    };
    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = make(fabric.clone());
    let run = RunId::new();
    host.start(run, json!("hello")).await.unwrap();
    let mut events = host.subscribe(run, 0);
    let first = events.next().await.unwrap().unwrap();
    let partial = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(&partial.kind, RunEventKind::OutputDelta { delta, .. } if delta == "live"));
    drop(events);
    assert_eq!(
        host.engine.get_run("local", run).await.unwrap().status,
        GraphStatus::Running
    );
    host.engine.stop_workers().await;
    drop(host);
    drop(fabric);
    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = make(fabric);
    assert_eq!(
        host.events_after(run, 0, 128).await.unwrap(),
        vec![first, partial.clone()]
    );
    host.cancel(run).await.unwrap();
    let tail = host.events_after(run, partial.seq, 128).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].kind, RunEventKind::RunCancelled);
    assert!(host.subscribe(run, tail[0].seq).next().await.is_none());
}

#[tokio::test]
async fn durable_host_recovers_unprojected_approval_and_terminal_facts() {
    use agent_core::{
        agent::{GraphHost, GraphIngress},
        contracts::events::EventId,
    };
    use agent_extension::store::SqliteEventStore;
    use futures_util::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("host-approval.db");
    let models = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let build = |fabric: Arc<dyn agent_core::contracts::fabric::OperatorFabric>| {
        GraphHost::new(
            "local",
            AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap(),
            OperatorRegistry::builtins(),
            AgentResources::new(fabric)
                .model("primary", "v1", Arc::new(RoundModel(models.clone())))
                .toolset("workspace", "v1", Arc::new(ReviewedEcho(tools.clone()))),
        )
        .unwrap()
    };
    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = build(fabric.clone());
    let run = RunId::new();
    host.start(run, json!("approve")).await.unwrap();
    let approval = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = host
                .engine
                .events_after("local", run, 0, 128)
                .await
                .unwrap()
                .into_iter()
                .find(|e| e.kind == "approval.requested.v1")
            {
                break event.payload;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // No public reader has projected the approval yet.
    host.engine.stop_workers().await;
    drop(host);
    drop(fabric);
    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = build(fabric.clone());
    host.recover(run).await.unwrap();
    let pending = host.events_after(run, 0, 128).await.unwrap();
    assert_eq!(
        pending
            .iter()
            .filter(|e| matches!(e.kind, RunEventKind::ApprovalRequested { .. }))
            .count(),
        1
    );
    GraphIngress::new(fabric.clone()).send("local", run, EventId::new(), GraphEvent::new("approval.decided.v1", json!({"approval_id":approval["approval_id"],"binding_digest":approval["binding_digest"],"actor":"test","resolution":ApprovalResolution::allow_once()}))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !host
            .engine
            .get_run("local", run)
            .await
            .unwrap()
            .is_terminal()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // Terminal state committed, but no public reader has projected its terminal event.
    host.engine.stop_workers().await;
    drop(host);
    drop(fabric);
    let fabric = Arc::new(SqliteEventStore::open(&path, "host").await.unwrap());
    let host = build(fabric);
    host.recover(run).await.unwrap();
    let events: Vec<_> = tokio::time::timeout(
        Duration::from_secs(5),
        host.subscribe(run, 0).collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .into_iter()
    .map(Result::unwrap)
    .collect();
    assert!(events.starts_with(&pending));
    let position = |name: &str| {
        events
            .iter()
            .position(|e| e.kind.event_name() == name)
            .unwrap()
    };
    assert!(position("approval_requested") < position("approval_resolved"));
    assert!(position("approval_resolved") < position("tool_execution_started"));
    assert!(position("tool_execution_started") < position("tool_execution_completed"));
    assert!(position("tool_execution_completed") < position("run_completed"));
    assert_eq!(events.iter().filter(|e| e.kind.is_terminal()).count(), 1);
    assert_eq!(host.events_after(run, 0, 128).await.unwrap(), events);
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 2);
    host.engine.stop_workers().await;
}

#[tokio::test]
async fn public_terminal_preserves_safe_provider_failure_and_retryability() {
    use futures_util::StreamExt;
    struct Failed;
    impl ModelPort for Failed {
        fn stream(&self, _: ModelRequest) -> ModelEventStream {
            Box::pin(futures_util::stream::iter([ModelEvent::Failed {
                error: ModelError::new(ModelErrorKind::RateLimited, "Please retry later", true),
            }]))
        }
    }
    let host = agent_core::agent::GraphHost::new(
        "local",
        text_definition(),
        OperatorRegistry::builtins(),
        AgentResources::new(Arc::new(MemoryFabric::default())).model(
            "primary",
            "v1",
            Arc::new(Failed),
        ),
    )
    .unwrap();
    let run = RunId::new();
    host.start(run, json!("test")).await.unwrap();
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        host.subscribe(run, 0).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(matches!(&events.last().unwrap().as_ref().unwrap().kind,
        RunEventKind::RunFailed { code, message, retryable:true } if code == ModelErrorKind::RateLimited.code() && message == "Please retry later"));
    host.engine.stop_workers().await;
}

struct CleanupInterrupted {
    inner: Arc<dyn agent_core::contracts::fabric::OperatorFabric>,
    failing: std::sync::atomic::AtomicBool,
}
impl agent_core::contracts::fabric::OperatorFabric for CleanupInterrupted {
    fn capabilities(&self) -> agent_core::contracts::fabric::FabricCapabilities {
        self.inner.capabilities()
    }
    fn execute(
        &self,
        command: agent_core::contracts::fabric::FabricCommand,
    ) -> agent_core::contracts::fabric::FabricFuture<'_> {
        use agent_core::contracts::fabric::*;
        Box::pin(async move {
            if self.failing.load(Ordering::SeqCst)
                && matches!(&command, FabricCommand::Transact { checkpoint, .. } if checkpoint.key.ends_with(":cleaned"))
            {
                return Err(FabricError::Storage(
                    "simulated cleanup interruption".into(),
                ));
            }
            self.inner.execute(command).await
        })
    }
}
#[tokio::test]
async fn terminal_recovery_finishes_interrupted_external_cleanup_without_reexecution() {
    use agent_core::{agent::GraphWaitBridge, contracts::events::*};
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteEventStore::open(dir.path().join("cleanup.db"), "cleanup")
            .await
            .unwrap(),
    );
    let fabric = Arc::new(CleanupInterrupted {
        inner: store.clone(),
        failing: true.into(),
    });
    let bridge = Arc::new(GraphWaitBridge::new(
        fabric.clone(),
        store.clone(),
        store.clone(),
    ));
    let model_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let resources = AgentResources::new(fabric.clone())
        .model("primary", "v1", Arc::new(WaitingModel(model_calls.clone())))
        .toolset("workspace", "v1", Arc::new(WaitingEcho(tool_calls.clone())))
        .wait_service("durable", "v1", bridge);
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources.clone())
        .build(wait_definition())
        .unwrap();
    let run = agent.run("cleanup").await.unwrap().run_id;
    let subscription = SubscriptionId::stable("graph-wait-test", &run.to_string());
    tokio::time::timeout(Duration::from_secs(5), async {
        while store
            .get_subscription(subscription)
            .await
            .unwrap()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.cancel("local", run).await.unwrap();
    agent.engine.stop_workers().await;
    let saved = agent.engine.get_run("local", run).await.unwrap();
    assert!(saved.is_terminal());
    assert!(!saved.cleanup_complete);
    drop(agent);
    fabric.failing.store(false, Ordering::SeqCst);
    let recovered = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(wait_definition())
        .unwrap();
    recovered.engine.recover("local", run).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !recovered
            .engine
            .get_run("local", run)
            .await
            .unwrap()
            .cleanup_complete
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store
            .get_subscription(subscription)
            .await
            .unwrap()
            .unwrap()
            .status,
        SubscriptionStatus::Cancelled
    );
    assert_eq!(model_calls.load(Ordering::SeqCst), 1);
    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    recovered.engine.stop_workers().await;
}

struct PausedRegistration {
    inner: Arc<dyn agent_core::contracts::fabric::OperatorFabric>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    paused: std::sync::atomic::AtomicBool,
}
impl agent_core::contracts::fabric::OperatorFabric for PausedRegistration {
    fn capabilities(&self) -> agent_core::contracts::fabric::FabricCapabilities {
        self.inner.capabilities()
    }
    fn execute(
        &self,
        command: agent_core::contracts::fabric::FabricCommand,
    ) -> agent_core::contracts::fabric::FabricFuture<'_> {
        use agent_core::contracts::fabric::*;
        Box::pin(async move {
            if matches!(&command, FabricCommand::Transact { checkpoint, .. } if checkpoint.key.starts_with("graph-wait-subscription:"))
                && self.paused.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.execute(command).await
        })
    }
}
#[tokio::test]
async fn public_waiting_is_emitted_only_after_external_subscription_is_ready() {
    use agent_core::{
        agent::{GraphHost, GraphWaitBridge},
        contracts::events::*,
    };
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteEventStore::open(dir.path().join("registration.db"), "ready")
            .await
            .unwrap(),
    );
    let fabric = Arc::new(PausedRegistration {
        inner: store.clone(),
        entered: Default::default(),
        release: Default::default(),
        paused: true.into(),
    });
    let bridge = Arc::new(GraphWaitBridge::new(
        fabric.clone(),
        store.clone(),
        store.clone(),
    ));
    let host = GraphHost::new(
        "local",
        wait_definition(),
        OperatorRegistry::builtins(),
        AgentResources::new(fabric.clone())
            .model(
                "primary",
                "v1",
                Arc::new(WaitingModel(Arc::new(AtomicUsize::new(0)))),
            )
            .toolset(
                "workspace",
                "v1",
                Arc::new(WaitingEcho(Arc::new(AtomicUsize::new(0)))),
            )
            .wait_service("durable", "v1", bridge),
    )
    .unwrap();
    let run = RunId::new();
    host.start(run, json!("wait")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), fabric.entered.notified())
        .await
        .unwrap();
    let subscription = SubscriptionId::stable("graph-wait-test", &run.to_string());
    assert!(
        store
            .get_subscription(subscription)
            .await
            .unwrap()
            .is_none()
    );
    let events = host.events_after(run, 0, 128).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::RunWaiting { .. }))
    );
    fabric.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = host.events_after(run, 0, 128).await.unwrap();
            if events
                .iter()
                .any(|e| matches!(e.kind, RunEventKind::RunWaiting { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store
            .get_subscription(subscription)
            .await
            .unwrap()
            .unwrap()
            .status,
        SubscriptionStatus::Active
    );
    host.cancel(run).await.unwrap();
    host.engine.stop_workers().await;
}

fn multi_trigger_definition() -> AgentDefinition {
    let mut definition = echo_definition();
    definition.triggers.insert(
        "api".into(),
        agent_core::agent::TriggerDefinition::manual("echo.start.v1"),
    );
    let mut edge = definition.subscriptions[0].clone();
    edge.id = "api-start".into();
    edge.from = "api".into();
    definition.subscriptions.push(edge);
    definition
}

#[test]
fn trigger_nodes_validate_names_types_protocols_and_reachability() {
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(Echo)))
        .unwrap();
    let resources = AgentResources::new(Arc::new(MemoryFabric::default()));
    let valid = multi_trigger_definition();
    valid.validate(&registry, &resources).unwrap();
    for case in 0..9 {
        let mut invalid = valid.clone();
        match case {
            0 => invalid.triggers.clear(),
            1 => invalid.lifecycle.start.trigger = "missing".into(),
            2 => invalid.triggers.get_mut("api").unwrap().kind = "cron".into(),
            3 => invalid.triggers.get_mut("api").unwrap().version = 99,
            4 => invalid.triggers.get_mut("api").unwrap().config = json!({"unknown":true}),
            5 => invalid.triggers.get_mut("api").unwrap().event = "other.start.v1".into(),
            6 => {
                invalid.triggers.insert(
                    "echo".into(),
                    agent_core::agent::TriggerDefinition::manual("echo.start.v1"),
                );
            }
            7 => invalid.subscriptions[0].from = "$input".into(),
            _ => invalid.schema_version = 1,
        }
        assert!(
            invalid.validate(&registry, &resources).is_err(),
            "case {case}"
        );
    }
    let mut invalid = valid;
    invalid.subscriptions.retain(|s| s.from != "api");
    assert!(invalid.validate(&registry, &resources).is_err());
    assert!(
        AgentGraph::new("duplicate")
            .trigger(
                "same",
                agent_core::agent::TriggerDefinition::manual("echo.start.v1")
            )
            .trigger(
                "same",
                agent_core::agent::TriggerDefinition::manual("echo.start.v1")
            )
            .lifecycle(echo_definition().lifecycle)
            .limits(GraphLimits::new(1000, 10))
            .finish()
            .is_err()
    );
}

#[tokio::test]
async fn manual_triggers_create_independent_runs_and_pin_retry_identity() {
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(Echo)))
        .unwrap();
    let fabric = Arc::new(MemoryFabric::default());
    let agent = AgentBuilder::new(registry, AgentResources::new(fabric))
        .build(multi_trigger_definition())
        .unwrap();
    assert!(agent.trigger("missing", "hello").await.is_err());
    let invalid = RunId::new();
    assert!(
        agent
            .engine
            .admit_trigger("local", invalid, "api", json!({}))
            .await
            .is_err()
    );
    assert!(agent.engine.get_run("local", invalid).await.is_err());
    let first = agent.trigger("api", "one").await.unwrap();
    let second = agent.run("two").await.unwrap();
    assert_ne!(first.run_id, second.run_id);
    assert_eq!(
        terminal(&agent, first.run_id).await.output,
        Some(json!({"input":"one"}))
    );
    assert_eq!(terminal(&agent, second.run_id).await.trigger, "start");
    let events = agent
        .engine
        .events_after("local", first.run_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(events[0].from, "api");
    agent
        .engine
        .start_trigger("local", first.run_id, "api", json!("one"))
        .await
        .unwrap();
    assert_eq!(
        agent
            .engine
            .events_after("local", first.run_id, 0, 100)
            .await
            .unwrap()
            .len(),
        events.len()
    );
    assert!(
        agent
            .engine
            .admit_trigger("local", first.run_id, "start", json!("one"))
            .await
            .is_err()
    );
    assert!(
        agent
            .engine
            .admit_trigger("local", first.run_id, "api", json!("changed"))
            .await
            .is_err()
    );
    agent.engine.stop_workers().await;
}

#[tokio::test]
async fn sqlite_recovery_keeps_the_selected_trigger_without_firing_again() {
    use agent_extension::store::SqliteEventStore;
    let mut registry = OperatorRegistry::default();
    registry
        .register(echo_descriptor(), |_, _| Ok(Arc::new(Echo)))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trigger.sqlite");
    let definition = multi_trigger_definition();
    let run = RunId::new();
    {
        let store = Arc::new(SqliteEventStore::open(&path, "first").await.unwrap());
        let agent = AgentBuilder::new(registry.clone(), AgentResources::new(store))
            .build(definition.clone())
            .unwrap();
        agent
            .engine
            .admit_trigger("local", run, "api", json!("durable"))
            .await
            .unwrap();
        assert_eq!(
            agent
                .engine
                .events_after("local", run, 0, 100)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    let store = Arc::new(SqliteEventStore::open(&path, "reopened").await.unwrap());
    let agent = AgentBuilder::new(registry, AgentResources::new(store))
        .build(definition)
        .unwrap();
    agent.engine.recover("local", run).await.unwrap();
    let saved = terminal(&agent, run).await;
    assert_eq!(saved.trigger, "api");
    assert_eq!(saved.output, Some(json!({"input":"durable"})));
    let events = agent
        .engine
        .events_after("local", run, 0, 100)
        .await
        .unwrap();
    assert_eq!(events.iter().filter(|e| e.from == "api").count(), 1);
    agent.engine.stop_workers().await;
}

#[tokio::test]
async fn waiting_run_archive_moves_sqlite_dependencies_and_resumes_once() {
    use agent_core::{
        agent::{GraphWaitBridge, RunManager},
        contracts::events::*,
    };
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let models = Arc::new(AtomicUsize::new(0));
    let resources = |store: Arc<SqliteEventStore>| {
        AgentResources::new(store.clone())
            .model("primary", "v1", Arc::new(WaitingModel(models.clone())))
            .toolset("workspace", "v1", Arc::new(WaitingEcho(calls.clone())))
            .wait_service(
                "durable",
                "v1",
                Arc::new(GraphWaitBridge::new(store.clone(), store.clone(), store)),
            )
    };
    let source = Arc::new(
        SqliteEventStore::open(dir.path().join("source.db"), "source")
            .await
            .unwrap(),
    );
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(source.clone()))
        .build(wait_definition())
        .unwrap();
    let run = agent.run("wait").await.unwrap().run_id;
    let subscription = SubscriptionId::stable("graph-wait-test", &run.to_string());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if source
                .get_subscription(subscription)
                .await
                .unwrap()
                .is_some()
                && agent
                    .engine
                    .get_run("local", run)
                    .await
                    .unwrap()
                    .step_usage
                    .iter()
                    .any(|s| s.instance == "wait")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.stop_workers().await;
    let manager = RunManager::new("local", source.clone());
    manager
        .begin_maintenance("source", i64::MAX - 1)
        .await
        .unwrap();
    let bundle = manager.export_run(run, "source").await.unwrap();
    assert!(bundle.dependencies.is_some());
    let target = Arc::new(
        SqliteEventStore::open(dir.path().join("target.db"), "target")
            .await
            .unwrap(),
    );
    let imported = RunManager::new("local", target.clone());
    imported
        .begin_maintenance("target", i64::MAX - 1)
        .await
        .unwrap();
    imported
        .import_run(
            bundle.clone(),
            "target",
            &OperatorRegistry::builtins(),
            &resources(target.clone()),
            None,
        )
        .await
        .unwrap();
    imported
        .import_run(
            bundle,
            "target",
            &OperatorRegistry::builtins(),
            &resources(target.clone()),
            None,
        )
        .await
        .unwrap();
    assert!(
        target
            .get_subscription(subscription)
            .await
            .unwrap()
            .is_some()
    );
    let event = PublishEvent {
        event_id: EventId::new(),
        topic: "test.answer".into(),
        event_type: "test.answer".into(),
        schema_version: 1,
        source: EventSource::Gateway,
        subject: Some(format!("run/{run}")),
        correlation_id: Some(run.to_string()),
        causation_id: None,
        occurred_at_ms: 0,
        recorded_at_ms: 0,
        payload: json!({"answer":42}),
    };
    assert!(target.publish(event.clone()).await.is_err());
    assert!(source.publish(event.clone()).await.is_err());
    imported.end_maintenance("target").await.unwrap();
    let bridge = Arc::new(GraphWaitBridge::new(
        target.clone(),
        target.clone(),
        target.clone(),
    ));
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(target.clone()))
        .build(wait_definition())
        .unwrap();
    agent.engine.recover("local", run).await.unwrap();
    let published = target.publish(event).await.unwrap();
    let deliveries = target
        .claim_deliveries(ClaimDeliveries {
            worker_id: "restore".into(),
            now_ms: 1,
            lease_until_ms: 10000,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    bridge
        .deliver(deliveries[0].clone(), published.event)
        .await
        .unwrap();
    terminal(&agent, run).await;
    agent.engine.stop_workers().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn approval_archive_preserves_binding_and_executes_only_after_decision() {
    use agent_core::agent::{GraphIngress, RunManager};
    use agent_core::contracts::events::EventId;
    let models = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap();
    let resources = |fabric: Arc<dyn agent_core::contracts::fabric::OperatorFabric>| {
        AgentResources::new(fabric)
            .model("primary", "v1", Arc::new(RoundModel(models.clone())))
            .toolset("workspace", "v1", Arc::new(ReviewedEcho(calls.clone())))
    };
    let source = Arc::new(MemoryFabric::default());
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(source.clone()))
        .build(definition.clone())
        .unwrap();
    let run = agent.run("approve").await.unwrap().run_id;
    let request = approval_event(&agent, run).await;
    agent.engine.stop_workers().await;
    let source_manager = RunManager::new("local", source);
    source_manager
        .begin_maintenance("source", i64::MAX - 1)
        .await
        .unwrap();
    let bundle = source_manager.export_run(run, "source").await.unwrap();
    let target = Arc::new(MemoryFabric::default());
    let target_manager = RunManager::new("local", target.clone());
    target_manager
        .begin_maintenance("target", i64::MAX - 1)
        .await
        .unwrap();
    target_manager
        .import_run(
            bundle,
            "target",
            &OperatorRegistry::builtins(),
            &resources(target.clone()),
            None,
        )
        .await
        .unwrap();
    target_manager.end_maintenance("target").await.unwrap();
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(target.clone()))
        .build(definition)
        .unwrap();
    agent.engine.recover("local", run).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    GraphIngress::new(target).send("local",run,EventId::new(),GraphEvent::new("approval.decided.v1",json!({"approval_id":request["approval_id"],"binding_digest":request["binding_digest"],"actor":"test","resolution":{"decision":"allow-once"}}))).await.unwrap();
    assert_eq!(terminal(&agent, run).await.status, GraphStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    agent.engine.stop_workers().await;
}

#[tokio::test]
async fn archive_keeps_unconfirmed_model_invocation_without_replay() {
    use agent_core::agent::RunManager;
    let calls = Arc::new(AtomicUsize::new(0));
    let resources = |fabric: Arc<MemoryFabric>| {
        AgentResources::new(fabric).model("primary", "v1", Arc::new(Pending(calls.clone())))
    };
    let source = Arc::new(MemoryFabric::default());
    let mut definition = text_definition();
    definition.limits.timeout_ms = 400;
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(source.clone()))
        .build(definition.clone())
        .unwrap();
    let run = agent.run("pending").await.unwrap().run_id;
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.stop_workers().await;
    let source_manager = RunManager::new("local", source);
    source_manager
        .begin_maintenance("source", i64::MAX - 1)
        .await
        .unwrap();
    let bundle = source_manager.export_run(run, "source").await.unwrap();
    assert!(
        !serde_json::to_value(bundle.graph_run().unwrap()).unwrap()["inflight"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    let target = Arc::new(MemoryFabric::default());
    let target_manager = RunManager::new("local", target.clone());
    target_manager
        .begin_maintenance("target", i64::MAX - 1)
        .await
        .unwrap();
    target_manager
        .import_run(
            bundle,
            "target",
            &OperatorRegistry::builtins(),
            &resources(target.clone()),
            None,
        )
        .await
        .unwrap();
    target_manager.end_maintenance("target").await.unwrap();
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources(target))
        .build(definition)
        .unwrap();
    agent.engine.recover("local", run).await.unwrap();
    assert_eq!(
        terminal(&agent, run).await.error.as_deref(),
        Some("run_timeout")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    agent.engine.stop_workers().await;
}

#[tokio::test]
async fn compactor_writes_back_before_model_and_keeps_compacted_history_after_tools() {
    use agent_core::agent::CompactorConfig;
    let models = Arc::new(AtomicUsize::new(0));
    let summaries = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let fabric = Arc::new(MemoryFabric::default());
    let resources = AgentResources::new(fabric)
        .model("primary", "m1", Arc::new(RoundModel(models.clone())))
        .model("summary", "s1", Arc::new(Text(summaries.clone())))
        .toolset("workspace", "t1", Arc::new(EchoTool(tools.clone())));
    let mut definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools.yaml")).unwrap();
    let mut config = CompactorConfig::new("summary", 4000, 256, 32000);
    config.safety_margin_tokens = 64;
    config.max_output_tokens = 128;
    config.keep_recent_blocks = 1;
    definition.enable_compaction(config).unwrap();
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources.clone())
        .build(definition.clone())
        .unwrap();
    let run = agent.engine.run("local", json!({"input":"Continue the task", "prior_messages":[ModelMessage::user("OLD_HISTORY_MARKER ".repeat(3000))]})).await.unwrap();
    let state = terminal(&agent, run.run_id).await;
    assert_eq!(state.status, GraphStatus::Completed, "{:?}", state.error);
    assert_eq!(state.usage, Some(test_usage(30, 15)));
    assert_eq!(models.load(Ordering::SeqCst), 2);
    assert_eq!(summaries.load(Ordering::SeqCst), 1);
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    let events = agent
        .engine
        .events_after("local", run.run_id, 0, 128)
        .await
        .unwrap();
    let requests: Vec<_> = events
        .iter()
        .filter(|e| e.kind == "model.requested.v1")
        .collect();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            !request.payload["messages"]
                .to_string()
                .contains("OLD_HISTORY_MARKER")
        );
        assert!(
            request.payload["messages"]
                .to_string()
                .contains("Conversation summary")
        );
        assert!(
            request.payload["messages"]
                .to_string()
                .contains("Continue the task")
        );
        assert_eq!(request.from, "context");
    }
    let state = serde_json::to_value(state).unwrap();
    assert!(
        !state["states"]["context"]["messages"]
            .to_string()
            .contains("OLD_HISTORY_MARKER")
    );
    agent.engine.stop_workers().await;
    let recovered = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(definition)
        .unwrap();
    recovered.engine.recover("local", run.run_id).await.unwrap();
    assert_eq!(
        terminal(&recovered, run.run_id).await.status,
        GraphStatus::Completed
    );
    assert_eq!(summaries.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 2);
    recovered.engine.stop_workers().await;
}

#[test]
fn compactor_rejects_invalid_thresholds_and_unreserved_model_output() {
    use agent_core::agent::CompactorConfig;
    let resources = text_resources(Arc::new(AtomicUsize::new(0)));
    let mut definition = text_definition();
    definition
        .enable_compaction(CompactorConfig::new("primary", 8000, 1024, 8000))
        .unwrap();
    definition
        .validate(&OperatorRegistry::builtins(), &resources)
        .unwrap();
    definition.operators.get_mut("llm").unwrap().config["max_output_tokens"] = json!(2048);
    assert!(
        definition
            .validate(&OperatorRegistry::builtins(), &resources)
            .is_err()
    );
    definition.operators.get_mut("llm").unwrap().config["max_output_tokens"] = json!(1024);
    definition.operators.get_mut("compactor").unwrap().config["target_percent"] = json!(90);
    assert!(
        definition
            .validate(&OperatorRegistry::builtins(), &resources)
            .is_err()
    );
}

struct GrowingContextModel(Arc<AtomicUsize>);
impl ModelPort for GrowingContextModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let round = self.0.fetch_add(1, Ordering::SeqCst);
        // A summary must never leave orphan tool results in the provider request.
        let calls: std::collections::BTreeSet<_> = request
            .messages
            .iter()
            .flat_map(|m| m.tool_calls.iter().map(|c| c.id.as_str()))
            .collect();
        for message in &request.messages {
            if let Some(id) = &message.tool_call_id {
                assert!(calls.contains(id.as_str()));
            }
        }
        let events = if round < 5 {
            let id = format!("growing-{round}");
            vec![
                ModelEvent::ToolCallStarted {
                    call_id: id.clone(),
                    name: "echo".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: id,
                    delta: json!({"text":"x".repeat(2000)}).to_string(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]
        } else {
            vec![
                ModelEvent::TextDelta {
                    delta: "done".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]
        };
        Box::pin(futures_util::stream::iter(events))
    }
}

#[tokio::test]
async fn compactor_checks_each_tool_round_and_rolls_previous_summary_forward() {
    use agent_core::agent::CompactorConfig;
    let models = Arc::new(AtomicUsize::new(0));
    let summaries = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let resources = AgentResources::new(Arc::new(MemoryFabric::default()))
        .model(
            "primary",
            "m1",
            Arc::new(GrowingContextModel(models.clone())),
        )
        .model("summary", "s1", Arc::new(Text(summaries.clone())))
        .toolset("workspace", "t1", Arc::new(EchoTool(tools.clone())));
    let mut config = agent_core::agent::GraphAgentConfig::new("business", "system");
    let mut compaction = CompactorConfig::new("summary", 4000, 256, 32000);
    compaction.safety_margin_tokens = 64;
    compaction.max_output_tokens = 128;
    compaction.keep_recent_blocks = 1;
    config.compaction = Some(compaction);
    let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources)
        .build(config.definition().unwrap())
        .unwrap();
    let run = agent.run("Keep working").await.unwrap();
    let state = terminal(&agent, run.run_id).await;
    assert_eq!(state.status, GraphStatus::Completed, "{:?}", state.error);
    assert_eq!(models.load(Ordering::SeqCst), 6);
    assert_eq!(tools.load(Ordering::SeqCst), 5);
    assert!(summaries.load(Ordering::SeqCst) >= 2);
    let events = agent
        .engine
        .events_after("local", run.run_id, 0, 128)
        .await
        .unwrap();
    let prepared: Vec<_> = events
        .iter()
        .filter(|e| e.kind == "context.prepared.v1")
        .collect();
    assert_eq!(prepared.len(), 6);
    assert_eq!(prepared[0].payload["compacted"], false);
    let compacted: Vec<_> = prepared
        .iter()
        .filter(|e| e.payload["compacted"] == true)
        .collect();
    assert!(compacted.len() >= 2);
    for result in compacted {
        let messages: Vec<ModelMessage> =
            serde_json::from_value(result.payload["messages"].clone()).unwrap();
        assert_eq!(
            messages
                .iter()
                .filter(|m| m.content.starts_with("Conversation summary"))
                .count(),
            1
        );
        assert!(messages.iter().any(|m| m.content == "Keep working"));
    }
    agent.engine.stop_workers().await;
}

#[test]
fn compaction_example_has_valid_wiring() {
    let resources = text_resources(Arc::new(AtomicUsize::new(0))).toolset(
        "workspace",
        "t1",
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
    );
    let definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/tools-compaction.yaml"))
            .unwrap();
    definition
        .validate(&OperatorRegistry::builtins(), &resources)
        .unwrap();
}
