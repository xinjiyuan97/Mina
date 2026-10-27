use agent_core::agent::AgentLoop;
use agent_core::{
    agent::{
        AgentEngine, AgentResources, AgentRunConfig, OperatorBus,
        executor::{FabricHandler, FabricRuntimeConfig, HandlerFuture, HandlerOutput},
        memory_fabric::MemoryFabric,
    },
    contracts::fabric::*,
    contracts::*,
};
use agent_extension::store::InMemoryRunStore;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct EchoOperator(Arc<AtomicUsize>);
impl FabricHandler for EchoOperator {
    fn handle(&self, _: ClaimedDelivery) -> HandlerFuture<'_> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(HandlerOutput {
                result: json!({"ok":true}),
                outbox: vec![],
            })
        })
    }
}
#[tokio::test]
async fn low_level_subscription_publish_and_operator_delivery_are_independent() {
    let fabric = Arc::new(MemoryFabric::default());
    let mut bus = OperatorBus::new(
        fabric,
        FabricRuntimeConfig {
            worker: "api-test".into(),
            claim_limit: 8,
            lease_ms: 1000,
            max_attempts: 3,
            backoff_ms: 10,
            max_backoff_ms: 100,
        },
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let spec = |consumer: &str| SubscriptionSpec {
        consumer: consumer.into(),
        subscription: "test".into(),
        topic: "custom".into(),
        event_type: None,
        tenant: Some("local".into()),
    };
    bus.subscribe_events(spec("observer")).await.unwrap();
    bus.subscribe_operator(spec("echo"), Arc::new(EchoOperator(calls.clone())))
        .await
        .unwrap();
    let event: FabricEvent = serde_json::from_value(json!({
        "topic":"custom", "payload_schema":true, "envelope": {
            "schema_version":1,"engine_version":"operator_v1","projection_version":1,
            "event_id":agent_core::contracts::events::EventId::new(),"event_type":"test.input",
            "tenant_id":"local","run_id":RunId::new(),"session_id":null,"effect_id":"e",
            "invocation_id":"i","attempt":1,"issued_revision":0,"lease_epoch":1,
            "correlation_id":"c","causation_id":null,"operator":{"name":"host","version":1},
            "operator_seq":1,"delivery_class":"durable","occurred_at_ms":0,"payload":{}
        }
    }))
    .unwrap();
    bus.send(event.clone()).await.unwrap();
    bus.send(event).await.unwrap();
    bus.tick(|| 1).await.unwrap();
    bus.tick(|| 2).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let events = bus.events_after("observer", "test", 0, 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert!(
        bus.events_after("observer", "test", events[0].offset, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

struct TextModel(Arc<AtomicUsize>);
impl ModelPort for TextModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures_util::stream::iter([
            ModelEvent::TextDelta {
                delta: "hello".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}
struct NoTools;
impl ToolPort for NoTools {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![]
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        panic!("no tools")
    }
}
#[tokio::test]
async fn high_level_start_replay_and_recovery_require_no_harness() {
    let fabric = Arc::new(MemoryFabric::default());
    let runs = Arc::new(InMemoryRunStore::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = AgentResources::new(fabric)
        .model("primary", "v1", Arc::new(TextModel(calls.clone())))
        .toolset("workspace", "v1", Arc::new(NoTools));
    let engine = Arc::new(AgentEngine::new(runtime.clone(), runs.clone()));
    let config = AgentRunConfig::new("test", "");
    let mut started = engine.run(&config, "hi").await.unwrap();
    let run_id = started.run_id;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !started.events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .unwrap();
    let events = engine.events_after(run_id, 0, 100).await.unwrap();
    assert!(
        engine
            .events_after(run_id, events.last().unwrap().seq, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        events.last().unwrap().kind,
        RunEventKind::RunCompleted { .. }
    ));
    assert_eq!(
        engine.get_run(run_id).await.unwrap().unwrap().output,
        "hello"
    );
    engine.stop_workers().await;
    let reopened = Arc::new(AgentEngine::new(runtime, runs));
    let mut replay = reopened.recover(run_id).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !replay.events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(reopened.events_after(run_id, 0, 100).await.unwrap(), events);
    reopened.stop_workers().await;
}

struct PendingModel(Arc<tokio::sync::Notify>);
impl ModelPort for PendingModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.0.notify_one();
        Box::pin(futures_util::stream::pending())
    }
}
#[tokio::test]
async fn configured_long_model_timeout_is_not_cut_to_two_minutes() {
    let fabric = Arc::new(MemoryFabric::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let resources = AgentResources::new(fabric)
        .model("primary", "v1", Arc::new(PendingModel(entered.clone())))
        .toolset("workspace", "v1", Arc::new(NoTools));
    let engine = Arc::new(AgentEngine::new(
        resources,
        Arc::new(InMemoryRunStore::default()),
    ));
    let mut config = AgentRunConfig::new("test", "");
    config.model_timeout_ms = 600_000;
    let started = engine.run(&config, "slow model").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let host = engine.host(started.run_id).await.unwrap();
    let saved = host.engine.get_run("local", started.run_id).await.unwrap();
    assert_eq!(
        saved.definition.operators["llm"].config["timeout_ms"],
        600_000
    );
    engine.cancel(started.run_id, "test").await.unwrap();
    engine.stop_workers().await;
}

struct ToolModel;
impl ModelPort for ToolModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(futures_util::stream::iter([
            ModelEvent::ToolCallStarted {
                call_id: "call".into(),
                name: "blocking".into(),
            },
            ModelEvent::ToolCallArgumentsDelta {
                call_id: "call".into(),
                delta: "{}".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::ToolCall,
            },
        ]))
    }
}
struct CapturingTool {
    entered: Arc<tokio::sync::Notify>,
    token: Arc<std::sync::Mutex<Option<RunCancellation>>>,
}
impl ToolPort for CapturingTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "blocking",
            "test",
            json!({"type":"object"}),
        )]
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        *self.token.lock().unwrap() = Some(request.cancellation);
        self.entered.notify_one();
        Box::pin(futures_util::future::pending())
    }
}
#[tokio::test]
async fn dropping_stream_cancels_detached_tool_work() {
    use futures_util::StreamExt;
    let entered = Arc::new(tokio::sync::Notify::new());
    let token = Arc::new(std::sync::Mutex::new(None));
    let harness = agent_core::agent::Harness::new(AgentLoop::new(
        ToolModel,
        CapturingTool {
            entered: entered.clone(),
            token: token.clone(),
        },
        "test",
        "",
        None,
        2,
    ));
    let mut execution = harness.start("block").unwrap();
    let task = tokio::spawn(async move { while execution.events.next().await.is_some() {} });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(token.lock().unwrap().as_ref().unwrap().is_cancelled());
}

#[tokio::test]
async fn slow_disconnected_client_replays_bounded_pages_without_stopping_the_run() {
    use futures_util::StreamExt;
    struct Many;
    impl ModelPort for Many {
        fn stream(&self, _: ModelRequest) -> ModelEventStream {
            Box::pin(async_stream::stream! {
                for _ in 0..600 { yield ModelEvent::TextDelta { delta: "part".into() }; }
                yield ModelEvent::Completed { finish_reason: FinishReason::Stop };
            })
        }
    }
    let runtime = AgentResources::new(Arc::new(MemoryFabric::default()))
        .model("primary", "v1", Arc::new(Many))
        .toolset("workspace", "v1", Arc::new(NoTools));
    let engine = Arc::new(AgentEngine::new(
        runtime,
        Arc::new(InMemoryRunStore::default()),
    ));
    let started = engine
        .run(&AgentRunConfig::new("test", ""), "slow reader")
        .await
        .unwrap();
    let run = started.run_id;
    // A durable run outlives both unpolled and disconnected clients.
    drop(started.events);
    let first_connection = engine.event_pages(run, 0);
    drop(first_connection);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if engine
                .get_run(run)
                .await
                .unwrap()
                .is_some_and(|run| run.is_terminal())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut pages = engine.event_pages(run, 0);
    let mut events = Vec::new();
    let mut cursor = 0;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.len() <= 128);
        cursor = page.last().unwrap().seq;
        events.extend(page);
        tokio::task::yield_now().await;
    }
    assert!(events.windows(2).all(|pair| pair[1].seq == pair[0].seq + 1));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, RunEventKind::OutputDelta { .. }))
            .count(),
        600
    );
    assert!(events.last().unwrap().kind.is_terminal());
    assert!(engine.event_pages(run, cursor).next().await.is_none());
    engine.stop_workers().await;
}

struct InterruptedAdmission {
    inner: MemoryFabric,
    fail: std::sync::atomic::AtomicBool,
}
impl OperatorFabric for InterruptedAdmission {
    fn capabilities(&self) -> FabricCapabilities {
        self.inner.capabilities()
    }
    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        Box::pin(async move {
            if matches!(&command, FabricCommand::TransactLog { log, expected: 0, .. } if log.starts_with("static-agent:"))
                && self.fail.swap(false, Ordering::SeqCst)
            {
                return Err(FabricError::Storage(
                    "simulated crash before graph creation".into(),
                ));
            }
            self.inner.execute(command).await
        })
    }
}
#[tokio::test]
async fn recovery_repairs_admission_committed_before_graph_creation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fabric = Arc::new(InterruptedAdmission {
        inner: MemoryFabric::default(),
        fail: true.into(),
    });
    let resources = AgentResources::new(fabric)
        .model("primary", "v1", Arc::new(TextModel(calls.clone())))
        .toolset("workspace", "v1", Arc::new(NoTools));
    let runs = Arc::new(InMemoryRunStore::default());
    let engine = Arc::new(AgentEngine::new(resources.clone(), runs.clone()));
    let run = RunId::new();
    let result = engine
        .start(
            &AgentRunConfig::new("test", ""),
            agent_core::agent::PlannedRun {
                run_id: run,
                input: "recover admission".into(),
                attachments: vec![],
                prior_messages: vec![],
                allowed_tools: None,
                allow_run_adf: false,
                max_steps: None,
                context_fingerprint: None,
                execution_manifest: None,
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(engine);
    let engine = Arc::new(AgentEngine::new(resources, runs));
    let mut started = engine.recover(run).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !started.events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .unwrap();
    assert_eq!(engine.get_run(run).await.unwrap().unwrap().output, "hello");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine.stop_workers().await;
}

struct JsonNode(Arc<AtomicUsize>);
impl agent_core::agent::GraphOperator for JsonNode {
    fn handle(
        &self,
        context: agent_core::agent::GraphContext,
    ) -> agent_core::agent::graph::GraphFuture<'_> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(agent_core::agent::GraphOutput::new(
                serde_json::Value::Null,
                vec![agent_core::agent::GraphEvent::new(
                    "custom.done.v1",
                    json!({"output":format!("value={}",context.event.payload["value"])}),
                )],
            ))
        })
    }
}
#[tokio::test]
async fn application_engine_admits_custom_json_graph_and_recovers_pinned_definition() {
    use agent_core::agent::*;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = OperatorRegistry::default();
    let counted = calls.clone();
    registry.register(OperatorDescriptor::new("test.json", 1)
        .input("custom.start.v1", json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}}))
        .output("custom.done.v1",json!({"type":"object"}))
        .output("custom.failed.v1",json!({"type":"object"}))
        .output("custom.cancelled.v1",json!({"type":"object"})), move |_,_| Ok(Arc::new(JsonNode(counted.clone())))).unwrap();
    let definition = AgentGraph::new("custom")
        .trigger(
            "start",
            agent_core::agent::TriggerDefinition::manual("custom.start.v1"),
        )
        .operator("node", OperatorDefinition::new("test.json", 1, json!({})))
        .subscribe("start", "start", "node", ["custom.start.v1"])
        .lifecycle(GraphLifecycle {
            start: GraphStart {
                trigger: "start".into(),
            },
            terminal: GraphTerminal {
                from: "node".into(),
                completed: "custom.done.v1".into(),
                failed: "custom.failed.v1".into(),
                cancelled: "custom.cancelled.v1".into(),
            },
        })
        .limits(GraphLimits::new(10000, 100))
        .finish()
        .unwrap();
    let resources = AgentResources::new(Arc::new(MemoryFabric::default()));
    let runs = Arc::new(InMemoryRunStore::default());
    let engine = Arc::new(AgentEngine::with_registry(
        resources.clone(),
        runs.clone(),
        registry.clone(),
    ));
    let run = RunId::new();
    let plan = || PlannedRun {
        run_id: run,
        input: "custom JSON request".into(),
        attachments: vec![],
        prior_messages: vec![],
        allowed_tools: None,
        allow_run_adf: false,
        max_steps: None,
        context_fingerprint: None,
        execution_manifest: None,
    };
    assert!(
        engine
            .admit_graph(definition.clone(), json!({"value":"invalid"}), plan(), None)
            .await
            .is_err()
    );
    assert!(engine.get_run(run).await.unwrap().is_none());
    let mut malformed = definition.clone();
    malformed.operators.clear();
    assert!(
        malformed
            .validate_start(&registry, &json!({"value":42}))
            .is_err()
    );
    engine
        .admit_graph(definition.clone(), json!({"value":42}), plan(), None)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut changed = definition.clone();
    changed.name = "changed".into();
    assert!(
        engine
            .admit_graph(changed, json!({"value":42}), plan(), None)
            .await
            .is_err()
    );
    assert!(
        engine
            .admit_graph(definition.clone(), json!({"value":43}), plan(), None)
            .await
            .is_err()
    );
    drop(engine);
    let reopened = Arc::new(AgentEngine::with_registry(
        resources.clone(),
        runs.clone(),
        registry.clone(),
    ));
    let mut started = reopened.recover(run).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !started.events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .unwrap();
    assert_eq!(
        reopened.get_run(run).await.unwrap().unwrap().input,
        "custom JSON request"
    );
    assert_eq!(
        reopened.get_run(run).await.unwrap().unwrap().output,
        "value=42"
    );
    let events = reopened.events_after(run, 0, 128).await.unwrap();
    reopened.stop_workers().await;
    let again = Arc::new(AgentEngine::with_registry(resources, runs, registry));
    let mut replay = again.recover(run).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !replay.events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .unwrap();
    assert_eq!(again.events_after(run, 0, 128).await.unwrap(), events);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        again
            .trigger_graph(definition.clone(), "missing", json!({"value":44}))
            .await
            .is_err()
    );
    let mut triggered = again
        .trigger_graph(definition, "start", json!({"value":44}))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !triggered.events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .unwrap();
    assert_eq!(
        again
            .get_run(triggered.run_id)
            .await
            .unwrap()
            .unwrap()
            .output,
        "value=44"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    again.stop_workers().await;
}
