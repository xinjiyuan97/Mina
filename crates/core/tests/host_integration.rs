use agent_core::{
    agent::memory_fabric::MemoryFabric, agent::*, contracts::fabric::*, contracts::host::*,
    contracts::*,
};
use agent_extension::store::SqliteEventStore;
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
struct Text(Arc<AtomicUsize>);
impl ModelPort for Text {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.model, "approved");
        assert_eq!(request.max_output_tokens, Some(128));
        Box::pin(futures_util::stream::iter([
            ModelEvent::TextDelta {
                delta: "done".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}
fn policy() -> HostPolicy {
    HostPolicy {
        allowed_tools: BTreeSet::new(),
        allowed_models: BTreeSet::from(["approved".into()]),
        max_steps: 3,
        max_output_tokens: 128,
        timeout_ms: 60000,
        review_level: 10,
        allow_run_adf: false,
    }
}
fn template() -> AgentDefinition {
    let mut d =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/text.yaml")).unwrap();
    d.schema_version = 3;
    d.operators.get_mut("context").unwrap().version = 2;
    d.bindings = Some(BindingContract {
        id: "assistant".into(),
        version: 1,
        schema: json!({"type":"object","additionalProperties":false,"required":["prompt","model","context"],"properties":{"prompt":{"type":"string"},"model":{"type":"string"},"context":{"type":"object","additionalProperties":false,"properties":{"prior_messages":{"type":"array"}}}}}),
    });
    d.operators.get_mut("context").unwrap().config["system_prompt"] = json!({"$binding":"prompt"});
    d.operators.get_mut("context").unwrap().config["context"] = json!({"$binding":"context"});
    d.operators.get_mut("llm").unwrap().config["model_name"] = json!({"$binding":"model"});
    d
}
fn inputs() -> HostBindings {
    HostBindings {
        contract_id: "assistant".into(),
        version: 1,
        values: [
            ("prompt".into(), json!("host prompt")),
            ("model".into(), json!("approved")),
            ("context".into(), json!({"prior_messages":[]})),
        ]
        .into(),
    }
}
fn resources(
    f: Arc<dyn OperatorFabric>,
    calls: Arc<AtomicUsize>,
    revision: &str,
) -> AgentResources {
    AgentResources::new(f).model("primary", revision, Arc::new(Text(calls)))
}
#[test]
fn bindings_are_typed_versioned_and_name_independent() {
    let d = template();
    let mut renamed = d.clone();
    let op = renamed.operators.remove("llm").unwrap();
    renamed.operators.insert("answer".into(), op);
    for s in &mut renamed.subscriptions {
        if s.subscriber == "llm" {
            s.subscriber = "answer".into();
        }
        if s.from == "llm" {
            s.from = "answer".into();
        }
    }
    renamed.lifecycle.terminal.from = "answer".into();
    let d = d.bind(inputs(), policy()).unwrap();
    let renamed = renamed.bind(inputs(), policy()).unwrap();
    assert_eq!(
        d.operators["llm"].config,
        renamed.operators["answer"].config
    );
    assert_eq!(d.operators["context"].config["max_steps"], 3);
    assert_eq!(d.operators["llm"].config["max_output_tokens"], 128);
    let calls = Arc::new(AtomicUsize::new(0));
    let r = resources(Arc::new(MemoryFabric::default()), calls.clone(), "v1");
    d.validate(&OperatorRegistry::builtins(), &r).unwrap();
    let mut missing = inputs();
    missing.values.remove("prompt");
    assert!(template().bind(missing, policy()).is_err());
    let mut wrong = inputs();
    wrong.values.insert("prompt".into(), json!(1));
    assert!(template().bind(wrong, policy()).is_err());
    let mut wrong = inputs();
    wrong.version = 2;
    assert!(template().bind(wrong, policy()).is_err());
    let mut forbidden = inputs();
    forbidden.values.insert("model".into(), json!("unapproved"));
    assert!(template().bind(forbidden, policy()).is_err());
    assert!(
        template()
            .validate(&OperatorRegistry::builtins(), &r)
            .is_err()
    );
    let mut tampered = d;
    tampered.operators.get_mut("llm").unwrap().config["max_output_tokens"] = json!(10000);
    assert!(
        tampered
            .validate(&OperatorRegistry::builtins(), &r)
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
async fn roundtrip(source: Arc<dyn OperatorFabric>, target: Arc<dyn OperatorFabric>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = template().bind(inputs(), policy()).unwrap();
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        resources(source.clone(), calls.clone(), "v1"),
    )
    .build(definition.clone())
    .unwrap();
    let run = RunId::new();
    agent
        .engine
        .admit("local", run, json!("hello"))
        .await
        .unwrap();
    let manager = RunManager::new("local", source.clone());
    let page = manager.inspect_run(run, None, 1).await.unwrap();
    assert!(page.next_cursor.is_some());
    let other = RunId::new();
    assert!(
        manager
            .inspect_run(other, page.next_cursor.as_deref(), 1)
            .await
            .is_err()
    );
    assert!(manager.export_run(run, "missing").await.is_err());
    manager
        .begin_maintenance("source-fence", i64::MAX - 1)
        .await
        .unwrap();
    assert!(
        source
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: "blocked".into(),
                    expected: None,
                    value: json!(1)
                },
                outbox: vec![],
                delivery: None
            })
            .await
            .is_err()
    );
    let bundle = manager.export_run(run, "source-fence").await.unwrap();
    bundle.verify().unwrap();
    let mut damaged = serde_json::to_value(&bundle).unwrap();
    damaged["purpose"] = json!("diagnostic");
    let damaged: agent_core::contracts::run_archive::RunBundle =
        serde_json::from_value(damaged).unwrap();
    assert!(damaged.verify().is_err());
    let target_manager = RunManager::new("local", target.clone());
    let registry = OperatorRegistry::builtins();
    let good = resources(target.clone(), calls.clone(), "v1");
    let bad = resources(target.clone(), calls.clone(), "v2");
    assert!(
        target_manager
            .validate_import(&bundle, &registry, &bad, Some(&policy()))
            .is_err()
    );
    target_manager
        .begin_maintenance("target-fence", i64::MAX - 1)
        .await
        .unwrap();
    target_manager
        .import_run(
            bundle.clone(),
            "target-fence",
            &registry,
            &good,
            Some(&policy()),
        )
        .await
        .unwrap();
    target_manager
        .import_run(bundle, "target-fence", &registry, &good, Some(&policy()))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    target_manager
        .end_maintenance("target-fence")
        .await
        .unwrap();
    let target_agent = AgentBuilder::new(registry, good).build(definition).unwrap();
    target_agent.engine.recover("local", run).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !target_manager
                .list_completions(0, 10)
                .await
                .unwrap()
                .items
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "run={:?}, dlq={:?}",
        target_agent.engine.get_run("local", run).await,
        target
            .execute(FabricCommand::DeadLetters {
                after: 0,
                limit: 128
            })
            .await
    );
    target_agent.engine.stop_workers().await;
    let completed = target_manager.list_completions(0, 10).await.unwrap();
    assert_eq!(completed.items.len(), 1);
    assert_eq!(completed.items[0].1.run_id, run);
    let id = &completed.items[0].1.completion_id;
    target_manager
        .acknowledge_completion("session", 1, id)
        .await
        .unwrap();
    target_manager
        .acknowledge_completion("session", 1, id)
        .await
        .unwrap();
    assert_eq!(
        target_manager.completion_offset("session").await.unwrap(),
        1
    );
    assert_eq!(
        target_manager.completion_offset("profile").await.unwrap(),
        0
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let later = manager
        .inspect_run(run, page.next_cursor.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(page.snapshot_id, later.snapshot_id);
    assert!(
        RunManager::new("other", source)
            .inspect_run(run, None, 1)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn memory_archive_restores_once_with_completion_projection() {
    roundtrip(
        Arc::new(MemoryFabric::default()),
        Arc::new(MemoryFabric::default()),
    )
    .await;
}
#[tokio::test]
async fn sqlite_archive_restores_once_with_completion_projection() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(
        SqliteEventStore::open(dir.path().join("source.db"), "source")
            .await
            .unwrap(),
    );
    let target = Arc::new(
        SqliteEventStore::open(dir.path().join("target.db"), "target")
            .await
            .unwrap(),
    );
    roundtrip(source, target).await;
}
async fn host_atomic(fabric: Arc<dyn OperatorFabric>) {
    let record = HostRecord {
        tenant: "local".into(),
        namespace: "session".into(),
        run_id: None,
        operation_id: "finalize-1".into(),
        schema_version: 1,
        payload: json!({"message":"done"}),
    };
    let write = |expected| FabricCommand::Transact {
        checkpoint: CheckpointWrite {
            key: "agent-state".into(),
            expected,
            value: json!(1),
        },
        outbox: vec![],
        delivery: None,
    };
    let command = |transaction| FabricCommand::HostTransact {
        records: vec![record.clone()],
        completion: None,
        transaction: Box::new(transaction),
    };
    assert!(fabric.execute(command(write(Some(4)))).await.is_err());
    let manager = RunManager::new("local", fabric.clone());
    assert!(
        manager
            .read_host_record("session", "finalize-1")
            .await
            .unwrap()
            .is_none()
    );
    fabric.execute(command(write(None))).await.unwrap();
    assert_eq!(
        manager
            .read_host_record("session", "finalize-1")
            .await
            .unwrap(),
        Some(record.clone())
    );
    let mut conflict = record;
    conflict.payload = json!("different");
    assert!(
        fabric
            .execute(FabricCommand::HostTransact {
                records: vec![conflict],
                completion: None,
                transaction: Box::new(write(Some(0)))
            })
            .await
            .is_err()
    );
    let FabricReply::Checkpoint(Some(saved)) = fabric
        .execute(FabricCommand::ReadCheckpoint("agent-state".into()))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(saved.version, 0);
}
#[tokio::test]
async fn host_records_rollback_in_both_adapters() {
    host_atomic(Arc::new(MemoryFabric::default())).await;
    let dir = tempfile::tempdir().unwrap();
    host_atomic(Arc::new(
        SqliteEventStore::open(dir.path().join("host.db"), "test")
            .await
            .unwrap(),
    ))
    .await;
}

#[tokio::test]
async fn archive_includes_blob_contents_and_rejects_missing_dependencies() {
    let source = Arc::new(MemoryFabric::default());
    let blobs = agent_core::agent::artifacts::FabricBlobStore(source.clone());
    let blob = blobs
        .put(PutBlob {
            media_type: "text/plain".into(),
            name: None,
            data: b"attachment".to_vec(),
            created_at_ms: 0,
        })
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let d = template().bind(inputs(), policy()).unwrap();
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        resources(source.clone(), calls.clone(), "v1"),
    )
    .build(d)
    .unwrap();
    let run = RunId::new();
    let attachment = ModelAttachment {
        blob_id: blob.blob_id,
        media_type: "text/plain".into(),
        name: None,
        size_bytes: Some(blob.size_bytes),
    };
    agent
        .engine
        .admit(
            "local",
            run,
            json!({"input":"hello","attachments":[attachment]}),
        )
        .await
        .unwrap();
    let source_manager = RunManager::new("local", source.clone());
    source_manager.begin_maintenance("source", 0).await.unwrap();
    let bundle = source_manager.export_run(run, "source").await.unwrap();
    let target = Arc::new(MemoryFabric::default());
    let manager = RunManager::new("local", target.clone());
    manager.begin_maintenance("target", 0).await.unwrap();
    manager
        .import_run(
            bundle,
            "target",
            &OperatorRegistry::builtins(),
            &resources(target.clone(), calls, "v1"),
            Some(&policy()),
        )
        .await
        .unwrap();
    assert_eq!(
        agent_core::agent::artifacts::FabricBlobStore(target)
            .get(blob.blob_id)
            .await
            .unwrap()
            .unwrap()
            .data,
        b"attachment"
    );
    source_manager.end_maintenance("source").await.unwrap();
    let key = format!("pipeline-artifact:{}", blob.blob_id);
    let FabricReply::Checkpoint(Some(saved)) = source
        .execute(FabricCommand::ReadCheckpoint(key.clone()))
        .await
        .unwrap()
    else {
        panic!()
    };
    let mut value = saved.value;
    value["complete"] = json!(false);
    source
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key,
                expected: Some(saved.version),
                value,
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    source_manager.begin_maintenance("source", 0).await.unwrap();
    assert!(source_manager.export_run(run, "source").await.is_err());
}

#[test]
fn shipped_graph_and_recovery_resources_enforce_current_host_policy() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fabric = Arc::new(MemoryFabric::default());
    let definition =
        AgentDefinition::from_yaml(include_str!("../../../config/agents/host-bound.yaml")).unwrap();
    let bindings = HostBindings {
        contract_id: "mina.assistant".into(),
        version: 1,
        values: [
            ("system_prompt".into(), json!("prompt")),
            ("context".into(), json!({})),
            ("model_name".into(), json!("approved")),
            ("max_output_tokens".into(), json!(128)),
        ]
        .into(),
    };
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        resources(fabric.clone(), calls.clone(), "v1"),
    )
    .build_with_bindings(definition, bindings, policy())
    .unwrap();
    let mut tighter = policy();
    tighter.max_output_tokens = 32;
    assert!(
        agent
            .engine
            .definition()
            .validate(
                &OperatorRegistry::builtins(),
                &resources(fabric, calls.clone(), "v1").with_host_policy(tighter)
            )
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut graph = GraphAgentConfig::new("approved", "prompt")
        .definition()
        .unwrap();
    for name in ["context", "policy", "tools"] {
        graph.operators.get_mut(name).unwrap().config["allow_run_adf"] = json!(true);
        graph.operators.get_mut(name).unwrap().config["allowed_tools"] = json!(["forbidden"]);
    }
    graph.operators.get_mut("policy").unwrap().config["review_level"] = json!(100);
    graph.operators.get_mut("tools").unwrap().config["review_level"] = json!(90);
    graph.apply_host_policy(&policy()).unwrap();
    for name in ["context", "policy", "tools"] {
        assert_eq!(graph.operators[name].config["allow_run_adf"], false);
        assert_eq!(graph.operators[name].config["allowed_tools"], json!([]));
    }
    assert_eq!(graph.operators["policy"].config["review_level"], 10);
    assert_eq!(graph.operators["tools"].config["review_level"], 10);
}

async fn history_contract(fabric: Arc<dyn OperatorFabric>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let build = |id: &str, name: &str| {
        let mut definition = template();
        definition.name = name.into();
        AgentBuilder::new(
            OperatorRegistry::builtins(),
            resources(fabric.clone(), calls.clone(), "v1").with_agent_id(id),
        )
        .build_with_bindings(definition, inputs(), policy())
        .unwrap()
    };
    let agent = build("assistant", "original-graph");
    let manager = RunManager::new("local", fabric.clone());
    let waiting = RunId::new();
    agent
        .engine
        .admit("local", waiting, json!("not triggered yet"))
        .await
        .unwrap();
    let blobs = agent_core::agent::artifacts::FabricBlobStore(fabric.clone());
    let blob = blobs
        .put(PutBlob {
            media_type: "text/plain".into(),
            name: Some("input.txt".into()),
            data: b"history attachment".to_vec(),
            created_at_ms: 0,
        })
        .await
        .unwrap();
    let completed = RunId::new();
    agent.engine.start("local", completed, json!({"input":"hello", "attachments":[{"blob_id":blob.blob_id,"media_type":"text/plain","size_bytes":blob.size_bytes}]})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while manager.completion(completed).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    agent.engine.stop_workers().await;
    for (run, cancel) in [(RunId::new(), false), (RunId::new(), true)] {
        agent
            .engine
            .admit("local", run, json!("terminal"))
            .await
            .unwrap();
        if cancel {
            agent.engine.cancel("local", run).await.unwrap();
        } else {
            agent
                .engine
                .fail("local", run, "test_failure")
                .await
                .unwrap();
        }
    }
    let renamed = build("assistant", "renamed-graph");
    let renamed_run = RunId::new();
    renamed
        .engine
        .admit("local", renamed_run, json!("renamed"))
        .await
        .unwrap();
    renamed.engine.cancel("local", renamed_run).await.unwrap();
    let other = build("different-agent", "original-graph");
    let other_run = RunId::new();
    other
        .engine
        .admit("local", other_run, json!("private"))
        .await
        .unwrap();
    other.engine.cancel("local", other_run).await.unwrap();
    let other_tenant = RunId::new();
    agent
        .engine
        .admit("other-tenant", other_tenant, json!("private tenant"))
        .await
        .unwrap();
    agent
        .engine
        .cancel("other-tenant", other_tenant)
        .await
        .unwrap();
    let history = manager.export_agent_history("assistant").await.unwrap();
    history.verify().unwrap();
    assert_eq!(history.runs.len(), 4);
    assert!(
        history
            .runs
            .iter()
            .all(|r| r.run_id != waiting && r.run_id != other_run && r.run_id != other_tenant)
    );
    let finished = history.runs.iter().find(|r| r.run_id == completed).unwrap();
    assert_eq!(finished.final_state["status"], "completed");
    assert_eq!(finished.completion.as_ref().unwrap().run_id, completed);
    assert!(!finished.routed_events.is_empty());
    assert_eq!(finished.events_through as usize, finished.events.len());
    assert_eq!(finished.blobs.len(), 1);
    assert_eq!(finished.blobs[0].metadata.blob_id, blob.blob_id);
    assert_eq!(finished.blobs[0].base64, "aGlzdG9yeSBhdHRhY2htZW50");
    assert!(
        serde_json::from_value::<agent_core::contracts::run_archive::RunBundle>(
            serde_json::to_value(&history).unwrap()
        )
        .is_err()
    );
    let mut tampered = history.clone();
    tampered.runs.pop();
    assert!(tampered.verify().is_err());
    // Export never fences a running Run, and later completion cannot change this archive.
    agent.engine.cancel("local", waiting).await.unwrap();
    assert_eq!(history.runs.len(), 4);
    assert_eq!(
        manager
            .export_agent_history("assistant")
            .await
            .unwrap()
            .runs
            .len(),
        5
    );
    assert!(
        manager
            .register_run("different-agent", completed)
            .await
            .is_err()
    );
    assert!(other.engine.recover_run("local", completed).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Missing referenced data must reject the complete-history claim.
    let key = format!("pipeline-artifact:{}", blob.blob_id);
    let FabricReply::Checkpoint(Some(saved)) = fabric
        .execute(FabricCommand::ReadCheckpoint(key.clone()))
        .await
        .unwrap()
    else {
        panic!()
    };
    let mut value = saved.value;
    value["complete"] = json!(false);
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key,
                expected: Some(saved.version),
                value,
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    assert!(manager.export_agent_history("assistant").await.is_err());
}

#[tokio::test]
async fn agent_history_contains_only_terminal_runs_in_memory_and_sqlite() {
    history_contract(Arc::new(MemoryFabric::default())).await;
    let dir = tempfile::tempdir().unwrap();
    history_contract(Arc::new(
        SqliteEventStore::open(dir.path().join("history.db"), "history")
            .await
            .unwrap(),
    ))
    .await;
}

#[tokio::test]
async fn legacy_run_registration_is_explicit_and_immutable() {
    let fabric = Arc::new(MemoryFabric::default());
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        resources(fabric.clone(), Arc::new(AtomicUsize::new(0)), "v1"),
    )
    .build_with_bindings(template(), inputs(), policy())
    .unwrap();
    let run = RunId::new();
    agent
        .engine
        .admit("local", run, json!("legacy"))
        .await
        .unwrap();
    agent.engine.cancel("local", run).await.unwrap();
    let manager = RunManager::new("local", fabric.clone());
    assert!(
        manager
            .export_agent_history("assistant")
            .await
            .unwrap()
            .runs
            .is_empty()
    );
    manager.register_run("assistant", run).await.unwrap();
    manager.register_run("assistant", run).await.unwrap();
    assert_eq!(
        manager
            .export_agent_history("assistant")
            .await
            .unwrap()
            .runs
            .len(),
        1
    );
    assert!(manager.register_run("another", run).await.is_err());
    let key = agent_core::agent::graph::run_checkpoint_key("local", run);
    let FabricReply::Checkpoint(Some(saved)) = fabric
        .execute(FabricCommand::ReadCheckpoint(key.clone()))
        .await
        .unwrap()
    else {
        panic!()
    };
    let mut value = saved.value;
    value["event_count"] = json!(999);
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: key.clone(),
                expected: Some(saved.version),
                value,
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    assert!(manager.export_agent_history("assistant").await.is_err());
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key,
                expected: Some(saved.version + 1),
                value: json!({"retired":true}),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    assert!(manager.export_agent_history("assistant").await.is_err());
}

struct PendingHistoryModel(Arc<AtomicUsize>);
impl ModelPort for PendingHistoryModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures_util::stream::pending())
    }
}

#[tokio::test]
async fn history_export_does_not_wait_for_an_active_model_or_its_lease() {
    let dir = tempfile::tempdir().unwrap();
    let fabrics: Vec<Arc<dyn OperatorFabric>> = vec![
        Arc::new(MemoryFabric::default()),
        Arc::new(
            SqliteEventStore::open(dir.path().join("active.db"), "active")
                .await
                .unwrap(),
        ),
    ];
    for fabric in fabrics {
        let calls = Arc::new(AtomicUsize::new(0));
        let agent = AgentBuilder::new(
            OperatorRegistry::builtins(),
            AgentResources::new(fabric.clone())
                .with_agent_id("assistant")
                .model(
                    "primary",
                    "v1",
                    Arc::new(PendingHistoryModel(calls.clone())),
                ),
        )
        .build_with_bindings(template(), inputs(), policy())
        .unwrap();
        let terminal = RunId::new();
        agent
            .engine
            .admit("local", terminal, json!("cancelled"))
            .await
            .unwrap();
        agent.engine.cancel("local", terminal).await.unwrap();
        let active = RunId::new();
        agent
            .engine
            .start("local", active, json!("active"))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let manager = RunManager::new("local", fabric);
        let archive = tokio::time::timeout(
            Duration::from_secs(2),
            manager.export_agent_history("assistant"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(archive.runs.len(), 1);
        assert_eq!(archive.runs[0].run_id, terminal);
        assert_eq!(
            agent.engine.get_run("local", active).await.unwrap().status,
            GraphStatus::Running
        );
        agent.engine.cancel("local", active).await.unwrap();
        agent.engine.stop_workers().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
