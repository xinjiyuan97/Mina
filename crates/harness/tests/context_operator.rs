use agent_core::{
    context::*, context_operator::*, harness::RunId, operator_fabric::*, skill::SkillLock,
};
use agent_extension::store::{FilesystemBlobStore, SqliteEventStore};
use agent_harness::context_operator::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Preparer(AtomicUsize);

struct ExcessiveReferences;
impl ContextPreparer for ExcessiveReferences {
    fn prepare(&self, request: ContextRequest) -> PrepareFuture<'_> {
        Box::pin(async move {
            let mut prepared = Preparer(AtomicUsize::new(0)).prepare(request).await?;
            prepared.pack.artifact_refs = (0..129)
                .map(|_| ContextArtifactRef {
                    artifact_id: ContextArtifactId::new(),
                    content_digest: "digest".into(),
                })
                .collect();
            Ok(prepared)
        })
    }
}

#[tokio::test]
async fn p5_excessive_artifact_references_fail_context_durably() {
    let directory = tempfile::tempdir().expect("temp");
    let store = Arc::new(
        SqliteEventStore::open(directory.path().join("events.db"), "p5")
            .await
            .expect("open"),
    );
    let facade = AdmissionFacade::new(
        store,
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs"))),
        Arc::new(ExcessiveReferences),
    );
    let request = request();
    facade.publish(request.clone()).await.expect("publish");
    facade.tick().await.expect("tick");
    let Some(ContextOutcome::Failed(failure)) =
        facade.outcome(request.run_id).await.expect("outcome")
    else {
        panic!("must fail context")
    };
    assert_eq!(failure.code, "context_resource_limit");
}
impl ContextPreparer for Preparer {
    fn prepare(&self, _request: ContextRequest) -> PrepareFuture<'_> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(PreparedContext {
                pack: ContextPack {
                    messages: vec![],
                    memory_refs: vec![],
                    artifact_refs: vec![],
                    skill_refs: vec![],
                    effective_tools: vec![],
                    budget: ContextBudgetReport {
                        available_input_tokens: 100,
                        estimated_input_tokens: 0,
                        estimator_confidence: 1.0,
                        retained_item_ids: vec![],
                        dropped: vec![],
                    },
                    component_descriptors: vec![],
                    fingerprint: "fingerprint".into(),
                },
                skill_lock: SkillLock::default(),
                request_digest: "digest".into(),
            })
        })
    }
}

fn request() -> ContextRequest {
    ContextRequest {
        engine_version: Default::default(),
        execution_profile: None,
        run_id: RunId::new(),
        session_id: None,
        through_message_ordinal: None,
        request_hash: "hash".into(),
        input: "hello".into(),
        attachments: vec![],
        explicit_skills: vec![],
        max_steps: 10,
        policy: ContextPolicy {
            default_skills: vec![],
            max_skills: 8,
            memory_enabled: false,
            memory_scope: "test".into(),
            max_memories: 8,
            agent_profile: "test".into(),
            agent_instruction: "instruction".into(),
            model_profile: "test".into(),
            available_tools: vec![],
            budget: ContextBudget {
                model_context_tokens: 1000,
                reserved_output_tokens: 100,
                reserved_tool_schema_tokens: 0,
                max_skill_tokens: 100,
                max_memory_tokens: 100,
                max_history_tokens: 100,
            },
            policy_version: 1,
            allow_run_adf: false,
            script_runtime: serde_json::Value::Null,
        },
    }
}

#[tokio::test]
async fn p2_durable_request_survives_waiter_cancel_and_sqlite_reopen() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("events.sqlite");
    let blobs = Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let preparer = Arc::new(Preparer(AtomicUsize::new(0)));
    let request = request();
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("open"));
        let facade = Arc::new(AdmissionFacade::new(
            fabric,
            blobs.clone(),
            preparer.clone(),
        ));
        facade
            .publish(request.clone())
            .await
            .expect("durable publish");
        assert_eq!(preparer.0.load(Ordering::SeqCst), 0);
        let waiting = {
            let facade = facade.clone();
            let run_id = request.run_id;
            tokio::spawn(async move { facade.wait(run_id, Duration::from_secs(60)).await })
        };
        waiting.abort();
        assert!(waiting.await.expect_err("cancelled").is_cancelled());
    }
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("reopen"));
        let facade = AdmissionFacade::new(fabric, blobs.clone(), preparer.clone());
        facade
            .publish(request.clone())
            .await
            .expect("idempotent publish");
        assert_eq!(facade.tick().await.expect("consume").committed, 1);
        let prepared = facade
            .wait(request.run_id, Duration::from_secs(1))
            .await
            .expect("result");
        assert_eq!(prepared.pack.fingerprint, "fingerprint");
        assert_eq!(facade.tick().await.expect("duplicate tick").committed, 0);
    }
    let fabric = Arc::new(
        SqliteEventStore::open(&path, "test")
            .await
            .expect("second reopen"),
    );
    let facade = AdmissionFacade::new(fabric, blobs, preparer.clone());
    assert!(
        facade
            .wait(request.run_id, Duration::from_secs(1))
            .await
            .is_ok()
    );
    let mut conflict = request;
    conflict.request_hash = "different".into();
    assert!(matches!(
        facade.publish(conflict).await,
        Err(FabricError::Conflict)
    ));
    assert_eq!(preparer.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn p2_timeout_does_not_remove_durable_request() {
    let directory = tempfile::tempdir().expect("directory");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("state.sqlite"), "test")
            .await
            .expect("open"),
    );
    let facade = AdmissionFacade::new(
        fabric,
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs"))),
        Arc::new(Preparer(AtomicUsize::new(0))),
    );
    let request = request();
    facade.publish(request.clone()).await.expect("publish");
    assert!(matches!(
        facade.wait(request.run_id, Duration::ZERO).await,
        Err(AdmissionError::Timeout)
    ));
    facade.tick().await.expect("continue durable work");
    assert!(
        facade
            .wait(request.run_id, Duration::from_secs(1))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn p2_receipt_outbox_reopen_and_duplicate_delivery_commit_once() {
    use agent_harness::operator_fabric::FabricHandler;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("events.sqlite");
    let blobs = Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let preparer = Arc::new(Preparer(AtomicUsize::new(0)));
    let request = request();
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("open"));
        let facade = AdmissionFacade::new(fabric.clone(), blobs.clone(), preparer.clone());
        facade.publish(request.clone()).await.expect("publish");
        fabric
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: CONTEXT_CONSUMER.into(),
                subscription: "duplicate".into(),
                topic: CONTEXT_TOPIC.into(),
                event_type: Some(CONTEXT_REQUESTED.into()),
                tenant: Some("local".into()),
            }))
            .await
            .expect("subscribe");
        let now = agent_harness::unix_time_ms();
        let FabricReply::Claimed(mut deliveries) = fabric
            .execute(FabricCommand::Claim {
                worker: "test".into(),
                now_ms: now,
                lease_ms: 300_000,
                limit: 1,
                max_attempts: 10,
            })
            .await
            .expect("claim")
        else {
            panic!("claim reply");
        };
        let delivery = deliveries.remove(0);
        let lease = delivery.lease.clone();
        let output = facade
            .context_operator()
            .handle(delivery)
            .await
            .expect("prepare");
        fabric
            .execute(FabricCommand::Commit {
                lease: lease.clone(),
                now_ms: now,
                result: output.result.clone(),
                outbox: output.outbox.clone(),
            })
            .await
            .expect("atomic commit");
        fabric
            .execute(FabricCommand::Commit {
                lease,
                now_ms: now,
                result: output.result,
                outbox: output.outbox,
            })
            .await
            .expect("duplicate commit");
        assert!(
            facade
                .outcome(request.run_id)
                .await
                .expect("not dispatched")
                .is_none()
        );
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("reopen"));
    let facade = AdmissionFacade::new(fabric.clone(), blobs, preparer.clone());
    let report = facade.tick().await.expect("dispatch and receipt reuse");
    assert_eq!(report.reused, 1);
    assert_eq!(preparer.0.load(Ordering::SeqCst), 1);
    assert!(
        facade
            .wait(request.run_id, Duration::from_secs(1))
            .await
            .is_ok()
    );
    let FabricReply::Replay(events) = fabric
        .execute(FabricCommand::ScanEvents {
            topic: CONTEXT_TOPIC.into(),
            event_type: CONTEXT_COMPLETED.into(),
            after: 0,
            limit: 128,
        })
        .await
        .expect("scan")
    else {
        panic!("scan reply");
    };
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].event.envelope.engine_version,
        agent_core::operator::EngineVersion::LegacyMachine
    );
}

#[tokio::test]
async fn p2_context_matches_legacy_input_fingerprint_skill_lock_and_memory() {
    use agent_core::{harness::*, memory::*, skill::*};
    use agent_extension::store::{FilesystemSkillStore, InMemoryMemoryStore, InMemoryRunStore};
    use sha2::{Digest, Sha256};
    let directory = tempfile::tempdir().expect("directory");
    let package = directory.path().join("skills/test");
    std::fs::create_dir_all(&package).expect("skill directory");
    std::fs::write(
        package.join("skill.toml"),
        "skill_id = 'test'\nversion = '1'\ndescription = 'test'\nrequired_tools = ['read']\n",
    )
    .expect("manifest");
    std::fs::write(package.join("SKILL.md"), "Use concise answers.").expect("instruction");
    let skills = Arc::new(SkillOrchestrator::new(Arc::new(FilesystemSkillStore::new(
        directory.path().join("skills"),
        "test-skills",
    ))));
    let sessions = Arc::new(InMemoryRunStore::default());
    let session_id = SessionId::new();
    sessions
        .create_session(CreateSession {
            session_id,
            agent_profile: "test".into(),
            title: None,
            created_at_ms: 1,
        })
        .await
        .expect("session");
    let memories = Arc::new(InMemoryMemoryStore::default());
    memories
        .put(PutMemory {
            record: MemoryRecord {
                memory_id: MemoryId::new(),
                scope: MemoryScope("test".into()),
                kind: MemoryKind::Semantic,
                content: vec![ContentPart::text("hello concise answers")],
                source_refs: vec![MemorySourceRef::ExplicitUserInput { run_id: None }],
                confidence: 1.0,
                salience: 1.0,
                version: 1,
                expires_at_ms: None,
                supersedes: None,
                created_at_ms: 1,
            },
            expected_absent: true,
        })
        .await
        .expect("memory");
    let context = ContextEngine::new(
        sessions,
        memories,
        Arc::new(SlidingWindowCompressor::new(Arc::new(
            HeuristicTokenEstimator,
        ))),
        Arc::new(InMemoryContextArtifactStore::default()),
    );
    let mut request = request();
    request.session_id = Some(session_id);
    request.through_message_ordinal = Some(0);
    request.explicit_skills = vec![(SkillId("test".into()), "1".into())];
    request.policy.memory_enabled = true;
    request.policy.available_tools = vec!["read".into(), "write".into()];
    let resolved = skills
        .resolve(ResolveSkillsRequest {
            explicit: request.explicit_skills.clone(),
            profile_defaults: vec![],
            current_input: request.input.clone(),
            max_skills: 8,
        })
        .await
        .expect("legacy skills");
    let skill_lock = compile_skills(&resolved).skill_lock;
    let mut digest = Sha256::new();
    digest.update(request.input.as_bytes());
    for skill in &resolved {
        digest.update(skill.locked.skill_id.0.as_bytes());
        digest.update(skill.locked.version.as_bytes());
        digest.update(skill.locked.digest.as_bytes());
    }
    let request_digest = format!("sha256:{:x}", digest.finalize());
    let legacy = context
        .build(ContextBuildRequest {
            run_id: request.run_id,
            session_id: request.session_id,
            through_message_ordinal: request.through_message_ordinal,
            current_input: request.input.clone(),
            request_digest: request_digest.clone(),
            agent_profile: request.policy.agent_profile.clone(),
            agent_instruction: request.policy.agent_instruction.clone(),
            model_profile: request.policy.model_profile.clone(),
            resolved_skills: resolved,
            memory_scopes: vec![
                MemoryScope("test".into()),
                MemoryScope(format!("session:{session_id}")),
            ],
            available_tools: request.policy.available_tools.clone(),
            max_memories: request.policy.max_memories,
            budget: request.policy.budget.clone(),
            policy_version: request.policy.policy_version,
            now_ms: agent_harness::unix_time_ms(),
        })
        .await
        .expect("legacy pack");
    assert!(!legacy.memory_refs.is_empty());
    let preparer = Arc::new(AgentContextPreparer { context, skills });
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("events.sqlite"), "test")
            .await
            .expect("open"),
    );
    let facade = AdmissionFacade::new(
        fabric,
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs"))),
        preparer,
    );
    facade.publish(request.clone()).await.expect("publish");
    facade.tick().await.expect("prepare");
    let prepared = facade
        .wait(request.run_id, Duration::from_secs(1))
        .await
        .expect("barrier");
    assert_eq!(prepared.pack, legacy);
    assert_eq!(prepared.skill_lock, skill_lock);
    assert_eq!(prepared.request_digest, request_digest);
    let plan = legacy_plan(request, prepared);
    assert_eq!(plan.prior_messages, legacy.messages);
    assert_eq!(plan.allowed_tools, Some(legacy.effective_tools));
    assert_eq!(plan.context_fingerprint, Some(legacy.fingerprint));
}

#[tokio::test]
async fn p2_failure_is_a_committed_fact_not_a_retry() {
    struct Failure;
    impl ContextPreparer for Failure {
        fn prepare(&self, _: ContextRequest) -> PrepareFuture<'_> {
            Box::pin(async {
                Err(ContextFailed {
                    code: "skill_resolution_failed".into(),
                    message: "missing skill".repeat(10_000),
                })
            })
        }
    }
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("events.sqlite");
    let blobs = Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let request = request();
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("open"));
        let facade = AdmissionFacade::new(fabric, blobs.clone(), Arc::new(Failure));
        facade.publish(request.clone()).await.expect("publish");
        let report = facade.tick().await.expect("commit failure");
        assert_eq!(report.committed, 1);
        assert_eq!(report.retried, 0);
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("reopen"));
    let preparer = Arc::new(Preparer(AtomicUsize::new(0)));
    let facade = AdmissionFacade::new(fabric, blobs, preparer.clone());
    assert!(
        matches!(facade.wait(request.run_id, Duration::from_secs(1)).await, Err(AdmissionError::Preparation(ContextFailed { code, .. })) if code == "skill_resolution_failed")
    );
    facade.tick().await.expect("no retry");
    assert_eq!(preparer.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn p2_large_input_is_referenced_and_engine_cannot_switch() {
    let directory = tempfile::tempdir().expect("directory");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("events.sqlite"), "test")
            .await
            .expect("open"),
    );
    let facade = AdmissionFacade::new(
        fabric.clone(),
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs"))),
        Arc::new(Preparer(AtomicUsize::new(0))),
    );
    let mut request = request();
    request.input = "private-input-".repeat(10_000);
    facade
        .publish(request.clone())
        .await
        .expect("large request via artifact");
    for (sequence, kind) in [RUN_REQUESTED, CONTEXT_REQUESTED].iter().enumerate() {
        let FabricReply::Event(Some(event)) = fabric
            .execute(FabricCommand::ReadEvent(event_id(request.run_id, kind)))
            .await
            .expect("read")
        else {
            panic!("event");
        };
        assert_eq!(event.envelope.operator_seq.0, sequence as u64 + 1);
        assert_eq!(event.envelope.run_id, request.run_id);
        assert_eq!(event.envelope.payload["request_hash"], request.request_hash);
        let encoded = serde_json::to_string(&event).expect("serialize");
        assert!(!encoded.contains("private-input-"));
        assert!(encoded.len() < 4096);
    }
    facade.tick().await.expect("prepare");
    let FabricReply::Event(Some(mut event)) = fabric
        .execute(FabricCommand::ReadEvent(event_id(
            request.run_id,
            CONTEXT_COMPLETED,
        )))
        .await
        .expect("read")
    else {
        panic!("event");
    };
    event.envelope.event_id = agent_core::event_runtime::EventId::new();
    event.envelope.engine_version = agent_core::operator::EngineVersion::OperatorV1;
    event.envelope.operator_seq.0 = 4;
    assert!(matches!(
        fabric.execute(FabricCommand::Publish(event)).await,
        Err(FabricError::Invalid(_))
    ));
}

#[tokio::test]
async fn p2_claimed_uncommitted_context_reopens_and_reclaims_expired_lease() {
    use agent_harness::operator_fabric::FabricHandler;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("events.sqlite");
    let blobs = Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let preparer = Arc::new(Preparer(AtomicUsize::new(0)));
    let request = request();
    let old_lease;
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("open"));
        let facade = AdmissionFacade::new(fabric.clone(), blobs.clone(), preparer.clone());
        facade.publish(request.clone()).await.expect("publish");
        fabric
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: CONTEXT_CONSUMER.into(),
                subscription: CONTEXT_CONSUMER.into(),
                topic: CONTEXT_TOPIC.into(),
                event_type: Some(CONTEXT_REQUESTED.into()),
                tenant: Some("local".into()),
            }))
            .await
            .expect("subscribe");
        let FabricReply::Claimed(mut claimed) = fabric
            .execute(FabricCommand::Claim {
                worker: "crashed".into(),
                now_ms: 0,
                lease_ms: 1,
                limit: 1,
                max_attempts: 10,
            })
            .await
            .expect("claim")
        else {
            panic!("claimed");
        };
        let delivery = claimed.remove(0);
        old_lease = delivery.lease.clone();
        facade
            .context_operator()
            .handle(delivery)
            .await
            .expect("uncommitted computation");
        assert!(
            facade
                .outcome(request.run_id)
                .await
                .expect("outcome")
                .is_none()
        );
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "test").await.expect("reopen"));
    let facade = AdmissionFacade::new(fabric.clone(), blobs, preparer.clone());
    facade.tick().await.expect("reclaim expired lease");
    assert!(
        facade
            .wait(request.run_id, Duration::from_secs(1))
            .await
            .is_ok()
    );
    assert_eq!(preparer.0.load(Ordering::SeqCst), 2);
    assert!(matches!(
        fabric
            .execute(FabricCommand::Commit {
                lease: old_lease,
                now_ms: agent_harness::unix_time_ms(),
                result: serde_json::json!({}),
                outbox: vec![]
            })
            .await,
        Err(FabricError::Fenced)
    ));
}
