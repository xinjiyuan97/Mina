use std::sync::Arc;

use agent_core::context::{
    ContextArtifactStore, ContextBudget, ContextBuildRequest, ContextCompressor, ContextEngine,
    HeuristicTokenEstimator, InMemoryContextArtifactStore, SlidingWindowCompressor, TokenEstimator,
};
use agent_core::harness::{
    BeginSessionRun, ContentPart, CreateSession, FinalizeSessionRun, FinishReason, RunEvent,
    RunEventKind, RunSnapshot, RunStore, SessionId, SessionStore,
};
use agent_core::memory::{
    MemoryId, MemoryKind, MemoryRecord, MemoryRetriever, MemoryScope, MemorySourceRef, MemoryStore,
    PutMemory,
};
use agent_extension::store::{InMemoryMemoryStore, InMemoryRunStore};

#[tokio::test]
async fn builds_deterministic_context_from_session_memory_and_contracts() {
    let state_store = Arc::new(InMemoryRunStore::default());
    let sessions: Arc<dyn SessionStore> = state_store.clone();
    let run_store: Arc<dyn RunStore> = state_store;
    let session_id = SessionId::new();
    let session = sessions
        .create_session(CreateSession {
            session_id,
            agent_profile: "test".into(),
            title: None,
            created_at_ms: 1,
        })
        .await
        .expect("session should be created");
    let first_run = agent_core::harness::RunId::new();
    sessions
        .begin_run(
            BeginSessionRun {
                session_id,
                run_id: first_run,
                expected_revision: session.revision,
                idempotency_key: "first".into(),
                request_hash: "first-hash".into(),
                input: vec![ContentPart::text("first question")],
                created_at_ms: 2,
            },
            RunSnapshot::new(first_run, "first question", 2),
        )
        .await
        .expect("run should begin");
    run_store
        .append_event(RunEvent::new(first_run, 1, RunEventKind::RunStarted), 3)
        .await
        .expect("run should start");
    run_store
        .append_event(
            RunEvent::new(
                first_run,
                2,
                RunEventKind::OutputDelta {
                    channel: agent_core::harness::OutputChannel::AssistantText,
                    delta: "first answer".into(),
                },
            ),
            4,
        )
        .await
        .expect("output should append");
    let terminal = run_store
        .append_event(
            RunEvent::new(
                first_run,
                3,
                RunEventKind::RunCompleted {
                    finish_reason: FinishReason::Stop,
                },
            ),
            5,
        )
        .await
        .expect("run should complete");
    let finalized = sessions
        .finalize_run(FinalizeSessionRun {
            session_id,
            run: terminal,
            finalized_at_ms: 6,
        })
        .await
        .expect("session should finalize");

    let memory_adapter = Arc::new(InMemoryMemoryStore::default());
    memory_adapter
        .put(PutMemory {
            record: MemoryRecord {
                memory_id: MemoryId::new(),
                scope: MemoryScope("global".into()),
                kind: MemoryKind::Semantic,
                content: vec![ContentPart::text("prefers concise answers")],
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
        .expect("memory should persist");
    let memories: Arc<dyn MemoryRetriever> = memory_adapter;
    let estimator: Arc<dyn TokenEstimator> = Arc::new(HeuristicTokenEstimator);
    let compressor: Arc<dyn ContextCompressor> = Arc::new(SlidingWindowCompressor::new(estimator));
    let artifacts: Arc<dyn ContextArtifactStore> =
        Arc::new(InMemoryContextArtifactStore::default());
    let engine = ContextEngine::new(sessions, memories, compressor, artifacts);
    let request = ContextBuildRequest {
        run_id: agent_core::harness::RunId::new(),
        session_id: Some(session_id),
        through_message_ordinal: Some(finalized.next_message_ordinal - 1),
        current_input: "second concise question".into(),
        request_digest: "request".into(),
        agent_profile: "test".into(),
        agent_instruction: "be helpful".into(),
        model_profile: "test-model".into(),
        resolved_skills: Vec::new(),
        memory_scopes: vec![MemoryScope("global".into())],
        available_tools: vec!["read_text_file".into()],
        max_memories: 5,
        budget: ContextBudget {
            model_context_tokens: 4_096,
            reserved_output_tokens: 512,
            reserved_tool_schema_tokens: 128,
            max_skill_tokens: 512,
            max_memory_tokens: 512,
            max_history_tokens: 2_048,
        },
        policy_version: 1,
        now_ms: 10,
    };
    let first = engine
        .build(request.clone())
        .await
        .expect("context should build");
    let replay = engine.build(request).await.expect("replay should build");

    let rendered = first
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("first question"));
    assert!(rendered.contains("first answer"));
    assert!(rendered.contains("prefers concise answers"));
    assert!(!rendered.contains("second concise question"));
    assert_eq!(first.effective_tools, vec!["read_text_file"]);
    assert_eq!(first.fingerprint, replay.fingerprint);
}
