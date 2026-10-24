use agent_core::{context::*, context_operator::*, harness::*, skill::SkillLock};
use agent_extension::{
    pipeline::SqlitePipelineStore,
    store::{FilesystemBlobStore, SqliteEventStore, SqliteRunStore},
};
use agent_harness::{
    context_operator::*,
    pipeline::{PipelineRuntime, PipelineStart},
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Prepared;
impl ContextPreparer for Prepared {
    fn prepare(&self, request: ContextRequest) -> PrepareFuture<'_> {
        Box::pin(async move {
            Ok(PreparedContext {
                pack: ContextPack {
                    messages: vec![],
                    memory_refs: vec![],
                    artifact_refs: vec![],
                    skill_refs: vec![],
                    effective_tools: vec![],
                    budget: ContextBudgetReport {
                        available_input_tokens: 1000,
                        estimated_input_tokens: 0,
                        estimator_confidence: 1.0,
                        retained_item_ids: vec![],
                        dropped: vec![],
                    },
                    component_descriptors: vec![],
                    fingerprint: request.request_hash.clone(),
                },
                skill_lock: SkillLock::default(),
                request_digest: request.request_hash,
            })
        })
    }
}

#[derive(Default)]
struct Capture(Mutex<Vec<ModelRequest>>);
impl ModelPort for Capture {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        self.0.lock().expect("capture").push(request);
        Box::pin(futures_util::stream::iter([ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        }]))
    }
}

async fn facade(path: &Path) -> AdmissionFacade {
    AdmissionFacade::new(
        Arc::new(
            SqliteEventStore::open(path.join("events.db"), "p6-process")
                .await
                .expect("events"),
        ),
        Arc::new(FilesystemBlobStore::new(path.join("blobs"))),
        Arc::new(Prepared),
    )
}

async fn pipeline(path: &Path, model: Arc<Capture>) -> PipelineRuntime {
    PipelineRuntime::new(
        Arc::new(SqlitePipelineStore::open(path.join("runs.db")).expect("ledger")),
        Arc::new(
            SqliteRunStore::open(path.join("runs.db"))
                .await
                .expect("runs"),
        ),
        Arc::new(FilesystemBlobStore::new(path.join("blobs"))),
        model,
    )
}

#[tokio::test]
async fn p6_crash_child() {
    let Ok(path) = std::env::var("MINA_P6_KILL_DIRECTORY") else {
        return;
    };
    let path = Path::new(&path);
    let request: ContextRequest =
        serde_json::from_slice(&std::fs::read(path.join("request.json")).expect("request"))
            .expect("decode");
    if std::env::var("MINA_P6_KILL_STAGE").expect("stage") == "context" {
        facade(path)
            .await
            .publish(request)
            .await
            .expect("publish pending context");
    } else {
        let mut start = PipelineStart::text(request.run_id, &request.input, "fake");
        start.start.attachments = request.attachments;
        pipeline(path, Arc::new(Capture::default()))
            .await
            .admit(start)
            .await
            .expect("pending model");
    }
    std::fs::write(path.join("ready"), b"durable").expect("ready");
    std::future::pending::<()>().await;
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn p6_real_process_kill_pending_context_and_model() {
    for stage in ["context", "model"] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path();
        let blobs = FilesystemBlobStore::new(path.join("blobs"));
        let mut attachments = vec![];
        for name in ["first.png", "second.pdf"] {
            let blob = blobs
                .put(PutBlob {
                    media_type: if name.ends_with("png") {
                        "image/png"
                    } else {
                        "application/pdf"
                    }
                    .into(),
                    name: Some(name.into()),
                    data: vec![1, 2, 3],
                    created_at_ms: 0,
                })
                .await
                .expect("blob");
            attachments.push(ModelAttachment {
                blob_id: blob.blob_id,
                media_type: blob.media_type,
                name: blob.name,
                size_bytes: Some(blob.size_bytes),
            });
        }
        let request = ContextRequest {
            engine_version: agent_core::operator::EngineVersion::OperatorV1,
            execution_profile: Some("operator_v1_text".into()),
            request_hash: "p6-process".into(),
            run_id: RunId::new(),
            session_id: None,
            through_message_ordinal: None,
            input: String::new(),
            attachments: attachments.clone(),
            explicit_skills: vec![],
            max_steps: 1,
            policy: ContextPolicy {
                default_skills: vec![],
                max_skills: 8,
                memory_enabled: false,
                memory_scope: "test".into(),
                max_memories: 8,
                agent_profile: "test".into(),
                agent_instruction: String::new(),
                model_profile: "fake".into(),
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
        };
        std::fs::write(
            path.join("request.json"),
            serde_json::to_vec(&request).expect("serialize"),
        )
        .expect("request file");
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", "p6_crash_child", "--nocapture"])
                .env("MINA_P6_KILL_DIRECTORY", path)
                .env("MINA_P6_KILL_STAGE", stage)
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("child"),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !path.join("ready").exists() {
            assert!(
                child.0.try_wait().expect("status").is_none(),
                "child exited early"
            );
            assert!(std::time::Instant::now() < deadline, "child readiness");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.0.kill().expect("kill pending worker process");
        assert!(!child.0.wait().expect("wait killed process").success());
        std::fs::remove_file(path.join("request.json")).expect("remove original ingress fixture");
        let model = Arc::new(Capture::default());
        let runtime = pipeline(path, model.clone()).await;
        if stage == "context" {
            let facade = facade(path).await;
            let persisted = facade
                .request(request.run_id)
                .await
                .expect("request")
                .expect("persisted");
            assert_eq!(persisted.attachments, attachments);
            facade.tick().await.expect("recover context");
            let prepared = facade
                .wait(request.run_id, Duration::from_secs(1))
                .await
                .expect("completed context");
            let plan = legacy_plan(persisted, prepared);
            let mut start = PipelineStart::text(plan.run_id, &plan.input, "fake");
            start.start.attachments = plan.attachments;
            runtime.admit(start).await.expect("start recovered plan");
        }
        runtime.drive(request.run_id).await.expect("recover model");
        let requests = model.0.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].messages.last().expect("user").attachments,
            attachments
        );
    }
}
