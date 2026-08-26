use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use agent_core::harness::{
    Agent, AgentMetadata, Harness, HarnessError, ModelMessage, RunCancellation, RunEvent,
    RunEventKind, RunExecution, RunId, RunSnapshot, RunStore, RunStoreError,
};
use futures_util::StreamExt;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::broadcast;

const EVENT_CHANNEL_CAPACITY: usize = 512;

#[derive(Debug, Clone)]
struct ActiveRun {
    cancellation: RunCancellation,
    events: broadcast::Sender<RunEvent>,
}

type ActiveRuns = Arc<Mutex<HashMap<RunId, ActiveRun>>>;
type FinishHook = Arc<dyn Fn(RunId) + Send + Sync>;

pub(crate) struct StartedRun {
    pub run_id: RunId,
    pub events: broadcast::Receiver<RunEvent>,
}

pub(crate) struct PlannedRun {
    pub run_id: RunId,
    pub input: String,
    pub prior_messages: Vec<ModelMessage>,
    pub allowed_tools: Option<Vec<String>>,
    pub execution_manifest: Option<Value>,
}

pub(crate) struct RunRuntime<A> {
    harness: Harness<A>,
    store: Arc<dyn RunStore>,
    active_runs: ActiveRuns,
    on_finished: FinishHook,
}

impl<A> Clone for RunRuntime<A> {
    fn clone(&self) -> Self {
        Self {
            harness: self.harness.clone(),
            store: Arc::clone(&self.store),
            active_runs: Arc::clone(&self.active_runs),
            on_finished: Arc::clone(&self.on_finished),
        }
    }
}

impl<A> RunRuntime<A>
where
    A: Agent,
{
    pub(crate) fn new(
        harness: Harness<A>,
        store: Arc<dyn RunStore>,
        on_finished: impl Fn(RunId) + Send + Sync + 'static,
    ) -> Self {
        Self {
            harness,
            store,
            active_runs: ActiveRuns::default(),
            on_finished: Arc::new(on_finished),
        }
    }

    #[must_use]
    pub(crate) fn metadata(&self) -> AgentMetadata {
        self.harness.metadata()
    }

    #[cfg(test)]
    pub(crate) async fn start(&self, input: String) -> Result<StartedRun, RunRuntimeError> {
        self.start_planned(PlannedRun {
            run_id: RunId::new(),
            input,
            prior_messages: Vec::new(),
            allowed_tools: None,
            execution_manifest: None,
        })
        .await
    }

    pub(crate) async fn start_planned(
        &self,
        plan: PlannedRun,
    ) -> Result<StartedRun, RunRuntimeError> {
        let execution = self.harness.start_with_context(
            plan.run_id,
            plan.input.clone(),
            plan.prior_messages,
            plan.allowed_tools,
        )?;
        let run_id = plan.run_id;
        let mut snapshot = RunSnapshot::new(run_id, plan.input, unix_time_ms());
        if let Some(manifest) = plan.execution_manifest {
            snapshot = snapshot.with_execution_manifest(manifest);
        }
        self.store.create_run(snapshot).await?;

        Ok(self.launch(execution))
    }

    pub(crate) async fn start_persisted(
        &self,
        plan: PlannedRun,
    ) -> Result<StartedRun, RunRuntimeError> {
        if self.store.get_run(plan.run_id).await?.is_none() {
            return Err(RunRuntimeError::Store(RunStoreError::NotFound(plan.run_id)));
        }
        if let Some(manifest) = plan.execution_manifest {
            self.store
                .set_execution_manifest(plan.run_id, manifest)
                .await?;
        }
        let execution = self.harness.start_with_context(
            plan.run_id,
            plan.input,
            plan.prior_messages,
            plan.allowed_tools,
        )?;
        Ok(self.launch(execution))
    }

    fn launch(&self, execution: RunExecution) -> StartedRun {
        let run_id = execution.run_id;

        let (sender, receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        self.active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                run_id,
                ActiveRun {
                    cancellation: execution.cancellation.clone(),
                    events: sender.clone(),
                },
            );

        let store = Arc::clone(&self.store);
        let active_runs = Arc::clone(&self.active_runs);
        let on_finished = Arc::clone(&self.on_finished);
        tokio::spawn(async move {
            let mut events = execution.events;
            while let Some(event) = events.next().await {
                let terminal = event.kind.is_terminal();
                match store.append_event(event.clone(), unix_time_ms()).await {
                    Ok(_) => {
                        // No active subscriber is a valid state: the durable
                        // log remains authoritative for later replay.
                        let _ = sender.send(event);
                    }
                    Err(error) => {
                        execution.cancellation.cancel();
                        tracing::error!(%run_id, %error, "failed to persist run event");
                        if let Some(failure) = close_after_store_error(&store, run_id).await {
                            let _ = sender.send(failure);
                        }
                        break;
                    }
                }
                if terminal {
                    break;
                }
            }

            active_runs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&run_id);
            on_finished(run_id);
        });

        StartedRun {
            run_id,
            events: receiver,
        }
    }

    pub(crate) fn subscribe(&self, run_id: RunId) -> Option<broadcast::Receiver<RunEvent>> {
        self.active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&run_id)
            .map(|run| run.events.subscribe())
    }

    pub(crate) fn cancel(&self, run_id: RunId) -> bool {
        let cancellation = self
            .active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&run_id)
            .map(|run| run.cancellation.clone());
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
            true
        } else {
            false
        }
    }

    pub(crate) async fn get_run(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunSnapshot>, RunStoreError> {
        self.store.get_run(run_id).await
    }

    pub(crate) async fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, RunStoreError> {
        self.store.events_after(run_id, after_seq, limit).await
    }

    pub(crate) async fn fail_persisted(
        &self,
        run_id: RunId,
        code: &str,
        message: &str,
    ) -> Result<RunSnapshot, RunRuntimeError> {
        let snapshot = self
            .store
            .append_event(
                RunEvent::new(run_id, 1, RunEventKind::RunStarted),
                unix_time_ms(),
            )
            .await?;
        let failed = self
            .store
            .append_event(
                RunEvent::new(
                    run_id,
                    snapshot.last_seq.saturating_add(1),
                    RunEventKind::RunFailed {
                        code: code.into(),
                        message: message.into(),
                        retryable: false,
                    },
                ),
                unix_time_ms(),
            )
            .await?;
        Ok(failed)
    }

    /// Closes runs left non-terminal by a previous process. A single-run
    /// future is not serializable, so restart recovery is explicit failure,
    /// never an unsafe attempt to continue an unknown execution point.
    pub(crate) async fn recover_interrupted(&self) -> Result<usize, RunStoreError> {
        let unfinished = self.store.unfinished_runs().await?;
        let count = unfinished.len();
        for snapshot in unfinished {
            self.store
                .append_event(
                    RunEvent::new(
                        snapshot.run_id,
                        snapshot.last_seq.saturating_add(1),
                        RunEventKind::RunFailed {
                            code: "run_interrupted".into(),
                            message:
                                "the server restarted before this run reached a terminal state"
                                    .into(),
                            retryable: true,
                        },
                    ),
                    unix_time_ms(),
                )
                .await?;
        }
        Ok(count)
    }
}

async fn close_after_store_error(store: &Arc<dyn RunStore>, run_id: RunId) -> Option<RunEvent> {
    let snapshot = match store.get_run(run_id).await {
        Ok(Some(snapshot)) if !snapshot.is_terminal() => snapshot,
        _ => return None,
    };
    let failure = RunEvent::new(
        run_id,
        snapshot.last_seq.saturating_add(1),
        RunEventKind::RunFailed {
            code: "run_store_write_failed".into(),
            message: "the run stopped because its durable event could not be written".into(),
            retryable: true,
        },
    );
    match store.append_event(failure.clone(), unix_time_ms()).await {
        Ok(_) => Some(failure),
        Err(error) => {
            tracing::error!(%run_id, %error, "failed to persist run store failure terminal event");
            None
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum RunRuntimeError {
    #[error(transparent)]
    Harness(#[from] HarnessError),
    #[error(transparent)]
    Store(#[from] RunStoreError),
}

pub(crate) fn unix_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{AgentEvent, AgentEventStream, FinishReason, RunRequest, RunStatus};
    use agent_extension::store::InMemoryRunStore;
    use futures_util::stream;

    use super::*;

    #[derive(Debug)]
    struct Echo;

    impl Agent for Echo {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("echo", "test")
        }

        fn run(&self, request: RunRequest) -> AgentEventStream {
            Box::pin(stream::iter(vec![
                AgentEvent::text_delta(request.input),
                AgentEvent::completed(FinishReason::Stop),
            ]))
        }
    }

    #[derive(Debug)]
    struct Waiting;

    impl Agent for Waiting {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("waiting", "test")
        }

        fn run(&self, _request: RunRequest) -> AgentEventStream {
            Box::pin(stream::pending())
        }
    }

    #[tokio::test]
    async fn execution_is_owned_by_runtime_and_persisted_without_a_subscriber() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::default());
        let runtime = RunRuntime::new(Harness::new(Echo), Arc::clone(&store), |_| {});
        let started = runtime
            .start("hello".into())
            .await
            .expect("run should start");
        let run_id = started.run_id;
        drop(started.events);

        for _ in 0..20 {
            let snapshot = store
                .get_run(run_id)
                .await
                .expect("snapshot should load")
                .expect("snapshot should exist");
            if snapshot.is_terminal() {
                assert_eq!(snapshot.status, RunStatus::Completed);
                assert_eq!(snapshot.output, "hello");
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("run did not complete");
    }

    #[tokio::test]
    async fn startup_recovery_marks_unfinished_runs_as_interrupted() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::default());
        let run_id = RunId::new();
        store
            .create_run(RunSnapshot::new(run_id, "hello", 1))
            .await
            .expect("run should be created");
        let runtime = RunRuntime::new(Harness::new(Echo), Arc::clone(&store), |_| {});

        assert_eq!(
            runtime
                .recover_interrupted()
                .await
                .expect("recovery should work"),
            1
        );
        let snapshot = store
            .get_run(run_id)
            .await
            .expect("snapshot should load")
            .expect("snapshot should exist");

        assert_eq!(snapshot.status, RunStatus::Failed);
        assert_eq!(
            snapshot.failure.map(|failure| failure.code),
            Some("run_interrupted".into())
        );
    }

    #[tokio::test]
    async fn cancellation_reaches_a_durable_terminal_state() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::default());
        let runtime = RunRuntime::new(Harness::new(Waiting), Arc::clone(&store), |_| {});
        let mut started = runtime
            .start("wait".into())
            .await
            .expect("run should start");

        assert!(runtime.cancel(started.run_id));
        while let Ok(event) = started.events.recv().await {
            if event.kind.is_terminal() {
                break;
            }
        }
        let snapshot = store
            .get_run(started.run_id)
            .await
            .expect("snapshot should load")
            .expect("snapshot should exist");

        assert_eq!(snapshot.status, RunStatus::Cancelled);
    }
}
