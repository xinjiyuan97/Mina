use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_core::harness::{
    Agent, AgentMachine, AgentMetadata, ClaimedActivation, CompleteFlowRun, ContinueFlowRun,
    FlowError, FlowRunState, FlowRunStatus, FlowStore, MachineOutput, MachineResumeRequest,
    MachineStartRequest, ModelAttachment, ModelMessage, ObservedRunEvent, RenewFlowLease,
    RunCancellation, RunEvent, RunEventKind, RunId, RunSnapshot, RunStore, RunStoreError,
    StepOutcome, SuspendFlowRun,
};
use futures_util::StreamExt;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch};

use crate::{Harness, HarnessError, MAX_RUN_STEPS, RunExecution, RunOptions};

const EVENT_CHANNEL_CAPACITY: usize = 512;
const PERSISTENCE_CHANNEL_CAPACITY: usize = 1_024;
const PERSISTENCE_BATCH_MAX_EVENTS: usize = 128;
const PERSISTENCE_BATCH_WINDOW: Duration = Duration::from_millis(40);
const PERSISTENCE_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const FLOW_WORKER_INTERVAL: Duration = Duration::from_millis(100);
const FLOW_ACTIVATION_LEASE: Duration = Duration::from_secs(120);
const FLOW_ACTIVATION_HEARTBEAT: Duration = Duration::from_secs(30);
const FLOW_CLAIM_LIMIT: usize = 16;

#[derive(Debug, Clone)]
struct ActiveRun {
    cancellation: RunCancellation,
    events: broadcast::Sender<RunEvent>,
}

type ActiveRuns = Arc<Mutex<HashMap<RunId, ActiveRun>>>;
type FinishHook = Arc<dyn Fn(RunId) + Send + Sync>;

pub struct StartedRun {
    pub run_id: RunId,
    pub events: broadcast::Receiver<RunEvent>,
}

pub struct PlannedRun {
    pub run_id: RunId,
    pub input: String,
    pub attachments: Vec<ModelAttachment>,
    pub prior_messages: Vec<ModelMessage>,
    pub allowed_tools: Option<Vec<String>>,
    pub allow_run_adf: bool,
    pub max_steps: Option<u32>,
    pub context_fingerprint: Option<String>,
    pub execution_manifest: Option<Value>,
}

struct DurableRuntime {
    machine: Arc<dyn AgentMachine>,
    flows: Arc<dyn FlowStore>,
    activation_lease: Duration,
    activation_heartbeat: Duration,
}

pub struct RunRuntime<A> {
    harness: Harness<A>,
    store: Arc<dyn RunStore>,
    active_runs: ActiveRuns,
    on_finished: FinishHook,
    durable: Option<Arc<DurableRuntime>>,
}

impl<A> Clone for RunRuntime<A> {
    fn clone(&self) -> Self {
        Self {
            harness: self.harness.clone(),
            store: Arc::clone(&self.store),
            active_runs: Arc::clone(&self.active_runs),
            on_finished: Arc::clone(&self.on_finished),
            durable: self.durable.as_ref().map(Arc::clone),
        }
    }
}

impl<A> RunRuntime<A>
where
    A: Agent,
{
    pub fn new(
        harness: Harness<A>,
        store: Arc<dyn RunStore>,
        on_finished: impl Fn(RunId) + Send + Sync + 'static,
    ) -> Self {
        Self {
            harness,
            store,
            active_runs: ActiveRuns::default(),
            on_finished: Arc::new(on_finished),
            durable: None,
        }
    }

    pub fn new_durable(
        harness: Harness<A>,
        store: Arc<dyn RunStore>,
        flows: Arc<dyn FlowStore>,
        on_finished: impl Fn(RunId) + Send + Sync + 'static,
    ) -> Self
    where
        A: AgentMachine,
    {
        let machine: Arc<dyn AgentMachine> = harness.agent_handle();
        Self {
            harness,
            store,
            active_runs: ActiveRuns::default(),
            on_finished: Arc::new(on_finished),
            durable: Some(Arc::new(DurableRuntime {
                machine,
                flows,
                activation_lease: FLOW_ACTIVATION_LEASE,
                activation_heartbeat: FLOW_ACTIVATION_HEARTBEAT,
            })),
        }
    }

    #[cfg(test)]
    fn with_flow_activation_timing(mut self, lease: Duration, heartbeat: Duration) -> Self {
        assert!(!lease.is_zero());
        assert!(!heartbeat.is_zero() && heartbeat < lease);
        let durable = Arc::get_mut(
            self.durable
                .as_mut()
                .expect("flow timing requires a durable runtime"),
        )
        .expect("flow timing must be configured before cloning the runtime");
        durable.activation_lease = lease;
        durable.activation_heartbeat = heartbeat;
        self
    }

    #[must_use]
    pub fn metadata(&self) -> AgentMetadata {
        self.harness.metadata()
    }

    pub async fn start(&self, input: String) -> Result<StartedRun, RunRuntimeError> {
        self.start_planned(PlannedRun {
            run_id: RunId::new(),
            input,
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            allowed_tools: None,
            allow_run_adf: false,
            max_steps: None,
            context_fingerprint: None,
            execution_manifest: None,
        })
        .await
    }

    pub async fn start_planned(&self, plan: PlannedRun) -> Result<StartedRun, RunRuntimeError> {
        if self.durable.is_some() {
            return self.start_machine(plan, true).await;
        }
        let execution = self.harness.start_with_options_and_attachments(
            plan.run_id,
            plan.input.clone(),
            plan.attachments,
            plan.prior_messages,
            RunOptions {
                allowed_tools: plan.allowed_tools,
                allow_run_adf: plan.allow_run_adf,
                max_steps: plan.max_steps,
            },
        )?;
        let run_id = plan.run_id;
        let mut snapshot = RunSnapshot::new(run_id, plan.input, unix_time_ms());
        if let Some(manifest) = plan.execution_manifest {
            snapshot = snapshot.with_execution_manifest(manifest);
        }
        self.store.create_run(snapshot).await?;

        Ok(self.launch(execution))
    }

    pub async fn start_persisted(&self, plan: PlannedRun) -> Result<StartedRun, RunRuntimeError> {
        if self.durable.is_some() {
            return self.start_machine(plan, false).await;
        }
        if self.store.get_run(plan.run_id).await?.is_none() {
            return Err(RunRuntimeError::Store(RunStoreError::NotFound(plan.run_id)));
        }
        if let Some(manifest) = plan.execution_manifest {
            self.store
                .set_execution_manifest(plan.run_id, manifest)
                .await?;
        }
        let execution = self.harness.start_with_options_and_attachments(
            plan.run_id,
            plan.input,
            plan.attachments,
            plan.prior_messages,
            RunOptions {
                allowed_tools: plan.allowed_tools,
                allow_run_adf: plan.allow_run_adf,
                max_steps: plan.max_steps,
            },
        )?;
        Ok(self.launch(execution))
    }

    async fn start_machine(
        &self,
        plan: PlannedRun,
        create_snapshot: bool,
    ) -> Result<StartedRun, RunRuntimeError> {
        let durable = self
            .durable
            .as_ref()
            .expect("durable start requires a durable runtime");
        validate_machine_plan(&plan)?;
        let now_ms = unix_time_ms();
        let cancellation = RunCancellation::new();
        let start_request = MachineStartRequest {
            run_id: plan.run_id,
            input: plan.input.clone(),
            attachments: plan.attachments,
            prior_messages: plan.prior_messages,
            allowed_tools: plan.allowed_tools,
            allow_run_adf: plan.allow_run_adf,
            max_steps: plan.max_steps,
            context_fingerprint: plan.context_fingerprint,
            activated_at_ms: now_ms,
            cancellation: cancellation.clone(),
        };
        let checkpoint = durable
            .machine
            .initial_checkpoint(&start_request)
            .map_err(RunRuntimeError::machine)?;
        if create_snapshot {
            let mut snapshot = RunSnapshot::new(plan.run_id, plan.input, now_ms);
            if let Some(manifest) = plan.execution_manifest {
                snapshot = snapshot.with_execution_manifest(manifest);
            }
            self.store.create_run(snapshot).await?;
        } else {
            if self.store.get_run(plan.run_id).await?.is_none() {
                return Err(RunRuntimeError::Store(RunStoreError::NotFound(plan.run_id)));
            }
            if let Some(manifest) = plan.execution_manifest {
                self.store
                    .set_execution_manifest(plan.run_id, manifest)
                    .await?;
            }
        }

        let started = self.register_machine_run(plan.run_id, cancellation);
        if let Err(error) = durable
            .flows
            .create(FlowRunState {
                run_id: plan.run_id,
                revision: 0,
                status: FlowRunStatus::Runnable,
                activation_id: None,
                checkpoint,
                wait_subscription_ids: Vec::new(),
                lease_owner: None,
                lease_until_ms: None,
                updated_at_ms: now_ms,
            })
            .await
        {
            self.active_runs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&plan.run_id);
            return Err(error.into());
        }
        Ok(started)
    }

    fn register_machine_run(&self, run_id: RunId, cancellation: RunCancellation) -> StartedRun {
        let mut active = self
            .active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = active.get(&run_id) {
            return StartedRun {
                run_id,
                events: existing.events.subscribe(),
            };
        }
        let (sender, receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        active.insert(
            run_id,
            ActiveRun {
                cancellation,
                events: sender,
            },
        );
        StartedRun {
            run_id,
            events: receiver,
        }
    }

    pub fn start_flow_worker(&self) {
        if self.durable.is_none() {
            return;
        }
        let runtime = self.clone();
        tokio::spawn(async move {
            let worker_id = format!("server-flow-worker-{}", std::process::id());
            let mut interval = tokio::time::interval(FLOW_WORKER_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = runtime.drive_flows_once(&worker_id).await {
                    tracing::warn!(%error, "flow activation tick failed");
                }
            }
        });
    }

    async fn drive_flows_once(&self, worker_id: &str) -> Result<(), RunRuntimeError> {
        let Some(durable) = self.durable.as_ref() else {
            return Ok(());
        };
        let now_ms = unix_time_ms();
        let claimed = durable
            .flows
            .claim_runnable(
                worker_id.to_owned(),
                now_ms,
                deadline_after(now_ms, durable.activation_lease),
                FLOW_CLAIM_LIMIT,
            )
            .await?;
        for activation in claimed {
            let runtime = self.clone();
            tokio::spawn(async move {
                let run_id = activation.state.run_id;
                if let Err(error) = runtime.run_machine_activation(activation).await {
                    tracing::error!(%run_id, %error, "flow activation failed");
                }
            });
        }
        Ok(())
    }

    async fn run_machine_activation(
        &self,
        activation: ClaimedActivation,
    ) -> Result<(), RunRuntimeError> {
        let durable = self
            .durable
            .as_ref()
            .expect("machine activation requires durable runtime");
        let run_id = activation.state.run_id;
        let activation_id = activation
            .state
            .activation_id
            .ok_or_else(|| RunRuntimeError::machine_message("claimed flow has no activation id"))?;
        let worker_id =
            activation.state.lease_owner.clone().ok_or_else(|| {
                RunRuntimeError::machine_message("claimed flow has no lease owner")
            })?;
        let expected_revision = activation.state.revision;
        let (cancellation, sender) = self.machine_activation_context(run_id);
        let snapshot = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(RunStoreError::NotFound(run_id))?;
        if snapshot.is_terminal() {
            let _ = durable.flows.cancel(run_id, unix_time_ms()).await;
            self.finish_machine_run(run_id);
            return Ok(());
        }
        let lifecycle = if snapshot.last_seq == 0 {
            RunEventKind::RunStarted
        } else {
            RunEventKind::RunResumed {
                activation_id,
                checkpoint_revision: activation.state.revision,
            }
        };
        let lifecycle_event = RunEvent::new(run_id, snapshot.last_seq.saturating_add(1), lifecycle);
        let mut snapshot = self
            .store
            .append_event(lifecycle_event.clone(), unix_time_ms())
            .await?;
        let _ = sender.send(lifecycle_event);

        let mut outputs = durable.machine.resume(MachineResumeRequest {
            run_id,
            activation_id,
            checkpoint: activation.state.checkpoint.clone(),
            inbox: activation.inbox,
            activated_at_ms: unix_time_ms(),
            cancellation: cancellation.clone(),
        });
        let (persistence_tx, persistence_rx) = mpsc::channel(PERSISTENCE_CHANNEL_CAPACITY);
        let (failure_tx, _failure_rx) = watch::channel(None::<String>);
        let persistence = tokio::spawn(persist_run_events(
            Arc::clone(&self.store),
            persistence_rx,
            failure_tx,
        ));
        let mut outcome = None;
        let mut lease_error = None;
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + durable.activation_heartbeat,
            durable.activation_heartbeat,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let output = tokio::select! {
                biased;
                _ = heartbeat.tick() => {
                    let renewed_at_ms = unix_time_ms();
                    match durable.flows.renew_lease(RenewFlowLease {
                        run_id,
                        activation_id,
                        expected_revision,
                        worker_id: worker_id.clone(),
                        renewed_at_ms,
                        lease_until_ms: deadline_after(
                            renewed_at_ms,
                            durable.activation_lease,
                        ),
                    }).await {
                        Ok(_) => continue,
                        Err(error) => {
                            lease_error = Some(error);
                            None
                        }
                    }
                },
                output = outputs.next() => output,
            };
            let Some(output) = output else {
                break;
            };
            match output {
                MachineOutput::Event(agent_event) => {
                    if outcome.is_some() {
                        outcome = Some(StepOutcome::Failed {
                            error: agent_core::harness::MachineError {
                                code: "machine_protocol_violation".into(),
                                message: "agent machine emitted an event after yielding".into(),
                                retryable: false,
                            },
                        });
                        break;
                    }
                    let kind = agent_event.into_run_event_kind();
                    if kind.is_terminal() {
                        outcome = Some(StepOutcome::Failed {
                            error: agent_core::harness::MachineError {
                                code: "machine_protocol_violation".into(),
                                message:
                                    "agent machine emitted a terminal event instead of yielding"
                                        .into(),
                                retryable: false,
                            },
                        });
                        break;
                    }
                    let event = RunEvent::new(run_id, snapshot.last_seq.saturating_add(1), kind);
                    snapshot.last_seq = event.seq;
                    let _ = sender.send(event.clone());
                    if persistence_tx
                        .send(ObservedRunEvent::new(event, unix_time_ms()))
                        .await
                        .is_err()
                    {
                        outcome = Some(StepOutcome::Failed {
                            error: agent_core::harness::MachineError {
                                code: "run_store_write_failed".into(),
                                message: "the run persistence worker stopped unexpectedly".into(),
                                retryable: true,
                            },
                        });
                        break;
                    }
                }
                MachineOutput::Yield(next) => {
                    if outcome.replace(next).is_some() {
                        outcome = Some(StepOutcome::Failed {
                            error: agent_core::harness::MachineError {
                                code: "machine_protocol_violation".into(),
                                message: "agent machine yielded more than once".into(),
                                retryable: false,
                            },
                        });
                    }
                    break;
                }
                _ => {
                    outcome = Some(StepOutcome::Failed {
                        error: agent_core::harness::MachineError {
                            code: "machine_protocol_violation".into(),
                            message: "agent machine emitted an unsupported output".into(),
                            retryable: false,
                        },
                    });
                    break;
                }
            }
        }
        drop(outputs);
        drop(persistence_tx);
        match persistence.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                cancellation.cancel();
                return Err(RunRuntimeError::Persistence(error));
            }
            Err(error) => {
                cancellation.cancel();
                return Err(RunRuntimeError::Persistence(format!(
                    "run persistence worker failed: {error}"
                )));
            }
        }
        if let Some(error) = lease_error {
            return Err(RunRuntimeError::Flow(error));
        }
        let outcome = outcome.unwrap_or_else(|| StepOutcome::Failed {
            error: agent_core::harness::MachineError {
                code: "machine_protocol_violation".into(),
                message: "agent machine stream ended without yielding".into(),
                retryable: false,
            },
        });
        let snapshot = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(RunStoreError::NotFound(run_id))?;
        let next_seq = snapshot.last_seq.saturating_add(1);
        let now_ms = unix_time_ms();
        match outcome {
            StepOutcome::Continue {
                checkpoint,
                effects,
            } => {
                durable
                    .flows
                    .continue_run(ContinueFlowRun {
                        run_id,
                        activation_id,
                        expected_revision: activation.state.revision,
                        checkpoint,
                        effects,
                        continued_at_ms: now_ms,
                    })
                    .await?;
            }
            StepOutcome::Suspend {
                checkpoint,
                waits,
                effects,
            } => {
                let waiting = durable
                    .flows
                    .suspend(SuspendFlowRun {
                        run_id,
                        activation_id,
                        expected_revision: activation.state.revision,
                        checkpoint,
                        waits,
                        effects,
                        suspended_at_ms: now_ms,
                    })
                    .await?;
                let event = RunEvent::new(
                    run_id,
                    next_seq,
                    RunEventKind::RunWaiting {
                        checkpoint_revision: waiting.revision,
                        wait_count: u32::try_from(waiting.wait_subscription_ids.len())
                            .unwrap_or(u32::MAX),
                    },
                );
                self.store.append_event(event.clone(), now_ms).await?;
                let _ = sender.send(event);
            }
            StepOutcome::Complete { finish_reason } => {
                durable
                    .flows
                    .complete(CompleteFlowRun {
                        run_id,
                        activation_id,
                        expected_revision: activation.state.revision,
                        status: FlowRunStatus::Completed,
                        completed_at_ms: now_ms,
                    })
                    .await?;
                let event = RunEvent::new(
                    run_id,
                    next_seq,
                    RunEventKind::RunCompleted { finish_reason },
                );
                self.store.append_event(event.clone(), now_ms).await?;
                let _ = sender.send(event);
                self.finish_machine_run(run_id);
            }
            StepOutcome::Failed { error } => {
                durable
                    .flows
                    .complete(CompleteFlowRun {
                        run_id,
                        activation_id,
                        expected_revision: activation.state.revision,
                        status: FlowRunStatus::Failed,
                        completed_at_ms: now_ms,
                    })
                    .await?;
                let event = RunEvent::new(
                    run_id,
                    next_seq,
                    RunEventKind::RunFailed {
                        code: error.code,
                        message: error.message,
                        retryable: error.retryable,
                    },
                );
                self.store.append_event(event.clone(), now_ms).await?;
                let _ = sender.send(event);
                self.finish_machine_run(run_id);
            }
            StepOutcome::Cancelled => {
                durable
                    .flows
                    .complete(CompleteFlowRun {
                        run_id,
                        activation_id,
                        expected_revision: activation.state.revision,
                        status: FlowRunStatus::Cancelled,
                        completed_at_ms: now_ms,
                    })
                    .await?;
                let event = RunEvent::new(run_id, next_seq, RunEventKind::RunCancelled);
                self.store.append_event(event.clone(), now_ms).await?;
                let _ = sender.send(event);
                self.finish_machine_run(run_id);
            }
        }
        Ok(())
    }

    fn machine_activation_context(
        &self,
        run_id: RunId,
    ) -> (RunCancellation, broadcast::Sender<RunEvent>) {
        let mut active = self
            .active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = active.entry(run_id).or_insert_with(|| {
            let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
            ActiveRun {
                cancellation: RunCancellation::new(),
                events,
            }
        });
        (entry.cancellation.clone(), entry.events.clone())
    }

    fn finish_machine_run(&self, run_id: RunId) {
        self.active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&run_id);
        (self.on_finished)(run_id);
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
            let (persistence_tx, persistence_rx) = mpsc::channel(PERSISTENCE_CHANNEL_CAPACITY);
            let (failure_tx, mut failure_rx) = watch::channel(None::<String>);
            let persistence = tokio::spawn(persist_run_events(
                Arc::clone(&store),
                persistence_rx,
                failure_tx,
            ));
            let mut pending_terminal = None;
            let mut last_live_seq = 0_u64;
            let mut persistence_error = None;

            loop {
                let next = tokio::select! {
                    changed = failure_rx.changed() => {
                        if changed.is_ok() {
                            persistence_error = failure_rx.borrow().clone();
                        } else {
                            persistence_error = Some("run persistence worker stopped unexpectedly".into());
                        }
                        None
                    }
                    event = events.next() => event,
                };
                let Some(event) = next else {
                    break;
                };
                let terminal = event.kind.is_terminal();

                // Delta delivery is intentionally independent from durable
                // writes. A terminal event is held until the writer drains so
                // clients never observe durable completion before it commits.
                if terminal {
                    pending_terminal = Some(event.clone());
                } else {
                    last_live_seq = event.seq;
                    let _ = sender.send(event.clone());
                }

                let observed = ObservedRunEvent::new(event, unix_time_ms());
                let queued = tokio::select! {
                    result = persistence_tx.send(observed) => result.is_ok(),
                    changed = failure_rx.changed() => {
                        if changed.is_ok() {
                            persistence_error = failure_rx.borrow().clone();
                        } else {
                            persistence_error = Some("run persistence worker stopped unexpectedly".into());
                        }
                        false
                    }
                };
                if !queued || terminal {
                    break;
                }
            }
            drop(persistence_tx);

            if persistence_error.is_none() {
                persistence_error = match persistence.await {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(error) => Some(format!("run persistence worker failed: {error}")),
                };
            } else {
                let _ = persistence.await;
            }

            if let Some(error) = persistence_error {
                execution.cancellation.cancel();
                tracing::error!(%run_id, %error, "failed to persist run event batch");
                let durable_failure = tokio::time::timeout(
                    PERSISTENCE_WRITE_TIMEOUT,
                    close_after_store_error(&store, run_id),
                )
                .await
                .ok()
                .flatten();
                let failure = durable_failure.filter(|event| event.seq > last_live_seq).unwrap_or_else(|| {
                    RunEvent::new(
                        run_id,
                        last_live_seq.saturating_add(1),
                        RunEventKind::RunFailed {
                            code: "run_store_write_failed".into(),
                            message: "the run stopped because its durable events could not be written".into(),
                            retryable: true,
                        },
                    )
                });
                let _ = sender.send(failure);
            } else if let Some(terminal) = pending_terminal {
                let _ = sender.send(terminal);
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

    pub fn subscribe(&self, run_id: RunId) -> Option<broadcast::Receiver<RunEvent>> {
        self.active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&run_id)
            .map(|run| run.events.subscribe())
    }

    pub fn cancel(&self, run_id: RunId) -> bool {
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

    pub async fn cancel_durable(&self, run_id: RunId) -> Result<bool, RunRuntimeError> {
        let Some(durable) = &self.durable else {
            return Ok(false);
        };
        let Some(flow) = durable.flows.get(run_id).await? else {
            return Ok(false);
        };
        if flow.status.is_terminal() {
            return Ok(false);
        }
        let (cancellation, sender) = self.machine_activation_context(run_id);
        cancellation.cancel();
        let cancelled = durable.flows.cancel(run_id, unix_time_ms()).await?;
        if cancelled.status != FlowRunStatus::Cancelled {
            return Ok(false);
        }
        let snapshot = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(RunStoreError::NotFound(run_id))?;
        if !snapshot.is_terminal() {
            let mut last_seq = snapshot.last_seq;
            if last_seq == 0 {
                let started = RunEvent::new(run_id, 1, RunEventKind::RunStarted);
                let projected = self
                    .store
                    .append_event(started.clone(), unix_time_ms())
                    .await?;
                last_seq = projected.last_seq;
                let _ = sender.send(started);
            }
            let event = RunEvent::new(
                run_id,
                last_seq.saturating_add(1),
                RunEventKind::RunCancelled,
            );
            self.store
                .append_event(event.clone(), unix_time_ms())
                .await?;
            let _ = sender.send(event);
        }
        self.finish_machine_run(run_id);
        Ok(true)
    }

    pub async fn get_run(&self, run_id: RunId) -> Result<Option<RunSnapshot>, RunStoreError> {
        self.store.get_run(run_id).await
    }

    pub async fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, RunStoreError> {
        self.store.events_after(run_id, after_seq, limit).await
    }

    pub async fn fail_persisted(
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
    pub async fn recover_interrupted(&self) -> Result<usize, RunStoreError> {
        let unfinished = self.store.unfinished_runs().await?;
        let mut count = 0;
        for snapshot in unfinished {
            if let Some(durable) = &self.durable {
                let flow = durable.flows.get(snapshot.run_id).await.map_err(|error| {
                    RunStoreError::Backend(format!("read durable flow recovery state: {error}"))
                })?;
                if let Some(flow) = flow {
                    if !flow.status.is_terminal() {
                        continue;
                    }
                    let mut last_seq = snapshot.last_seq;
                    if last_seq == 0 {
                        let started = self
                            .store
                            .append_event(
                                RunEvent::new(snapshot.run_id, 1, RunEventKind::RunStarted),
                                unix_time_ms(),
                            )
                            .await?;
                        last_seq = started.last_seq;
                    }
                    let terminal = match flow.status {
                        FlowRunStatus::Completed => RunEventKind::RunCompleted {
                            finish_reason: agent_core::harness::FinishReason::Stop,
                        },
                        FlowRunStatus::Failed => RunEventKind::RunFailed {
                            code: "flow_recovery_failed".into(),
                            message: "the durable flow failed before its public terminal event was committed".into(),
                            retryable: true,
                        },
                        FlowRunStatus::Cancelled => RunEventKind::RunCancelled,
                        FlowRunStatus::Runnable
                        | FlowRunStatus::Running
                        | FlowRunStatus::WaitingEvent => unreachable!("non-terminal flows returned above"),
                    };
                    self.store
                        .append_event(
                            RunEvent::new(snapshot.run_id, last_seq.saturating_add(1), terminal),
                            unix_time_ms(),
                        )
                        .await?;
                    count += 1;
                    continue;
                }
            }
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
            count += 1;
        }
        Ok(count)
    }
}

async fn persist_run_events(
    store: Arc<dyn RunStore>,
    mut receiver: mpsc::Receiver<ObservedRunEvent>,
    failure: watch::Sender<Option<String>>,
) -> Result<(), String> {
    while let Some(first) = receiver.recv().await {
        let flush_immediately = is_persistence_boundary(&first.event.kind);
        let mut batch = Vec::with_capacity(PERSISTENCE_BATCH_MAX_EVENTS);
        batch.push(first);

        if !flush_immediately {
            let window = tokio::time::sleep(PERSISTENCE_BATCH_WINDOW);
            tokio::pin!(window);
            loop {
                tokio::select! {
                    () = &mut window => break,
                    next = receiver.recv() => match next {
                        Some(next) => {
                            let boundary = is_persistence_boundary(&next.event.kind);
                            batch.push(next);
                            if boundary || batch.len() >= PERSISTENCE_BATCH_MAX_EVENTS {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        let result =
            tokio::time::timeout(PERSISTENCE_WRITE_TIMEOUT, store.append_events(batch)).await;
        let error = match result {
            Ok(Ok(_)) => continue,
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!(
                "event batch write exceeded {} seconds",
                PERSISTENCE_WRITE_TIMEOUT.as_secs()
            ),
        };
        let _ = failure.send(Some(error.clone()));
        return Err(error);
    }
    Ok(())
}

const fn is_persistence_boundary(kind: &RunEventKind) -> bool {
    !matches!(
        kind,
        RunEventKind::OutputDelta { .. } | RunEventKind::ToolCallArgumentsDelta { .. }
    )
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
pub enum RunRuntimeError {
    #[error(transparent)]
    Harness(#[from] HarnessError),
    #[error(transparent)]
    Store(#[from] RunStoreError),
    #[error(transparent)]
    Flow(#[from] FlowError),
    #[error("agent machine failed: {0}")]
    Machine(String),
    #[error("run persistence failed: {0}")]
    Persistence(String),
}

impl RunRuntimeError {
    fn machine(error: agent_core::harness::MachineError) -> Self {
        Self::Machine(format!("{}: {}", error.code, error.message))
    }

    fn machine_message(message: impl Into<String>) -> Self {
        Self::Machine(message.into())
    }
}

fn validate_machine_plan(plan: &PlannedRun) -> Result<(), HarnessError> {
    if plan.input.trim().is_empty() {
        return Err(HarnessError::InvalidInput);
    }
    if let Some(requested) = plan.max_steps
        && !(1..=MAX_RUN_STEPS).contains(&requested)
    {
        return Err(HarnessError::InvalidMaxSteps { requested });
    }
    Ok(())
}

pub fn unix_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn deadline_after(now_ms: i64, duration: Duration) -> i64 {
    now_ms.saturating_add(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use agent_core::harness::{
        AgentEvent, AgentEventStream, AgentLoop, ApprovalId, ApprovalResolution, CheckpointCodec,
        CheckpointEnvelope, FinishReason, MachineError, MachineStartRequest, ModelEvent,
        ModelEventStream, ModelPort, ModelRequest, ModelRole, RunRequest, RunStatus,
        RunStoreFuture, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolOutput, ToolPort,
        ToolRiskLevel, WaitSpec,
    };
    use agent_core::{
        event_runtime::{
            ClaimDeliveries, CompleteDelivery, CreateSubscription, DeliveryTarget, EventFilter,
            EventId, EventSource, EventStore, PublishEvent, StartPosition, SubscriptionId,
            SubscriptionMode, SubscriptionOwner, SubscriptionScope,
        },
        harness::{AgentMachine, MachineStream, WakeFlowRun},
    };
    use agent_extension::store::{InMemoryRunStore, SqliteEventStore};
    use async_stream::stream as async_stream;
    use futures_util::stream;
    use serde_json::json;
    use tempfile::tempdir;

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

    #[derive(Debug)]
    struct ManyDeltas;

    impl Agent for ManyDeltas {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("many-deltas", "test")
        }

        fn run(&self, _request: RunRequest) -> AgentEventStream {
            let mut events: Vec<_> = (0..64).map(|_| AgentEvent::text_delta("x")).collect();
            events.push(AgentEvent::completed(FinishReason::Stop));
            Box::pin(stream::iter(events))
        }
    }

    struct RestartAwareProvider;

    impl ModelPort for RestartAwareProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            if request
                .messages
                .last()
                .is_some_and(|message| message.role == ModelRole::Tool)
            {
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::TextDelta {
                        delta: "resumed after approval".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::Stop,
                    },
                ]));
            }
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::ToolCallStarted {
                    call_id: "call_restart".into(),
                    name: "restart_tool".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "call_restart".into(),
                    delta: "{}".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]))
        }
    }

    struct RestartTool {
        calls: Arc<AtomicUsize>,
    }

    impl ToolPort for RestartTool {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![
                ToolDefinition::new(
                    "restart_tool",
                    "A restart-safe test tool.",
                    json!({"type": "object", "properties": {}}),
                )
                .with_risk_level(ToolRiskLevel::High),
            ]
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::text(r#"{"restored":true}"#)) })
        }
    }

    #[derive(Debug)]
    struct DurableWaitMachine {
        approval_id: ApprovalId,
    }

    impl Agent for DurableWaitMachine {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("durable-wait", "test")
        }

        fn run(&self, _request: RunRequest) -> AgentEventStream {
            Box::pin(stream::pending())
        }
    }

    impl AgentMachine for DurableWaitMachine {
        fn metadata(&self) -> AgentMetadata {
            Agent::metadata(self)
        }

        fn initial_checkpoint(
            &self,
            _request: &MachineStartRequest,
        ) -> Result<CheckpointEnvelope, MachineError> {
            Ok(test_checkpoint(0))
        }

        fn start(&self, request: MachineStartRequest) -> MachineStream {
            durable_wait_stream(
                request.run_id,
                self.approval_id,
                test_checkpoint(0),
                Vec::new(),
                request.activated_at_ms,
            )
        }

        fn resume(&self, request: MachineResumeRequest) -> MachineStream {
            durable_wait_stream(
                request.run_id,
                self.approval_id,
                request.checkpoint,
                request.inbox,
                request.activated_at_ms,
            )
        }
    }

    #[derive(Debug)]
    struct SlowLeaseMachine {
        activations: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl Agent for SlowLeaseMachine {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("slow-lease", "test")
        }

        fn run(&self, _request: RunRequest) -> AgentEventStream {
            Box::pin(stream::pending())
        }
    }

    impl AgentMachine for SlowLeaseMachine {
        fn metadata(&self) -> AgentMetadata {
            Agent::metadata(self)
        }

        fn initial_checkpoint(
            &self,
            _request: &MachineStartRequest,
        ) -> Result<CheckpointEnvelope, MachineError> {
            Ok(CheckpointEnvelope {
                agent_kind: "slow-lease".into(),
                schema_version: 1,
                codec: CheckpointCodec::Json,
                payload: json!({}),
            })
        }

        fn start(&self, request: MachineStartRequest) -> MachineStream {
            self.activation_stream(request.run_id)
        }

        fn resume(&self, request: MachineResumeRequest) -> MachineStream {
            self.activation_stream(request.run_id)
        }
    }

    impl SlowLeaseMachine {
        fn activation_stream(&self, _run_id: RunId) -> MachineStream {
            self.activations.fetch_add(1, Ordering::SeqCst);
            let delay = self.delay;
            Box::pin(async_stream! {
                tokio::time::sleep(delay).await;
                yield MachineOutput::Event(AgentEvent::text_delta("completed once"));
                yield MachineOutput::Yield(StepOutcome::Complete {
                    finish_reason: FinishReason::Stop,
                });
            })
        }
    }

    fn test_checkpoint(stage: u32) -> CheckpointEnvelope {
        CheckpointEnvelope {
            agent_kind: "durable-wait".into(),
            schema_version: 1,
            codec: CheckpointCodec::Json,
            payload: json!({"stage": stage}),
        }
    }

    fn durable_wait_stream(
        run_id: RunId,
        approval_id: ApprovalId,
        checkpoint: CheckpointEnvelope,
        inbox: Vec<agent_core::harness::FlowInboxItem>,
        now_ms: i64,
    ) -> MachineStream {
        let stage = checkpoint
            .payload
            .get("stage")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        Box::pin(async_stream! {
            if stage == 0 {
                yield MachineOutput::Event(AgentEvent::ToolCallStarted {
                    call_id: "call_wait".into(),
                    name: "risky".into(),
                });
                yield MachineOutput::Event(AgentEvent::ApprovalRequested {
                    approval_id,
                    call_id: "call_wait".into(),
                    tool_name: "risky".into(),
                    risk_level: ToolRiskLevel::High,
                    arguments: json!({}),
                });
                let key = approval_id.to_string();
                yield MachineOutput::Yield(StepOutcome::Suspend {
                    checkpoint: test_checkpoint(1),
                    waits: vec![WaitSpec {
                        wait_key: "approval".into(),
                        subscription: CreateSubscription {
                            subscription_id: SubscriptionId::stable("test-approval", &key),
                            owner: SubscriptionOwner::Run { run_id },
                            scope: SubscriptionScope::Run { run_id },
                            filter: EventFilter {
                                topics: vec!["tool.approval.resolved".into()],
                                correlation_id: Some(key),
                                ..EventFilter::default()
                            },
                            delivery: DeliveryTarget::WakeRun {
                                run_id,
                                wait_key: "approval".into(),
                            },
                            mode: SubscriptionMode::Once,
                            start_position: StartPosition::Now,
                            expires_at_ms: None,
                            max_deliveries: Some(1),
                            created_at_ms: now_ms,
                        },
                    }],
                    effects: Vec::new(),
                });
                return;
            }
            assert_eq!(inbox.len(), 1);
            yield MachineOutput::Event(AgentEvent::ApprovalResolved {
                approval_id,
                call_id: "call_wait".into(),
                resolution: ApprovalResolution::allow_once(),
            });
            yield MachineOutput::Yield(StepOutcome::Complete {
                finish_reason: FinishReason::Stop,
            });
        })
    }

    #[derive(Debug)]
    struct DelayedBatchStore {
        inner: InMemoryRunStore,
        delay: Duration,
        batches: AtomicUsize,
    }

    impl DelayedBatchStore {
        fn new(delay: Duration) -> Self {
            Self {
                inner: InMemoryRunStore::default(),
                delay,
                batches: AtomicUsize::new(0),
            }
        }
    }

    impl RunStore for DelayedBatchStore {
        fn create_run(&self, snapshot: RunSnapshot) -> RunStoreFuture<'_, RunSnapshot> {
            self.inner.create_run(snapshot)
        }

        fn get_run(&self, run_id: RunId) -> RunStoreFuture<'_, Option<RunSnapshot>> {
            self.inner.get_run(run_id)
        }

        fn set_execution_manifest(
            &self,
            run_id: RunId,
            manifest: Value,
        ) -> RunStoreFuture<'_, RunSnapshot> {
            self.inner.set_execution_manifest(run_id, manifest)
        }

        fn append_event(
            &self,
            event: RunEvent,
            observed_at_ms: i64,
        ) -> RunStoreFuture<'_, RunSnapshot> {
            self.inner.append_event(event, observed_at_ms)
        }

        fn append_events(&self, events: Vec<ObservedRunEvent>) -> RunStoreFuture<'_, RunSnapshot> {
            Box::pin(async move {
                self.batches.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(self.delay).await;
                self.inner.append_events(events).await
            })
        }

        fn events_after(
            &self,
            run_id: RunId,
            after_seq: u64,
            limit: usize,
        ) -> RunStoreFuture<'_, Vec<RunEvent>> {
            self.inner.events_after(run_id, after_seq, limit)
        }

        fn unfinished_runs(&self) -> RunStoreFuture<'_, Vec<RunSnapshot>> {
            self.inner.unfinished_runs()
        }
    }

    #[tokio::test]
    async fn live_deltas_do_not_wait_for_persistence_and_writes_are_batched() {
        let concrete = Arc::new(DelayedBatchStore::new(Duration::from_millis(250)));
        let store: Arc<dyn RunStore> = concrete.clone();
        let runtime = RunRuntime::new(Harness::new(ManyDeltas), store, |_| {});
        let mut started = runtime
            .start("hello".into())
            .await
            .expect("run should start");

        let first_delta = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                let event = started
                    .events
                    .recv()
                    .await
                    .expect("stream should remain open");
                if matches!(event.kind, RunEventKind::OutputDelta { .. }) {
                    return event;
                }
            }
        })
        .await
        .expect("live delta should not wait for the delayed store");
        assert_eq!(first_delta.seq, 2);

        loop {
            let event = started
                .events
                .recv()
                .await
                .expect("stream should remain open");
            if event.kind.is_terminal() {
                break;
            }
        }
        let snapshot = concrete
            .get_run(started.run_id)
            .await
            .expect("snapshot should load")
            .expect("snapshot should exist");
        assert_eq!(snapshot.status, RunStatus::Completed);
        assert_eq!(snapshot.output.len(), 64);
        assert!(
            concrete.batches.load(Ordering::Relaxed) <= 3,
            "64 deltas should be persisted in a small number of batches"
        );
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

    #[tokio::test]
    async fn durable_wait_survives_runtime_and_sqlite_reopen_then_resumes() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("flows.sqlite3");
        let approval_id = ApprovalId::new();
        let run_id = RunId::new();
        let runs = Arc::new(InMemoryRunStore::default());
        let run_store: Arc<dyn RunStore> = runs.clone();
        let first_adapter = Arc::new(
            SqliteEventStore::open(&path, "flows:test")
                .await
                .expect("flow store should open"),
        );
        let first_flows: Arc<dyn FlowStore> = first_adapter.clone();
        let first = RunRuntime::new_durable(
            Harness::new(DurableWaitMachine { approval_id }),
            Arc::clone(&run_store),
            first_flows,
            |_| {},
        );
        let started = first
            .start_planned(PlannedRun {
                run_id,
                input: "wait durably".into(),
                attachments: Vec::new(),
                prior_messages: Vec::new(),
                allowed_tools: None,
                allow_run_adf: false,
                max_steps: None,
                context_fingerprint: None,
                execution_manifest: None,
            })
            .await
            .expect("durable run should start");
        drop(started.events);
        first
            .drive_flows_once("first-flow-worker")
            .await
            .expect("first activation should claim");
        wait_for_status(&run_store, run_id, RunStatus::WaitingEvent).await;
        let waiting = first_adapter
            .get(run_id)
            .await
            .expect("flow should load")
            .expect("flow should exist");
        assert_eq!(waiting.status, FlowRunStatus::WaitingEvent);
        drop(first);
        drop(first_adapter);

        let reopened = Arc::new(
            SqliteEventStore::open(&path, "flows:test")
                .await
                .expect("flow store should reopen"),
        );
        let reopened_flows: Arc<dyn FlowStore> = reopened.clone();
        let second = RunRuntime::new_durable(
            Harness::new(DurableWaitMachine { approval_id }),
            Arc::clone(&run_store),
            Arc::clone(&reopened_flows),
            |_| {},
        );
        assert_eq!(
            second
                .recover_interrupted()
                .await
                .expect("waiting recovery should succeed"),
            0,
            "waiting durable runs must not be failed during startup recovery"
        );
        let published = reopened
            .publish(PublishEvent {
                event_id: EventId::new(),
                topic: "tool.approval.resolved".into(),
                event_type: "tool.approval.resolved".into(),
                schema_version: 1,
                source: EventSource::Gateway,
                subject: Some(format!("run/{run_id}")),
                correlation_id: Some(approval_id.to_string()),
                causation_id: None,
                occurred_at_ms: 20,
                recorded_at_ms: 20,
                payload: json!({"decision": "allow-once"}),
            })
            .await
            .expect("approval event should publish");
        assert_eq!(published.delivery_ids.len(), 1);
        let delivery = reopened
            .claim_deliveries(ClaimDeliveries {
                worker_id: "event-worker".into(),
                now_ms: 20,
                lease_until_ms: 1_020,
                limit: 1,
            })
            .await
            .expect("approval delivery should claim")
            .into_iter()
            .next()
            .expect("one approval delivery should exist");
        reopened_flows
            .wake(WakeFlowRun {
                run_id,
                subscription_id: delivery.delivery_id.subscription_id,
                event_id: delivery.delivery_id.event_id,
                woken_at_ms: 20,
            })
            .await
            .expect("approval should wake flow");
        reopened
            .complete_delivery(CompleteDelivery {
                delivery_id: delivery.delivery_id,
                worker_id: "event-worker".into(),
                delivered_at_ms: 20,
            })
            .await
            .expect("approval delivery should complete");

        second
            .drive_flows_once("second-flow-worker")
            .await
            .expect("resumed activation should claim");
        wait_for_status(&run_store, run_id, RunStatus::Completed).await;
        let flow = reopened_flows
            .get(run_id)
            .await
            .expect("flow should load")
            .expect("flow should exist");
        assert_eq!(flow.status, FlowRunStatus::Completed);
        let events = run_store
            .events_after(run_id, 0, 100)
            .await
            .expect("run events should load");
        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, RunEventKind::RunWaiting { .. }))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, RunEventKind::RunResumed { .. }))
        );
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }

    #[tokio::test]
    async fn heartbeat_prevents_reclaim_during_a_long_activation() {
        let directory = tempdir().expect("temporary directory should exist");
        let adapter = Arc::new(
            SqliteEventStore::open(directory.path().join("lease.sqlite3"), "flows:test")
                .await
                .expect("flow store should open"),
        );
        let flows: Arc<dyn FlowStore> = adapter;
        let runs = Arc::new(InMemoryRunStore::default());
        let run_store: Arc<dyn RunStore> = runs;
        let activations = Arc::new(AtomicUsize::new(0));
        let runtime = RunRuntime::new_durable(
            Harness::new(SlowLeaseMachine {
                activations: Arc::clone(&activations),
                delay: Duration::from_millis(600),
            }),
            Arc::clone(&run_store),
            Arc::clone(&flows),
            |_| {},
        )
        .with_flow_activation_timing(Duration::from_millis(200), Duration::from_millis(40));
        let run_id = RunId::new();
        let started = runtime
            .start_planned(PlannedRun {
                run_id,
                input: "stay active beyond the original lease".into(),
                attachments: Vec::new(),
                prior_messages: Vec::new(),
                allowed_tools: None,
                allow_run_adf: false,
                max_steps: None,
                context_fingerprint: None,
                execution_manifest: None,
            })
            .await
            .expect("durable run should start");
        drop(started.events);
        runtime
            .drive_flows_once("worker-1")
            .await
            .expect("first activation should claim");

        tokio::time::sleep(Duration::from_millis(350)).await;
        runtime
            .drive_flows_once("worker-2")
            .await
            .expect("reclaim check should succeed");
        assert_eq!(
            activations.load(Ordering::SeqCst),
            1,
            "a heartbeat-protected activation must not be reclaimed"
        );

        wait_for_status(&run_store, run_id, RunStatus::Completed).await;
        let events = run_store
            .events_after(run_id, 0, 100)
            .await
            .expect("run events should load");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.kind, RunEventKind::RunStarted))
                .count(),
            1
        );
        assert!(
            events
                .iter()
                .all(|event| !matches!(event.kind, RunEventKind::RunResumed { .. }))
        );
    }

    #[tokio::test]
    async fn real_agent_loop_approval_resumes_after_runtime_restart() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("agent-loop.sqlite3");
        let run_id = RunId::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let runs = Arc::new(InMemoryRunStore::default());
        let run_store: Arc<dyn RunStore> = runs;
        let first_adapter = Arc::new(
            SqliteEventStore::open(&path, "agent-loop:test")
                .await
                .expect("flow store should open"),
        );
        let first_flows: Arc<dyn FlowStore> = first_adapter.clone();
        let first_agent = AgentLoop::new(
            RestartAwareProvider,
            RestartTool {
                calls: Arc::clone(&calls),
            },
            "test-model",
            "system",
            None,
            4,
        );
        let first = RunRuntime::new_durable(
            Harness::new(first_agent),
            Arc::clone(&run_store),
            first_flows,
            |_| {},
        );
        let started = first
            .start_planned(PlannedRun {
                run_id,
                input: "run a risky tool".into(),
                attachments: Vec::new(),
                prior_messages: Vec::new(),
                allowed_tools: None,
                allow_run_adf: false,
                max_steps: Some(4),
                context_fingerprint: Some("sha256:test".into()),
                execution_manifest: None,
            })
            .await
            .expect("agent loop should start");
        drop(started.events);
        first
            .drive_flows_once("first-agent-worker")
            .await
            .expect("approval activation should claim");
        wait_for_status(&run_store, run_id, RunStatus::WaitingEvent).await;
        let approval_id = run_store
            .get_run(run_id)
            .await
            .expect("run should load")
            .expect("run should exist")
            .approvals
            .into_iter()
            .next()
            .expect("approval should be projected")
            .approval_id;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(first);
        drop(first_adapter);

        let reopened = Arc::new(
            SqliteEventStore::open(&path, "agent-loop:test")
                .await
                .expect("flow store should reopen"),
        );
        let flows: Arc<dyn FlowStore> = reopened.clone();
        let second_agent = AgentLoop::new(
            RestartAwareProvider,
            RestartTool {
                calls: Arc::clone(&calls),
            },
            "test-model",
            "system",
            None,
            4,
        );
        let second = RunRuntime::new_durable(
            Harness::new(second_agent),
            Arc::clone(&run_store),
            Arc::clone(&flows),
            |_| {},
        );
        assert_eq!(
            second
                .recover_interrupted()
                .await
                .expect("durable recovery should succeed"),
            0
        );
        reopened
            .publish(PublishEvent {
                event_id: EventId::stable("test-resolution", &approval_id.to_string()),
                topic: "tool.approval.resolved".into(),
                event_type: "tool.approval.resolved".into(),
                schema_version: 1,
                source: EventSource::Gateway,
                subject: Some(format!("run/{run_id}")),
                correlation_id: Some(approval_id.to_string()),
                causation_id: None,
                occurred_at_ms: 30,
                recorded_at_ms: 30,
                payload: json!({"decision": "allow-once"}),
            })
            .await
            .expect("approval should publish");
        let delivery = reopened
            .claim_deliveries(ClaimDeliveries {
                worker_id: "approval-worker".into(),
                now_ms: 30,
                lease_until_ms: 1_030,
                limit: 1,
            })
            .await
            .expect("approval delivery should claim")
            .into_iter()
            .next()
            .expect("approval delivery should exist");
        flows
            .wake(WakeFlowRun {
                run_id,
                subscription_id: delivery.delivery_id.subscription_id,
                event_id: delivery.delivery_id.event_id,
                woken_at_ms: 30,
            })
            .await
            .expect("approval should wake run");
        reopened
            .complete_delivery(CompleteDelivery {
                delivery_id: delivery.delivery_id,
                worker_id: "approval-worker".into(),
                delivered_at_ms: 30,
            })
            .await
            .expect("approval delivery should complete");

        second
            .drive_flows_once("resume-tool-worker")
            .await
            .expect("tool resume should claim");
        wait_for_flow_status(&flows, run_id, FlowRunStatus::Runnable).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        second
            .drive_flows_once("resume-model-worker")
            .await
            .expect("model continuation should claim");
        wait_for_status(&run_store, run_id, RunStatus::Completed).await;
        let snapshot = run_store
            .get_run(run_id)
            .await
            .expect("run should load")
            .expect("run should exist");
        assert_eq!(snapshot.output, "resumed after approval");
        assert_eq!(
            snapshot.approvals[0].resolution,
            Some(ApprovalResolution::allow_once())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    async fn wait_for_status(store: &Arc<dyn RunStore>, run_id: RunId, expected: RunStatus) {
        for _ in 0..100 {
            let snapshot = store
                .get_run(run_id)
                .await
                .expect("snapshot should load")
                .expect("snapshot should exist");
            if snapshot.status == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let status = store
            .get_run(run_id)
            .await
            .expect("snapshot should load")
            .expect("snapshot should exist")
            .status;
        panic!("run did not reach {expected:?}; current status is {status:?}");
    }

    async fn wait_for_flow_status(
        store: &Arc<dyn FlowStore>,
        run_id: RunId,
        expected: FlowRunStatus,
    ) {
        for _ in 0..100 {
            let state = store
                .get(run_id)
                .await
                .expect("flow should load")
                .expect("flow should exist");
            if state.status == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let status = store
            .get(run_id)
            .await
            .expect("flow should load")
            .expect("flow should exist")
            .status;
        panic!("flow did not reach {expected:?}; current status is {status:?}");
    }
}
