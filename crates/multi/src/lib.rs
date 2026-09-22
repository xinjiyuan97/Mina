//! Multi-agent coordination primitives. Patterns build on the transport here.
pub mod harness_node;
pub mod sqlite_bus;
pub use harness_node::HarnessAgentNode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use sqlite_bus::{
    DurableEventBus, SqliteBus, SqliteBusHandle, SqliteSubscription, SqliteSupervisorStore,
};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Topic(pub String);
impl<T: Into<String>> From<T> for Topic {
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub run_id: Uuid,
    pub topic: Topic,
    pub sender: String,
    pub kind: String,
    pub correlation_id: Option<Uuid>,
    pub payload: Value,
}
impl Message {
    pub fn new(
        run_id: Uuid,
        topic: impl Into<Topic>,
        sender: impl Into<String>,
        kind: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            run_id,
            topic: topic.into(),
            sender: sender.into(),
            kind: kind.into(),
            correlation_id: None,
            payload,
        }
    }
}

#[derive(Clone)]
pub struct InMemoryBus {
    capacity: usize,
    topics: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<Topic, broadcast::Sender<Message>>>,
    >,
}
pub trait EventBus: Clone + Send + Sync + 'static {
    type Subscription: Send;
    type Error;
    fn publish(&self, message: Message) -> Result<usize, Self::Error>;
    fn subscribe(&self, topic: impl Into<Topic>) -> Self::Subscription;
}
impl EventBus for InMemoryBus {
    type Subscription = Subscription;
    type Error = PublishError;
    fn publish(&self, message: Message) -> Result<usize, PublishError> {
        InMemoryBus::publish(self, message)
    }
    fn subscribe(&self, topic: impl Into<Topic>) -> Subscription {
        InMemoryBus::subscribe(self, topic)
    }
}
impl InMemoryBus {
    /// Capacity is per topic. A noisy topic never evicts another topic's messages.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "bus capacity must be positive");
        Self {
            capacity,
            topics: Default::default(),
        }
    }
    /// Returns the number of receivers subscribed to this exact topic.
    pub fn publish(&self, message: Message) -> Result<usize, PublishError> {
        let topics = self.topics.lock().expect("topic registry lock");
        topics
            .get(&message.topic)
            .ok_or(PublishError::NoSubscribers)?
            .send(message)
            .map_err(|_| PublishError::NoSubscribers)
    }
    pub fn subscribe(&self, topic: impl Into<Topic>) -> Subscription {
        let topic = topic.into();
        let mut topics = self.topics.lock().expect("topic registry lock");
        // Retire inactive topics on subscription churn; no historical messages replay.
        topics.retain(|_, sender| sender.receiver_count() > 0);
        let sender = topics
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.capacity).0);
        Subscription {
            rx: sender.subscribe(),
        }
    }
    pub fn subscriber_count(&self, topic: impl Into<Topic>) -> usize {
        self.topics
            .lock()
            .expect("topic registry lock")
            .get(&topic.into())
            .map_or(0, broadcast::Sender::receiver_count)
    }
}
pub struct Subscription {
    rx: broadcast::Receiver<Message>,
}
impl Subscription {
    pub async fn recv(&mut self) -> Result<Message, RecvError> {
        match self.rx.recv().await {
            Ok(message) => Ok(message),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(RecvError::Lagged(n)),
            Err(broadcast::error::RecvError::Closed) => Err(RecvError::Closed),
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("no subscribers")]
    NoSubscribers,
    #[error("worker selector returned no worker")]
    NoWorker,
}
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    #[error("subscription closed")]
    Closed,
    #[error("subscription lagged by {0} messages")]
    Lagged(u64),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentDescriptor {
    pub id: String,
    pub role: String,
}
pub trait Pattern: Send + Sync {
    fn name(&self) -> &'static str;
}

/// A hostable agent endpoint. Implementations usually wrap `agent_harness::Harness`.
pub trait AgentNode: Send + Sync {
    fn descriptor(&self) -> AgentDescriptor;
    fn handle(
        &self,
        message: Message,
        bus: InMemoryBus,
    ) -> futures_core::future::BoxFuture<'static, ()>;
    fn handle_with_cancel(
        &self,
        message: Message,
        bus: InMemoryBus,
        cancel: tokio_util::sync::CancellationToken,
    ) -> futures_core::future::BoxFuture<'static, ()> {
        let _ = cancel;
        self.handle(message, bus)
    }
}

/// Durable transport node boundary. A message is acknowledged only after the
/// handler future completes successfully; errors leave it available for replay.
pub trait DurableAgentNode: Send + Sync {
    fn handle_durable(
        &self,
        message: Message,
        bus: SqliteBusHandle,
    ) -> futures_core::future::BoxFuture<
        'static,
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    >;
    fn handle_durable_with_cancel(
        &self,
        message: Message,
        bus: SqliteBusHandle,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> futures_core::future::BoxFuture<
        'static,
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    > {
        self.handle_durable(message, bus)
    }
}

/// Convenience durable node for applications that already own their transport
/// mapping. The callback can publish follow-up messages through the handle.
pub struct DurableFunctionNode<F> {
    pub descriptor: AgentDescriptor,
    pub handler: F,
}
impl<F> DurableFunctionNode<F> {
    pub fn new(descriptor: AgentDescriptor, handler: F) -> Self {
        Self {
            descriptor,
            handler,
        }
    }
}
impl<F, Fut> DurableAgentNode for DurableFunctionNode<F>
where
    F: Fn(Message, SqliteBusHandle) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
        + Send
        + 'static,
{
    fn handle_durable(
        &self,
        message: Message,
        bus: SqliteBusHandle,
    ) -> futures_core::future::BoxFuture<
        'static,
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    > {
        Box::pin((self.handler)(message, bus))
    }
}

pub async fn serve_durable_node<N: DurableAgentNode + 'static>(
    node: std::sync::Arc<N>,
    bus: SqliteBusHandle,
    consumer: impl Into<String>,
    topic: Topic,
    cancel: tokio_util::sync::CancellationToken,
) -> rusqlite::Result<()> {
    let consumer = consumer.into();
    let mut sub = bus.subscribe(consumer, topic);
    loop {
        let (seq, message) = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = sub.recv_wait() => result?,
        };
        let result = node
            .handle_durable_with_cancel(message, bus.clone(), cancel.clone())
            .await;
        if cancel.is_cancelled() {
            return Ok(());
        }
        if result.is_ok() {
            sub.ack(seq).await?;
        }
    }
}

/// Durable task dispatch primitive used by supervisors backed by SQLite.
pub fn dispatch_durable(
    bus: &SqliteBusHandle,
    run_id: Uuid,
    worker: &AgentDescriptor,
    payload: Value,
) -> Result<Uuid, rusqlite::Error> {
    let request = Message::new(
        run_id,
        format!("agent.{}.request", worker.id),
        "supervisor",
        "task.request",
        payload,
    );
    bus.publish(&request)?;
    Ok(request.id)
}

#[cfg(test)]
mod durable_runtime_tests {
    use super::*;
    use serde_json::json;
    #[tokio::test]
    async fn durable_runtime_acks_only_after_success() {
        let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let node = std::sync::Arc::new(DurableFunctionNode::new(
            AgentDescriptor {
                id: "worker".into(),
                role: "worker".into(),
            },
            |_m: Message, _b: SqliteBusHandle| async { Ok(()) },
        ));
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(serve_durable_node(
            node,
            bus.clone(),
            "c",
            Topic("tasks".into()),
            cancel.clone(),
        ));
        let msg = Message::new(
            Uuid::new_v4(),
            "tasks",
            "supervisor",
            "task.request",
            json!({"input":"x"}),
        );
        bus.publish(&msg).unwrap();
        for _ in 0..50 {
            if bus
                .replay("c", &Topic("tasks".into()), 1)
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            bus.replay("c", &Topic("tasks".into()), 1)
                .unwrap()
                .is_empty()
        );
        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn durable_runtime_failure_leaves_message_for_replay() {
        let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let node = std::sync::Arc::new(DurableFunctionNode::new(
            AgentDescriptor {
                id: "failing".into(),
                role: "worker".into(),
            },
            |_m: Message, _b: SqliteBusHandle| async { Err("boom".into()) },
        ));
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(serve_durable_node(
            node,
            bus.clone(),
            "c",
            Topic("tasks".into()),
            cancel.clone(),
        ));
        let msg = Message::new(Uuid::new_v4(), "tasks", "s", "task.request", json!({}));
        bus.publish(&msg).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(bus.replay("c", &Topic("tasks".into()), 1).unwrap().len(), 1);
        cancel.cancel();
        task.await.unwrap().unwrap();
    }
}

/// Adapter boundary for a concrete Harness runner. Implementations translate
/// the message payload into their host's RunRequest and publish lifecycle events.
pub trait HarnessRunner: Send + Sync {
    fn run(
        &self,
        message: Message,
        bus: InMemoryBus,
    ) -> futures_core::future::BoxFuture<'static, ()>;
}
pub struct HarnessNode<R> {
    pub descriptor: AgentDescriptor,
    pub runner: std::sync::Arc<R>,
}
impl<R: HarnessRunner + 'static> AgentNode for HarnessNode<R> {
    fn descriptor(&self) -> AgentDescriptor {
        self.descriptor.clone()
    }
    fn handle(
        &self,
        message: Message,
        bus: InMemoryBus,
    ) -> futures_core::future::BoxFuture<'static, ()> {
        self.runner.run(message, bus)
    }
}

/// Convenience node for integrating any async handler (including Harness).
pub struct FunctionNode<F> {
    pub descriptor: AgentDescriptor,
    pub handler: F,
}
impl<F> FunctionNode<F> {
    pub fn new(descriptor: AgentDescriptor, handler: F) -> Self {
        Self {
            descriptor,
            handler,
        }
    }
}
impl<F, Fut> AgentNode for FunctionNode<F>
where
    F: Fn(Message, InMemoryBus) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    fn descriptor(&self) -> AgentDescriptor {
        self.descriptor.clone()
    }
    fn handle(
        &self,
        message: Message,
        bus: InMemoryBus,
    ) -> futures_core::future::BoxFuture<'static, ()> {
        Box::pin((self.handler)(message, bus))
    }
}

/// Minimal supervisor coordinator: delegates tasks and collects completions.
pub struct Supervisor {
    pub bus: InMemoryBus,
    pub run_id: Uuid,
    pub workers: Vec<AgentDescriptor>,
    tasks: std::sync::Mutex<std::collections::HashMap<Uuid, TaskRecord>>,
}

/// Durable state boundary for supervisor implementations. The contract keeps
/// task state independent from message transport so storage can be swapped
/// without changing topology APIs.
pub trait SupervisorStore: Send + Sync {
    type Error;
    fn load_tasks(&self, run_id: Uuid) -> Result<Vec<TaskRecord>, Self::Error>;
    fn save_tasks(&self, run_id: Uuid, tasks: &[TaskRecord]) -> Result<(), Self::Error>;
    fn commit_result(
        &self,
        run_id: Uuid,
        tasks: &[TaskRecord],
        consumer: &str,
        topic: &Topic,
        sequence: i64,
        checkpoint: &Message,
    ) -> Result<(), Self::Error>;
}

#[derive(Default)]
pub struct InMemorySupervisorStore(
    std::sync::Mutex<std::collections::HashMap<Uuid, Vec<TaskRecord>>>,
);
impl SupervisorStore for InMemorySupervisorStore {
    type Error = String;
    fn load_tasks(&self, run_id: Uuid) -> Result<Vec<TaskRecord>, Self::Error> {
        Ok(self
            .0
            .lock()
            .map_err(|e| e.to_string())?
            .get(&run_id)
            .cloned()
            .unwrap_or_default())
    }
    fn save_tasks(&self, run_id: Uuid, tasks: &[TaskRecord]) -> Result<(), Self::Error> {
        self.0
            .lock()
            .map_err(|e| e.to_string())?
            .insert(run_id, tasks.to_vec());
        Ok(())
    }
    fn commit_result(
        &self,
        run_id: Uuid,
        tasks: &[TaskRecord],
        _consumer: &str,
        _topic: &Topic,
        _sequence: i64,
        _checkpoint: &Message,
    ) -> Result<(), Self::Error> {
        self.save_tasks(run_id, tasks)
    }
}

/// Simple pluggable worker selection policy for topology-driven dispatch.
pub trait WorkerSelector: Send + Sync {
    fn select(&self, workers: &[AgentDescriptor], task_index: usize) -> Option<AgentDescriptor>;
}
pub struct RoundRobinSelector;
impl WorkerSelector for RoundRobinSelector {
    fn select(&self, workers: &[AgentDescriptor], task_index: usize) -> Option<AgentDescriptor> {
        workers.get(task_index % workers.len().max(1)).cloned()
    }
}

/// Runs a node's subscription loop. The callback is intentionally transport-level,
/// so adapters can invoke Harness with their own RunRequest mapping.
pub async fn serve_node<N: AgentNode + 'static>(
    node: std::sync::Arc<N>,
    bus: InMemoryBus,
    topic: Topic,
) -> Result<(), RecvError> {
    serve_node_with_cancel(node, bus, topic, tokio_util::sync::CancellationToken::new()).await
}
pub async fn serve_node_with_cancel<N: AgentNode + 'static>(
    node: std::sync::Arc<N>,
    bus: InMemoryBus,
    topic: Topic,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), RecvError> {
    let mut subscription = bus.subscribe(topic);
    loop {
        let message = tokio::select! { _ = cancel.cancelled() => return Ok(()), message = subscription.recv() => message? };
        let node = node.clone();
        let bus_for_task = bus.clone();
        let task_cancel = cancel.clone();
        tokio::spawn(async move {
            node.handle_with_cancel(message, bus_for_task, task_cancel)
                .await;
        });
    }
}
impl Supervisor {
    pub fn checkpoint_topic(&self) -> Topic {
        format!("run.{}.supervisor.checkpoint", self.run_id).into()
    }
    pub fn checkpoint_message(&self) -> Result<Message, serde_json::Error> {
        let payload = self.checkpoint()?;
        let mut message = Message::new(
            self.run_id,
            self.checkpoint_topic(),
            "supervisor",
            "supervisor.checkpoint",
            payload.clone(),
        );
        message.id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("supervisor-checkpoint:{}:{}", self.run_id, payload).as_bytes(),
        );
        Ok(message)
    }
    pub async fn persist_checkpoint(&self, bus: &SqliteBusHandle) -> rusqlite::Result<i64> {
        let message = self
            .checkpoint_message()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        bus.publish_async(message).await
    }
    pub async fn persist_task_state(&self, bus: &SqliteBusHandle) -> rusqlite::Result<()> {
        let tasks = self
            .tasks
            .lock()
            .expect("tasks lock")
            .values()
            .cloned()
            .collect();
        bus.upsert_supervisor_tasks(self.run_id, tasks).await
    }
    pub async fn restore_checkpoint_durable(
        &self,
        bus: &SqliteBusHandle,
        consumer: impl Into<String>,
    ) -> rusqlite::Result<()> {
        let rows = bus
            .replay_async(consumer.into(), self.checkpoint_topic(), u32::MAX)
            .await?;
        let message = rows.last().ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if message.1.run_id != self.run_id || message.1.kind != "supervisor.checkpoint" {
            return Err(rusqlite::Error::InvalidParameterName(
                "checkpoint envelope mismatch".into(),
            ));
        }
        self.restore_checkpoint(message.1.payload.clone())
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
    }
    /// Runs the durable result loop until all supplied tasks reach a terminal
    /// state or the deadline expires. Cancellation stops consumption and emits
    /// cancellation requests for remaining tasks.
    pub async fn run_durable_until_terminal(
        &self,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
        task_ids: &[Uuid],
        timeout: std::time::Duration,
    ) -> rusqlite::Result<bool> {
        let consumer = consumer.into();
        let done = tokio::time::timeout(timeout, async {
            loop {
                if task_ids
                    .iter()
                    .all(|id| self.status(*id).map(|s| s.is_terminal()).unwrap_or(false))
                {
                    return Ok::<(), rusqlite::Error>(());
                }
                let consumed = self
                    .consume_durable_once_checkpointed(bus.clone(), consumer.clone())
                    .await?;
                if !consumed {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        })
        .await;
        let done = match done {
            Ok(result) => {
                result?;
                true
            }
            Err(_) => false,
        };
        if !done {
            let pending: Vec<_> = task_ids
                .iter()
                .filter_map(|id| self.task(*id))
                .filter(|t| !t.status.is_terminal())
                .collect();
            for task in pending {
                let mut request = Message::new(
                    self.run_id,
                    self.worker_topic(&task.worker),
                    "supervisor",
                    "task.cancel",
                    serde_json::json!({"task_id": task.id}),
                );
                request.correlation_id = Some(task.request.id);
                bus.publish_async(request).await?;
            }
        }
        Ok(done)
    }

    pub async fn run_durable_with_retry(
        &self,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
        task_ids: &[Uuid],
        timeout: std::time::Duration,
        policy: RetryPolicy,
    ) -> rusqlite::Result<bool> {
        let deadline = tokio::time::Instant::now() + timeout;
        let consumer = consumer.into();
        loop {
            if task_ids
                .iter()
                .all(|id| self.status(*id).map(|s| s.is_terminal()).unwrap_or(false))
            {
                let failed: Vec<_> = task_ids
                    .iter()
                    .filter_map(|id| self.task(*id))
                    .filter(|t| t.status == TaskStatus::Failed)
                    .collect();
                if failed.is_empty() {
                    return Ok(true);
                }
                for task in failed {
                    if self
                        .retry_durable_with_policy(&bus, task.id, policy)
                        .await
                        .is_err()
                    {
                        return Ok(false);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            let _ = self
                .consume_durable_once(bus.clone(), consumer.clone())
                .await?;
        }
    }
    pub fn dispatch_auto<S: WorkerSelector>(
        &self,
        selector: &S,
        payloads: &[Value],
    ) -> Result<Vec<Uuid>, BatchDispatchError> {
        let mut jobs = Vec::with_capacity(payloads.len());
        for (i, p) in payloads.iter().enumerate() {
            let worker = selector
                .select(&self.workers, i)
                .ok_or(BatchDispatchError {
                    dispatched: Vec::new(),
                    source: PublishError::NoWorker,
                })?;
            jobs.push((worker, p.clone()));
        }
        self.dispatch_batch(&jobs)
    }
    pub fn new(run_id: Uuid, bus: InMemoryBus) -> Self {
        Self {
            bus,
            run_id,
            workers: Vec::new(),
            tasks: Default::default(),
        }
    }
    pub fn add_worker(&mut self, worker: AgentDescriptor) {
        self.workers.push(worker);
    }
    /// A failed batch reports already-dispatched IDs; published work cannot be rolled back.
    pub fn dispatch_batch(
        &self,
        jobs: &[(AgentDescriptor, Value)],
    ) -> Result<Vec<Uuid>, BatchDispatchError> {
        let mut ids = Vec::with_capacity(jobs.len());
        for (worker, payload) in jobs {
            match self.dispatch_with_id(worker, payload.clone()) {
                Ok((id, _)) => ids.push(id),
                Err(source) => {
                    return Err(BatchDispatchError {
                        dispatched: ids,
                        source,
                    });
                }
            }
        }
        Ok(ids)
    }
    pub fn statuses(&self, ids: &[Uuid]) -> Vec<Option<TaskStatus>> {
        ids.iter().map(|id| self.status(*id)).collect()
    }
    pub fn dispatch(
        &self,
        worker: &AgentDescriptor,
        payload: Value,
    ) -> Result<usize, PublishError> {
        self.dispatch_with_id(worker, payload)
            .map(|(_, count)| count)
    }
    pub fn dispatch_registered(
        &self,
        worker: &AgentDescriptor,
        payload: Value,
    ) -> Result<usize, DispatchError> {
        if !self
            .workers
            .iter()
            .any(|candidate| candidate.id == worker.id)
        {
            return Err(DispatchError::UnknownWorker);
        }
        self.dispatch(worker, payload)
            .map_err(DispatchError::Publish)
    }
    /// Dispatches through a durable SQLite transport while recording the same
    /// task identity and pending state as the in-memory path.
    pub fn dispatch_durable(
        &self,
        durable: &SqliteBusHandle,
        worker: &AgentDescriptor,
        payload: Value,
    ) -> Result<Uuid, rusqlite::Error> {
        let request = Message::new(
            self.run_id,
            self.worker_topic(worker),
            "supervisor",
            "task.request",
            payload,
        );
        let id = request.id;
        durable.publish(&request)?;
        self.tasks.lock().expect("tasks lock").insert(
            id,
            TaskRecord {
                id,
                request,
                worker: worker.clone(),
                status: TaskStatus::Pending,
                attempts: 1,
                result: None,
                prerequisites: Vec::new(),
            },
        );
        Ok(id)
    }

    /// Dispatches a job only when all prerequisite tasks completed.
    pub fn dispatch_after(
        &self,
        worker: &AgentDescriptor,
        payload: Value,
        prerequisites: &[Uuid],
    ) -> Result<(Uuid, usize), DispatchError> {
        if prerequisites
            .iter()
            .any(|id| self.status(*id) != Some(TaskStatus::Completed))
        {
            return Err(DispatchError::PrerequisitesPending);
        }
        let dispatched = self
            .dispatch_with_id(worker, payload)
            .map_err(DispatchError::Publish)?;
        if let Some(record) = self
            .tasks
            .lock()
            .expect("tasks lock")
            .get_mut(&dispatched.0)
        {
            record.prerequisites = prerequisites.to_vec();
        }
        Ok(dispatched)
    }

    /// Returns whether a set of prerequisite task IDs can be released.
    pub fn prerequisites_completed(&self, prerequisites: &[Uuid]) -> bool {
        prerequisites
            .iter()
            .all(|id| self.status(*id) == Some(TaskStatus::Completed))
    }
    /// Registers work without publishing it until prerequisites complete.
    pub fn schedule_after(
        &self,
        worker: &AgentDescriptor,
        payload: Value,
        prerequisites: &[Uuid],
    ) -> Uuid {
        let request = Message::new(
            self.run_id,
            self.worker_topic(worker),
            "supervisor",
            "task.request",
            payload,
        );
        let id = request.id;
        self.tasks.lock().expect("tasks lock").insert(
            id,
            TaskRecord {
                id,
                request,
                worker: worker.clone(),
                status: TaskStatus::Pending,
                attempts: 0,
                result: None,
                prerequisites: prerequisites.to_vec(),
            },
        );
        id
    }
    /// Publishes all scheduled tasks whose prerequisites are now complete.
    pub fn release_ready(&self) -> Result<Vec<Uuid>, PublishError> {
        let mut released = Vec::new();
        let mut tasks = self.tasks.lock().expect("tasks lock");
        let ready: Vec<Uuid> = tasks
            .values()
            .filter(|t| {
                t.attempts == 0
                    && t.prerequisites.iter().all(|p| {
                        tasks
                            .get(p)
                            .is_some_and(|r| r.status == TaskStatus::Completed)
                    })
            })
            .map(|t| t.id)
            .collect();
        for id in ready {
            let record = tasks.get_mut(&id).expect("task exists");
            self.bus.publish(record.request.clone())?;
            record.attempts = 1;
            released.push(id);
        }
        Ok(released)
    }
    pub fn release_ready_durable(
        &self,
        durable: &SqliteBusHandle,
    ) -> Result<Vec<Uuid>, rusqlite::Error> {
        let mut released = Vec::new();
        let mut tasks = self.tasks.lock().expect("tasks lock");
        let ready: Vec<Uuid> = tasks
            .values()
            .filter(|t| {
                t.attempts == 0
                    && t.prerequisites.iter().all(|p| {
                        tasks
                            .get(p)
                            .is_some_and(|r| r.status == TaskStatus::Completed)
                    })
            })
            .map(|t| t.id)
            .collect();
        for id in ready {
            let record = tasks.get_mut(&id).expect("task exists");
            durable.publish(&record.request)?;
            record.attempts = 1;
            released.push(id);
        }
        Ok(released)
    }
    pub fn dispatch_with_id(
        &self,
        worker: &AgentDescriptor,
        payload: Value,
    ) -> Result<(Uuid, usize), PublishError> {
        let request = Message::new(
            self.run_id,
            self.worker_topic(worker),
            "supervisor",
            "task.request",
            payload,
        );
        let id = request.id;
        let mut tasks = self.tasks.lock().expect("tasks lock");
        let count = self.bus.publish(request.clone())?;
        tasks.insert(
            id,
            TaskRecord {
                id,
                request,
                worker: worker.clone(),
                status: TaskStatus::Pending,
                attempts: 1,
                result: None,
                prerequisites: Vec::new(),
            },
        );
        Ok((id, count))
    }
    /// Request cancellation from the assigned worker. The record changes only
    /// after the worker emits `task.cancelled`.
    pub fn cancel(&self, id: Uuid) -> Result<usize, CancelError> {
        let task = self.task(id).ok_or(CancelError::UnknownTask)?;
        if task.status.is_terminal() {
            return Err(CancelError::AlreadyTerminal);
        }
        let mut request = Message::new(
            self.run_id,
            self.worker_topic(&task.worker),
            "supervisor",
            "task.cancel",
            serde_json::json!({"task_id": id}),
        );
        request.correlation_id = Some(task.request.id);
        self.bus.publish(request).map_err(CancelError::Publish)
    }
    pub fn cancel_all(&self) -> Vec<(Uuid, Result<usize, CancelError>)> {
        let ids: Vec<Uuid> = self
            .tasks
            .lock()
            .expect("tasks lock")
            .values()
            .filter(|task| !task.status.is_terminal())
            .map(|task| task.id)
            .collect();
        ids.into_iter().map(|id| (id, self.cancel(id))).collect()
    }
    pub fn task(&self, id: Uuid) -> Option<TaskRecord> {
        self.tasks.lock().expect("tasks lock").get(&id).cloned()
    }
    pub fn checkpoint(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(
            self.tasks
                .lock()
                .expect("tasks lock")
                .values()
                .cloned()
                .collect::<Vec<_>>(),
        )
    }
    pub fn restore_checkpoint(&self, value: Value) -> Result<(), serde_json::Error> {
        let records: Vec<TaskRecord> = serde_json::from_value(value)?;
        let mut ids = std::collections::HashSet::new();
        if records.iter().any(|record| {
            record.request.run_id != self.run_id || record.attempts == 0 || !ids.insert(record.id)
        }) {
            return Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid supervisor checkpoint",
            )));
        }
        let mut tasks = self.tasks.lock().expect("tasks lock");
        tasks.clear();
        for record in records {
            tasks.insert(record.id, record);
        }
        Ok(())
    }
    pub fn status(&self, id: Uuid) -> Option<TaskStatus> {
        self.task(id).map(|t| t.status)
    }
    pub fn mark_completed(&self, id: Uuid) {
        self.mark_local(id, event_kind::TASK_COMPLETED);
    }
    pub fn mark_failed(&self, id: Uuid) {
        self.mark_local(id, event_kind::TASK_FAILED);
    }
    pub fn mark_cancelled(&self, id: Uuid) {
        self.mark_local(id, event_kind::TASK_CANCELLED);
    }
    fn mark_local(&self, id: Uuid, kind: &str) {
        if let Some(task) = self.task(id) {
            self.mark_from_event(&lifecycle_event(
                &task.request,
                &task.worker.id,
                kind,
                Value::Null,
            ));
        }
    }
    /// Explicit retry, bounded by total attempts including the initial dispatch.
    /// A new request ID distinguishes late events from previous executions.
    pub fn retry(&self, id: Uuid, max_attempts: u32) -> Result<Uuid, RetryError> {
        let mut tasks = self.tasks.lock().expect("tasks lock");
        let task = tasks.get_mut(&id).ok_or(RetryError::UnknownTask)?;
        if task.status != TaskStatus::Failed {
            return Err(RetryError::NotFailed);
        }
        if task.attempts >= max_attempts {
            return Err(RetryError::Exhausted);
        }
        let mut request = task.request.clone();
        request.id = Uuid::new_v4();
        self.bus.publish(request.clone())?;
        task.request = request;
        task.attempts += 1;
        task.status = TaskStatus::Pending;
        task.result = None;
        Ok(task.request.id)
    }

    /// Durable retry counterpart. The record changes only after SQLite append
    /// succeeds, preserving replay safety across process failure.
    pub fn retry_durable(
        &self,
        durable: &SqliteBusHandle,
        id: Uuid,
        max_attempts: u32,
    ) -> Result<Uuid, RetryError> {
        let mut tasks = self.tasks.lock().expect("tasks lock");
        let record = tasks.get(&id).cloned().ok_or(RetryError::UnknownTask)?;
        if record.status != TaskStatus::Failed {
            return Err(RetryError::NotFailed);
        }
        if record.attempts >= max_attempts {
            return Err(RetryError::Exhausted);
        }
        let mut request = Message::new(
            self.run_id,
            self.worker_topic(&record.worker),
            "supervisor",
            "task.request",
            record.request.payload.clone(),
        );
        request.correlation_id = Some(id);
        durable
            .publish(&request)
            .map_err(|_| RetryError::DurablePublish)?;
        let mut next = record;
        next.request = request.clone();
        next.attempts += 1;
        next.status = TaskStatus::Pending;
        next.result = None;
        tasks.insert(id, next);
        Ok(request.id)
    }

    pub async fn retry_durable_with_policy(
        &self,
        durable: &SqliteBusHandle,
        id: Uuid,
        policy: RetryPolicy,
    ) -> Result<Uuid, RetryError> {
        let attempt = self.task(id).map(|t| t.attempts).unwrap_or(0);
        tokio::time::sleep(policy.delay_for(attempt)).await;
        self.retry_durable(durable, id, policy.max_attempts)
    }
    pub async fn retry_with_policy(
        &self,
        id: Uuid,
        policy: RetryPolicy,
    ) -> Result<Uuid, RetryError> {
        let attempt = self
            .task(id)
            .map(|task| task.attempts)
            .ok_or(RetryError::UnknownTask)?;
        if attempt >= policy.max_attempts {
            return Err(RetryError::Exhausted);
        }
        tokio::time::sleep(policy.delay_for(attempt)).await;
        self.retry(id, policy.max_attempts)
    }
    /// Accept only the assigned worker's current attempt, in this run and result topic.
    /// First terminal event wins. Duplicates and stale attempts cannot overwrite it.
    pub fn mark_from_event(&self, event: &Message) -> bool {
        if event.run_id != self.run_id || event.topic != self.result_topic() {
            return false;
        }
        let Some(correlation) = event.correlation_id else {
            return false;
        };
        let mut tasks = self.tasks.lock().expect("tasks lock");
        let Some(task) = tasks.values_mut().find(|t| t.request.id == correlation) else {
            return false;
        };
        if event.sender != task.worker.id || task.status.is_terminal() {
            return false;
        }
        let status = match event.kind.as_str() {
            event_kind::TASK_STARTED | event_kind::TASK_PROGRESS => TaskStatus::Running,
            event_kind::TASK_COMPLETED => TaskStatus::Completed,
            event_kind::TASK_FAILED => TaskStatus::Failed,
            event_kind::TASK_CANCELLED => TaskStatus::Cancelled,
            _ => return false,
        };
        task.status = status;
        if status.is_terminal() {
            task.result = Some(event.clone());
        }
        true
    }

    /// Consumes this supervisor's result topic. Rejected and stale events are
    /// acknowledged for this consumer so they cannot block later results;
    /// other consumers retain independent offsets.
    pub async fn consume_durable_results(
        &self,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> rusqlite::Result<()> {
        let mut sub = bus.subscribe(consumer, self.result_topic());
        loop {
            let (seq, event) = tokio::select! { _ = cancel.cancelled() => return Ok(()), result = sub.recv_wait() => result? };
            self.mark_from_event(&event);
            // Also retry release after a previous publication failure: the
            // terminal event may already have updated the in-memory record.
            self.release_ready_durable(&bus)?;
            sub.ack(seq).await?;
        }
    }

    pub async fn consume_durable_results_timeout(
        &self,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
        duration: std::time::Duration,
    ) -> rusqlite::Result<()> {
        let cancel = tokio_util::sync::CancellationToken::new();
        tokio::select! {
            result = self.consume_durable_results(bus, consumer, cancel.clone()) => result,
            _ = tokio::time::sleep(duration) => { cancel.cancel(); Ok(()) }
        }
    }

    /// Processes at most one durable result, useful for externally driven
    /// supervisors and deterministic tests.
    pub async fn consume_durable_once(
        &self,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
    ) -> rusqlite::Result<bool> {
        let consumer = consumer.into();
        let rows = bus
            .replay_async(consumer.clone(), self.result_topic(), 1)
            .await?;
        let Some((seq, event)) = rows.into_iter().next() else {
            return Ok(false);
        };
        self.mark_from_event(&event);
        self.release_ready_durable(&bus)?;
        bus.ack_async(consumer, seq).await?;
        Ok(true)
    }

    /// Durable consume variant with checkpoint-before-ack ordering. A crash
    /// between these operations replays an idempotent event rather than losing
    /// the supervisor state transition.
    pub async fn consume_durable_once_checkpointed(
        &self,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
    ) -> rusqlite::Result<bool> {
        let consumer = consumer.into();
        let rows = bus
            .replay_async(consumer.clone(), self.result_topic(), 1)
            .await?;
        let Some((seq, event)) = rows.into_iter().next() else {
            return Ok(false);
        };
        self.mark_from_event(&event);
        self.release_ready_durable(&bus)?;
        let tasks = self
            .tasks
            .lock()
            .expect("tasks lock")
            .values()
            .cloned()
            .collect();
        bus.commit_supervisor_result_async(
            self.run_id,
            tasks,
            consumer,
            self.result_topic(),
            seq,
            self.checkpoint_message()
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
        )
        .await?;
        Ok(true)
    }

    pub fn commit_event_with_store<S: SupervisorStore>(
        &self,
        store: &S,
        consumer: &str,
        topic: &Topic,
        sequence: i64,
        event: &Message,
    ) -> Result<(), S::Error> {
        self.mark_from_event(event);
        let tasks = self
            .tasks
            .lock()
            .expect("tasks lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let checkpoint = self
            .checkpoint_message()
            .expect("checkpoint serialization is infallible");
        store.commit_result(self.run_id, &tasks, consumer, topic, sequence, &checkpoint)
    }

    pub async fn consume_durable_once_with_store<S: SupervisorStore<Error = rusqlite::Error>>(
        &self,
        bus: SqliteBusHandle,
        store: &S,
        consumer: impl Into<String>,
    ) -> rusqlite::Result<bool> {
        let consumer = consumer.into();
        let rows = bus
            .replay_async(consumer.clone(), self.result_topic(), 1)
            .await?;
        let Some((sequence, event)) = rows.into_iter().next() else {
            return Ok(false);
        };
        self.commit_event_with_store(store, &consumer, &self.result_topic(), sequence, &event)?;
        Ok(true)
    }
    pub fn result_topic(&self) -> Topic {
        format!("run.{}.result", self.run_id).into()
    }
    pub fn worker_topic(&self, worker: &AgentDescriptor) -> Topic {
        format!("run.{}.task.{}", self.run_id, worker.id).into()
    }
    pub async fn consume_result(
        &self,
        subscription: &mut Subscription,
    ) -> Result<Message, RecvError> {
        let event = subscription.recv().await?;
        self.mark_from_event(&event);
        Ok(event)
    }
    /// Collect terminal results in input order, including results consumed while waiting
    /// for another task. Deadline applies to the entire wait, not each event.
    pub async fn wait_for_tasks(
        &self,
        subscription: &mut Subscription,
        ids: &[Uuid],
        timeout: std::time::Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Vec<Message>, WaitError> {
        if ids.iter().any(|id| self.task(*id).is_none()) {
            return Err(WaitError::UnknownTask);
        }
        let collect = async {
            loop {
                let results: Option<Vec<Message>> = ids
                    .iter()
                    .map(|id| self.task(*id).and_then(|t| t.result))
                    .collect();
                if let Some(results) = results {
                    return Ok(results);
                }
                self.consume_result(subscription).await?;
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(WaitError::Cancelled),
            result = tokio::time::timeout(timeout, collect) => result.map_err(|_| WaitError::Timeout)?,
        }
    }
    pub async fn wait_for_task(
        &self,
        subscription: &mut Subscription,
        task_id: Uuid,
    ) -> Result<Message, RecvError> {
        loop {
            if let Some(result) = self.task(task_id).and_then(|task| task.result) {
                return Ok(result);
            }
            self.consume_result(subscription).await?;
        }
    }
    pub async fn wait_for_task_timeout(
        &self,
        subscription: &mut Subscription,
        task_id: Uuid,
        timeout: std::time::Duration,
    ) -> Result<Message, WaitError> {
        if self.task(task_id).is_none() {
            return Err(WaitError::UnknownTask);
        }
        tokio::time::timeout(timeout, self.wait_for_task(subscription, task_id))
            .await
            .map_err(|_| WaitError::Timeout)?
            .map_err(WaitError::Receive)
    }
}
pub struct SupervisorPattern;
impl Pattern for SupervisorPattern {
    fn name(&self) -> &'static str {
        "supervisor"
    }
}
pub struct DiscussionPattern;
impl Pattern for DiscussionPattern {
    fn name(&self) -> &'static str {
        "discussion"
    }
}

/// Pluggable reducer/moderator for a completed discussion round.
pub trait DiscussionModerator: Send + Sync {
    fn reduce(&self, run_id: Uuid, round: u32, turns: Vec<Message>) -> Result<Value, String>;
}

pub struct DiscussionSession {
    pub run_id: Uuid,
    pub round: u32,
    pub max_rounds: u32,
    pub participants: Vec<String>,
    pub finished: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscussionFailurePolicy {
    Abort,
    Continue,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscussionCheckpoint {
    pub run_id: Uuid,
    pub round: u32,
    pub max_rounds: u32,
    pub participants: Vec<String>,
    pub finished: bool,
}
impl DiscussionSession {
    pub async fn restore_checkpoint_durable(
        bus: &crate::sqlite_bus::SqliteBusHandle,
        run_id: Uuid,
        consumer: impl Into<String>,
    ) -> Result<Self, DiscussionError> {
        let topic = format!("run.{run_id}.discussion.checkpoint").into();
        let rows = bus
            .replay_async(consumer.into(), topic, u32::MAX)
            .await
            .map_err(|_| DiscussionError::InvalidCheckpoint)?;
        let message = rows.last().ok_or(DiscussionError::InvalidCheckpoint)?;
        if message.1.run_id != run_id || message.1.kind != "discussion.checkpoint" {
            return Err(DiscussionError::InvalidCheckpoint);
        }
        serde_json::from_value(message.1.payload.clone())
            .map_err(|_| DiscussionError::InvalidCheckpoint)
            .and_then(Self::restore)
    }
    /// Stable topic for persisted session checkpoints.
    pub fn checkpoint_topic(&self) -> Topic {
        format!("run.{}.discussion.checkpoint", self.run_id).into()
    }
    /// Builds an idempotent checkpoint event for durable storage.
    pub fn checkpoint_message(&self) -> Message {
        let payload = self.checkpoint_json().unwrap_or(Value::Null);
        let mut message = Message::new(
            self.run_id,
            self.checkpoint_topic(),
            "discussion",
            "discussion.checkpoint",
            payload.clone(),
        );
        message.id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("discussion-checkpoint:{}:{}", self.run_id, payload).as_bytes(),
        );
        message
    }
    pub async fn persist_checkpoint(
        &self,
        bus: &crate::sqlite_bus::SqliteBusHandle,
    ) -> rusqlite::Result<i64> {
        bus.publish_async(self.checkpoint_message()).await
    }
    pub fn new(run_id: Uuid, max_rounds: u32) -> Self {
        assert!(max_rounds > 0, "discussion requires at least one round");
        Self {
            run_id,
            round: 0,
            max_rounds,
            participants: Vec::new(),
            finished: false,
        }
    }
    pub fn register(&mut self, id: impl Into<String>) -> bool {
        let id = id.into();
        if id.is_empty() || self.participants.contains(&id) {
            return false;
        }
        {
            self.participants.push(id);
        }
        true
    }
    pub fn checkpoint(&self) -> DiscussionCheckpoint {
        DiscussionCheckpoint {
            run_id: self.run_id,
            round: self.round,
            max_rounds: self.max_rounds,
            participants: self.participants.clone(),
            finished: self.finished,
        }
    }
    pub fn checkpoint_json(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self.checkpoint())
    }
    pub fn restore_json(value: Value) -> Result<Self, DiscussionError> {
        serde_json::from_value(value)
            .map_err(|_| DiscussionError::InvalidCheckpoint)
            .and_then(Self::restore)
    }
    pub fn restore(checkpoint: DiscussionCheckpoint) -> Result<Self, DiscussionError> {
        if checkpoint.max_rounds == 0
            || checkpoint.round >= checkpoint.max_rounds
            || checkpoint.participants.iter().any(|p| p.is_empty())
            || checkpoint.participants.windows(2).any(|w| w[0] == w[1])
        {
            return Err(DiscussionError::InvalidCheckpoint);
        }
        Ok(Self {
            run_id: checkpoint.run_id,
            round: checkpoint.round,
            max_rounds: checkpoint.max_rounds,
            participants: checkpoint.participants,
            finished: checkpoint.finished,
        })
    }
    pub fn publish_turn(
        &self,
        bus: &InMemoryBus,
        speaker: &str,
        payload: Value,
    ) -> Result<usize, PublishError> {
        bus.publish(discussion_message(
            self.run_id,
            self.round,
            speaker,
            payload,
        ))
    }
    pub fn publish_turn_checked(
        &self,
        bus: &InMemoryBus,
        speaker: &str,
        payload: Value,
    ) -> Result<usize, DiscussionError> {
        if self.finished {
            return Err(DiscussionError::Finished);
        }
        if !self.participants.iter().any(|id| id == speaker) {
            return Err(DiscussionError::InvalidTurn);
        }
        self.publish_turn(bus, speaker, payload)
            .map_err(DiscussionError::Publish)
    }
    /// Publish one turn for each registered participant in deterministic order.
    pub fn publish_round(
        &self,
        bus: &InMemoryBus,
        payloads: &std::collections::HashMap<String, Value>,
    ) -> Result<usize, PublishError> {
        let mut delivered = 0;
        for speaker in &self.participants {
            if let Some(payload) = payloads.get(speaker) {
                delivered += bus.publish(discussion_message(
                    self.run_id,
                    self.round,
                    speaker,
                    payload.clone(),
                ))?;
            }
        }
        Ok(delivered)
    }
    pub fn publish_round_checked(
        &self,
        bus: &InMemoryBus,
        payloads: &std::collections::HashMap<String, Value>,
    ) -> Result<usize, DiscussionError> {
        if self.finished {
            return Err(DiscussionError::Finished);
        }
        if payloads
            .keys()
            .any(|speaker| !self.participants.iter().any(|id| id == speaker))
        {
            return Err(DiscussionError::InvalidTurn);
        }
        self.publish_round(bus, payloads)
            .map_err(DiscussionError::Publish)
    }
    pub fn completion_message(&self, moderator: &str) -> Message {
        discussion_message(
            self.run_id,
            self.round,
            moderator,
            serde_json::json!({"type":"round.completed","round":self.round}),
        )
    }
    pub fn finish(&mut self) {
        self.finished = true;
    }
    pub fn handle_participant_failure(
        &mut self,
        participant: &str,
        policy: DiscussionFailurePolicy,
    ) -> Result<(), DiscussionError> {
        if !self.participants.iter().any(|id| id == participant) {
            return Err(DiscussionError::InvalidTurn);
        }
        match policy {
            DiscussionFailurePolicy::Abort => self.finished = true,
            DiscussionFailurePolicy::Continue => {
                self.participants.retain(|id| id != participant);
                if self.participants.is_empty() {
                    self.finished = true;
                }
            }
        }
        Ok(())
    }
    pub fn reduce_round<M: DiscussionModerator>(
        &self,
        moderator: &M,
        turns: Vec<Message>,
        bus: &InMemoryBus,
    ) -> Result<Message, DiscussionError> {
        if self.finished {
            return Err(DiscussionError::Finished);
        }
        let mut speakers = std::collections::HashSet::new();
        for turn in &turns {
            if turn.run_id != self.run_id
                || turn.topic != discussion_topic(self.run_id, self.round)
                || !self.participants.iter().any(|id| id == &turn.sender)
            {
                return Err(DiscussionError::InvalidTurn);
            }
            if !speakers.insert(turn.sender.clone()) {
                return Err(DiscussionError::DuplicateSpeaker);
            }
        }
        if speakers.len() != self.participants.len() {
            return Err(DiscussionError::MissingSpeaker);
        }
        let decision = moderator
            .reduce(self.run_id, self.round, turns)
            .map_err(DiscussionError::Moderator)?;
        let mut event = self.completion_message("moderator");
        event.payload =
            serde_json::json!({"type":"round.completed","round":self.round,"decision":decision});
        bus.publish(event.clone())
            .map_err(DiscussionError::Publish)?;
        Ok(event)
    }

    /// Collects one complete round from the in-memory topic and reduces it.
    pub async fn collect_round<M: DiscussionModerator>(
        &self,
        moderator: &M,
        bus: &InMemoryBus,
        timeout: std::time::Duration,
    ) -> Result<Message, DiscussionError> {
        if self.finished {
            return Err(DiscussionError::Finished);
        }
        let mut sub = bus.subscribe(discussion_topic(self.run_id, self.round));
        let mut turns = Vec::with_capacity(self.participants.len());
        let collect = async {
            while turns.len() < self.participants.len() {
                let message = sub.recv().await.map_err(|_| DiscussionError::InvalidTurn)?;
                if message.run_id != self.run_id
                    || message.topic != discussion_topic(self.run_id, self.round)
                {
                    continue;
                }
                turns.push(message);
            }
            Ok::<_, DiscussionError>(())
        };
        tokio::time::timeout(timeout, collect)
            .await
            .map_err(|_| DiscussionError::MissingSpeaker)??;
        self.reduce_round(moderator, turns, bus)
    }

    /// Durable counterpart that acknowledges the round only after reduction and
    /// decision publication succeed.
    pub async fn collect_round_durable<M: DiscussionModerator>(
        &self,
        moderator: &M,
        bus: SqliteBusHandle,
        consumer: impl Into<String>,
        timeout: std::time::Duration,
    ) -> Result<Message, Box<dyn std::error::Error + Send + Sync>> {
        if self.finished {
            return Err(DiscussionError::Finished.into());
        }
        if self.participants.is_empty() {
            return Err(DiscussionError::MissingSpeaker.into());
        }
        let consumer = consumer.into();
        let topic = discussion_topic(self.run_id, self.round);
        let limit = u32::try_from(self.participants.len())?;
        let rows = tokio::time::timeout(timeout, async {
            loop {
                let rows = bus
                    .replay_async(consumer.clone(), topic.clone(), limit)
                    .await?;
                let mut speakers = std::collections::HashSet::new();
                for (_, message) in &rows {
                    if message.run_id != self.run_id
                        || message.kind != "discussion.turn"
                        || !self.participants.contains(&message.sender)
                    {
                        return Err::<_, Box<dyn std::error::Error + Send + Sync>>(
                            DiscussionError::InvalidTurn.into(),
                        );
                    }
                    if !speakers.insert(&message.sender) {
                        return Err(DiscussionError::DuplicateSpeaker.into());
                    }
                }
                if rows.len() == self.participants.len() {
                    return Ok(rows);
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| DiscussionError::MissingSpeaker)??;
        let turns = rows.iter().map(|(_, m)| m.clone()).collect();
        let decision = moderator
            .reduce(self.run_id, self.round, turns)
            .map_err(DiscussionError::Moderator)?;
        let mut event = self.completion_message("moderator");
        event.payload =
            serde_json::json!({"type":"round.completed","round":self.round,"decision":decision});
        let checkpoint = self.checkpoint_message();
        bus.commit_messages_async(
            consumer,
            topic,
            rows.iter().map(|(seq, _)| *seq).collect(),
            vec![event.clone(), checkpoint],
        )
        .await?;
        Ok(event)
    }
    pub fn advance(&mut self) -> bool {
        if self.finished || self.round + 1 >= self.max_rounds {
            self.finished = true;
            false
        } else {
            self.round += 1;
            true
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DiscussionError {
    #[error("discussion is already finished")]
    Finished,
    #[error("turn does not belong to this discussion round or participant")]
    InvalidTurn,
    #[error("participant spoke more than once in this round")]
    DuplicateSpeaker,
    #[error("not all registered participants spoke in this round")]
    MissingSpeaker,
    #[error("invalid discussion checkpoint")]
    InvalidCheckpoint,
    #[error("moderator failed: {0}")]
    Moderator(String),
    #[error(transparent)]
    Publish(#[from] PublishError),
}

/// Deterministic round-robin topic used by discussion-style patterns.
pub fn discussion_topic(run_id: Uuid, round: u32) -> Topic {
    format!("run.{run_id}.discussion.round.{round}").into()
}

pub fn discussion_message(run_id: Uuid, round: u32, speaker: &str, payload: Value) -> Message {
    Message::new(
        run_id,
        discussion_topic(run_id, round),
        speaker,
        "discussion.turn",
        payload,
    )
}

/// Conventional lifecycle event names shared by all patterns.
pub mod event_kind {
    pub const TASK_STARTED: &str = "task.started";
    pub const TASK_PROGRESS: &str = "task.progress";
    pub const TASK_COMPLETED: &str = "task.completed";
    pub const TASK_FAILED: &str = "task.failed";
    pub const TASK_CANCELLED: &str = "task.cancelled";
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: Uuid,
    pub request: Message,
    pub worker: AgentDescriptor,
    pub status: TaskStatus,
    pub attempts: u32,
    pub result: Option<Message>,
    #[serde(default)]
    pub prerequisites: Vec<Uuid>,
}
#[derive(Debug, thiserror::Error)]
#[error("batch dispatch failed after {} tasks: {source}", dispatched.len())]
pub struct BatchDispatchError {
    pub dispatched: Vec<Uuid>,
    #[source]
    pub source: PublishError,
}
#[derive(Debug, thiserror::Error)]
pub enum RetryError {
    #[error("unknown task")]
    UnknownTask,
    #[error("only failed tasks can be retried")]
    NotFailed,
    #[error("attempt limit reached")]
    Exhausted,
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error("durable publish failed")]
    DurablePublish,
}

#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("unknown task")]
    UnknownTask,
    #[error("task is already terminal")]
    AlreadyTerminal,
    #[error(transparent)]
    Publish(#[from] PublishError),
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("prerequisites are not completed")]
    PrerequisitesPending,
    #[error("worker is not registered")]
    UnknownWorker,
    #[error(transparent)]
    Publish(#[from] PublishError),
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
}
impl RetryPolicy {
    pub fn delay_for(&self, attempt: u32) -> std::time::Duration {
        let shift = attempt.saturating_sub(1).min(31);
        let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        self.initial_delay
            .saturating_mul(factor)
            .min(self.max_delay)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("unknown task")]
    UnknownTask,
    #[error("waiting timed out")]
    Timeout,
    #[error("waiting cancelled; workers are not cancelled by this operation")]
    Cancelled,
    #[error(transparent)]
    Receive(#[from] RecvError),
}

/// Builds a reply preserving causal correlation with the request.
pub fn reply(
    request: &Message,
    topic: impl Into<Topic>,
    sender: impl Into<String>,
    kind: impl Into<String>,
    payload: Value,
) -> Message {
    let mut message = Message::new(request.run_id, topic, sender, kind, payload);
    message.correlation_id = Some(request.id);
    message
}

pub fn lifecycle_event(request: &Message, sender: &str, kind: &str, payload: Value) -> Message {
    reply(
        request,
        format!("run.{}.result", request.run_id),
        sender,
        kind,
        payload,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_supervisor_store_contract_round_trip() {
        let store = InMemorySupervisorStore::default();
        let run = Uuid::new_v4();
        let tasks = Vec::new();
        store.save_tasks(run, &tasks).unwrap();
        assert!(store.load_tasks(run).unwrap().is_empty());
    }

    #[test]
    fn supervisor_commits_event_through_store_contract() {
        let supervisor = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(2));
        let store = InMemorySupervisorStore::default();
        let topic: Topic = "result".into();
        let event = Message::new(
            supervisor.run_id,
            topic.clone(),
            "worker",
            event_kind::TASK_COMPLETED,
            Value::Null,
        );
        supervisor
            .commit_event_with_store(&store, "consumer", &topic, 1, &event)
            .unwrap();
        assert!(store.load_tasks(supervisor.run_id).unwrap().is_empty());
    }
    use serde_json::json;

    #[tokio::test]
    async fn broadcasts_only_matching_topic() {
        let bus = InMemoryBus::new(8);
        let mut sub = bus.subscribe("a");
        let _other = bus.subscribe("b");
        let run = Uuid::new_v4();
        bus.publish(Message::new(run, "b", "x", "ignored", json!({})))
            .unwrap();
        bus.publish(Message::new(run, "a", "x", "ok", json!({"v": 1})))
            .unwrap();
        let m = sub.recv().await.unwrap();
        assert_eq!(m.topic, Topic("a".into()));
        assert_eq!(m.kind, "ok");
        assert_eq!(m.payload["v"], 1);
    }

    #[tokio::test]
    async fn every_subscriber_gets_a_copy() {
        let bus = InMemoryBus::new(4);
        let mut a = bus.subscribe("x");
        let mut b = bus.subscribe("x");
        let msg = Message::new(Uuid::new_v4(), "x", "s", "k", json!(null));
        let id = msg.id;
        assert_eq!(bus.publish(msg).unwrap(), 2);
        assert_eq!(a.recv().await.unwrap().id, id);
        assert_eq!(b.recv().await.unwrap().id, id);
    }

    #[tokio::test]
    async fn reports_lag_instead_of_silent_loss() {
        let bus = InMemoryBus::new(1);
        let mut sub = bus.subscribe("x");
        for i in 0..3 {
            bus.publish(Message::new(
                Uuid::new_v4(),
                "x",
                "s",
                i.to_string(),
                json!(i),
            ))
            .unwrap();
        }
        assert!(matches!(sub.recv().await, Err(RecvError::Lagged(_))));
    }

    #[test]
    fn publish_without_subscribers_is_explicit_and_does_not_leave_pending() {
        let run = Uuid::new_v4();
        let supervisor = Supervisor::new(run, InMemoryBus::new(4));
        let worker = AgentDescriptor {
            id: "w".into(),
            role: "worker".into(),
        };
        let err = supervisor.dispatch(&worker, json!({})).unwrap_err();
        assert!(matches!(err, PublishError::NoSubscribers));
        assert!(supervisor.tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn preserves_fifo_order_for_each_subscription() {
        let bus = InMemoryBus::new(16);
        let mut sub = bus.subscribe("ordered");
        for i in 0..8u32 {
            bus.publish(Message::new(
                Uuid::new_v4(),
                "ordered",
                "p",
                "item",
                json!(i),
            ))
            .unwrap();
        }
        for i in 0..8u32 {
            assert_eq!(sub.recv().await.unwrap().payload, json!(i));
        }
    }

    #[tokio::test]
    async fn dropping_one_subscriber_does_not_affect_others() {
        let bus = InMemoryBus::new(4);
        let sub = bus.subscribe("x");
        let mut live = bus.subscribe("x");
        drop(sub);
        assert_eq!(
            bus.publish(Message::new(Uuid::new_v4(), "x", "p", "k", json!(1)))
                .unwrap(),
            1
        );
        assert_eq!(live.recv().await.unwrap().payload, json!(1));
    }

    #[test]
    fn subscriber_count_tracks_topic_lifecycle() {
        let bus = InMemoryBus::new(2);
        assert_eq!(bus.subscriber_count("x"), 0);
        let first = bus.subscribe("x");
        let second = bus.subscribe("x");
        assert_eq!(bus.subscriber_count("x"), 2);
        drop(first);
        assert_eq!(bus.subscriber_count("x"), 1);
        drop(second);
        assert_eq!(bus.subscriber_count("x"), 0);
    }

    #[test]
    fn supervisor_contract_is_stable() {
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(2);
        let mut supervisor = Supervisor::new(run, bus.clone());
        let worker = AgentDescriptor {
            id: "coder".into(),
            role: "implementation".into(),
        };
        supervisor.add_worker(worker.clone());
        let mut sub = bus.subscribe(format!("run.{run}.task.coder"));
        supervisor
            .dispatch(&worker, json!({"task":"build"}))
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let msg = rt.block_on(sub.recv()).unwrap();
        assert_eq!(msg.sender, "supervisor");
        assert_eq!(msg.kind, "task.request");
    }

    #[test]
    fn retry_policy_is_exponential_and_capped() {
        let policy = RetryPolicy {
            max_attempts: 4,
            initial_delay: std::time::Duration::from_millis(10),
            max_delay: std::time::Duration::from_millis(25),
        };
        assert_eq!(policy.delay_for(1), std::time::Duration::from_millis(10));
        assert_eq!(policy.delay_for(2), std::time::Duration::from_millis(20));
        assert_eq!(policy.delay_for(3), std::time::Duration::from_millis(25));
        assert_eq!(policy.delay_for(20), std::time::Duration::from_millis(25));
    }

    #[test]
    fn supervisor_updates_status_from_correlated_events() {
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(4);
        let supervisor = Supervisor::new(run, bus);
        let worker = AgentDescriptor {
            id: "w".into(),
            role: "worker".into(),
        };
        let mut sub = supervisor.bus.subscribe(format!("run.{run}.task.w"));
        supervisor.dispatch(&worker, json!({})).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request = rt.block_on(sub.recv()).unwrap();
        assert_eq!(supervisor.status(request.id), Some(TaskStatus::Pending));
        let event = reply(
            &request,
            supervisor.result_topic(),
            "w",
            event_kind::TASK_COMPLETED,
            json!({}),
        );
        supervisor.mark_from_event(&event);
        assert_eq!(supervisor.status(request.id), Some(TaskStatus::Completed));
    }

    #[test]
    fn discussion_messages_are_round_scoped() {
        let run = Uuid::new_v4();
        let first = discussion_message(run, 1, "critic", json!({"vote": "yes"}));
        assert_eq!(first.topic, discussion_topic(run, 1));
        assert_ne!(first.topic, discussion_topic(run, 2));
        assert_eq!(first.kind, "discussion.turn");
        assert_eq!(first.sender, "critic");
    }

    #[tokio::test]
    async fn discussion_session_registers_publishes_and_stops_at_limit() {
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(4);
        let mut sub = bus.subscribe(discussion_topic(run, 0));
        let mut session = DiscussionSession::new(run, 2);
        session.register("a");
        session.register("a");
        session.register("b");
        assert_eq!(session.participants, vec!["a", "b"]);
        session
            .publish_turn(&bus, "a", json!({"text":"hi"}))
            .unwrap();
        assert_eq!(sub.recv().await.unwrap().sender, "a");
        assert!(session.advance());
        assert!(!session.advance());
        assert!(session.finished);
    }

    #[tokio::test]
    async fn discussion_round_publishes_registered_participants_in_order() {
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(8);
        let mut session = DiscussionSession::new(run, 3);
        session.register("a");
        session.register("b");
        let mut sub = bus.subscribe(discussion_topic(run, 0));
        let payloads = [("b".to_string(), json!(2)), ("a".to_string(), json!(1))]
            .into_iter()
            .collect();
        assert_eq!(session.publish_round(&bus, &payloads).unwrap(), 2);
        assert_eq!(sub.recv().await.unwrap().sender, "a");
        assert_eq!(sub.recv().await.unwrap().sender, "b");
        assert_eq!(
            session.completion_message("moderator").payload["type"],
            "round.completed"
        );
    }

    struct CountModerator;
    impl DiscussionModerator for CountModerator {
        fn reduce(&self, _: Uuid, _: u32, turns: Vec<Message>) -> Result<Value, String> {
            Ok(json!({"count": turns.len()}))
        }
    }
    #[tokio::test]
    async fn discussion_reducer_publishes_decision() {
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(4);
        let mut session = DiscussionSession::new(run, 2);
        session.register("a");
        let mut sub = bus.subscribe(discussion_topic(run, 0));
        let turn = discussion_message(run, 0, "a", json!(1));
        let event = session
            .reduce_round(&CountModerator, vec![turn], &bus)
            .unwrap();
        assert_eq!(event.payload["decision"]["count"], 1);
        assert_eq!(sub.recv().await.unwrap().payload["type"], "round.completed");
    }

    #[test]
    fn discussion_reducer_rejects_incomplete_round() {
        struct M;
        impl DiscussionModerator for M {
            fn reduce(&self, _: Uuid, _: u32, _: Vec<Message>) -> Result<Value, String> {
                unreachable!()
            }
        }
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(2);
        let mut session = DiscussionSession::new(run, 1);
        session.register("a");
        session.register("b");
        let turn = discussion_message(run, 0, "a", json!(1));
        assert!(matches!(
            session.reduce_round(&M, vec![turn], &bus),
            Err(DiscussionError::MissingSpeaker)
        ));
    }

    #[test]
    fn discussion_reducer_rejects_duplicate_speaker() {
        struct M;
        impl DiscussionModerator for M {
            fn reduce(&self, _: Uuid, _: u32, _: Vec<Message>) -> Result<Value, String> {
                unreachable!()
            }
        }
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(2);
        let mut session = DiscussionSession::new(run, 1);
        session.register("a");
        let turns = vec![
            discussion_message(run, 0, "a", json!(1)),
            discussion_message(run, 0, "a", json!(2)),
        ];
        assert!(matches!(
            session.reduce_round(&M, turns, &bus),
            Err(DiscussionError::DuplicateSpeaker)
        ));
    }

    #[tokio::test]
    async fn discussion_moderator_failure_does_not_emit_completion() {
        struct F;
        impl DiscussionModerator for F {
            fn reduce(&self, _: Uuid, _: u32, _: Vec<Message>) -> Result<Value, String> {
                Err("no decision".into())
            }
        }
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(2);
        let mut session = DiscussionSession::new(run, 1);
        session.register("a");
        let turn = discussion_message(run, 0, "a", json!(1));
        assert!(matches!(
            session.reduce_round(&F, vec![turn], &bus),
            Err(DiscussionError::Moderator(_))
        ));
        let mut sub = bus.subscribe(discussion_topic(run, 0));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), sub.recv())
                .await
                .is_err()
        );
    }

    #[test]
    fn discussion_checkpoint_round_trips_and_rejects_invalid_state() {
        let run = Uuid::new_v4();
        let mut session = DiscussionSession::new(run, 3);
        session.register("a");
        assert!(session.advance());
        let checkpoint = session.checkpoint();
        let restored = DiscussionSession::restore(checkpoint.clone()).unwrap();
        assert_eq!(restored.checkpoint(), checkpoint);
        assert_eq!(
            DiscussionSession::restore_json(session.checkpoint_json().unwrap())
                .unwrap()
                .checkpoint(),
            checkpoint
        );
        let mut invalid = checkpoint;
        invalid.round = invalid.max_rounds;
        assert!(matches!(
            DiscussionSession::restore(invalid),
            Err(DiscussionError::InvalidCheckpoint)
        ));
    }

    #[test]
    fn discussion_registration_rejects_empty_and_duplicates() {
        let mut session = DiscussionSession::new(Uuid::new_v4(), 1);
        assert!(!session.register(""));
        assert!(session.register("a"));
        assert!(!session.register("a"));
        assert_eq!(session.participants, vec!["a"]);
    }

    #[test]
    fn discussion_failure_policy_aborts_or_removes_participant() {
        let run = Uuid::new_v4();
        let mut abort = DiscussionSession::new(run, 1);
        abort.register("a");
        abort
            .handle_participant_failure("a", DiscussionFailurePolicy::Abort)
            .unwrap();
        assert!(abort.finished);
        let mut continue_session = DiscussionSession::new(run, 1);
        continue_session.register("a");
        continue_session.register("b");
        continue_session
            .handle_participant_failure("a", DiscussionFailurePolicy::Continue)
            .unwrap();
        assert_eq!(continue_session.participants, vec!["b"]);
        assert!(!continue_session.finished);
    }

    #[test]
    fn discussion_finish_is_checkpointed() {
        let mut session = DiscussionSession::new(Uuid::new_v4(), 2);
        assert!(!session.checkpoint().finished);
        session.finish();
        assert!(session.checkpoint().finished);
    }

    #[test]
    fn checked_discussion_turn_rejects_unknown_or_finished_speaker() {
        let run = Uuid::new_v4();
        let bus = InMemoryBus::new(2);
        let mut session = DiscussionSession::new(run, 1);
        session.register("a");
        assert!(matches!(
            session.publish_turn_checked(&bus, "b", json!(1)),
            Err(DiscussionError::InvalidTurn)
        ));
        session.finish();
        assert!(matches!(
            session.publish_turn_checked(&bus, "a", json!(1)),
            Err(DiscussionError::Finished)
        ));
    }

    #[tokio::test]
    async fn function_node_executes_handler() {
        let node = FunctionNode::new(
            AgentDescriptor {
                id: "n".into(),
                role: "worker".into(),
            },
            |_m, _b| async {},
        );
        assert_eq!(node.descriptor().id, "n");
        node.handle(
            Message::new(Uuid::new_v4(), "x", "s", "k", json!({})),
            InMemoryBus::new(1),
        )
        .await;
    }

    #[tokio::test]
    async fn serve_node_with_cancel_stops_consumer_loop() {
        let bus = InMemoryBus::new(4);
        let node = std::sync::Arc::new(FunctionNode::new(
            AgentDescriptor {
                id: "n".into(),
                role: "worker".into(),
            },
            |_m, _b| async {},
        ));
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(serve_node_with_cancel(
            node,
            bus,
            "tasks".into(),
            cancel.clone(),
        ));
        cancel.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
    }
}
