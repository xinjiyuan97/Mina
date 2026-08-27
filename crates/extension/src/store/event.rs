use std::{
    collections::HashSet,
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_core::event_runtime::{
    ClaimDeliveries, ClaimTimers, CompleteDelivery, CreateSubscription, Delivery, DeliveryId,
    DeliveryStatus, EventComponentDescriptor, EventEnvelope, EventError, EventFuture, EventId,
    EventStore, PublishEvent, PublishResult, RetryDelivery, ScheduleOnce, StartPosition,
    Subscription, SubscriptionId, SubscriptionMode, SubscriptionScope, SubscriptionStatus, Timer,
    TimerId, TimerStatus, validate_publish,
};
use agent_core::harness::{
    ActivationId, ClaimedActivation, CompleteFlowEffect, CompleteFlowRun, ContinueFlowRun,
    EffectRequest, FlowEffect, FlowEffectId, FlowEffectStatus, FlowError, FlowFuture,
    FlowInboxItem, FlowRunState, FlowRunStatus, FlowStore, RetryFlowEffect, RunId, SuspendFlowRun,
    WakeFlowRun,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

const MAX_CLAIM_BATCH: usize = 1_000;

#[derive(Debug, Clone)]
pub struct SqliteEventStore {
    pub(super) connection: Arc<Mutex<Connection>>,
    pub(super) identity: String,
}

impl SqliteEventStore {
    pub async fn open(
        path: impl AsRef<Path>,
        identity: impl Into<String>,
    ) -> Result<Self, EventError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| {
                EventError::backend(format!("create event store directory: {error}"))
            })?;
        }
        let migration_path = path;
        let connection = run_blocking(move || {
            let connection = connect(&migration_path)?;
            migrate(&connection)?;
            Ok(connection)
        })
        .await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            identity: identity.into(),
        })
    }
}

impl EventStore for SqliteEventStore {
    fn descriptor(&self) -> EventComponentDescriptor {
        EventComponentDescriptor {
            identity: self.identity.clone(),
            kind: "sqlite_event_store".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn publish(&self, command: PublishEvent) -> EventFuture<'_, PublishResult> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_publish(&command)?;
            run_blocking(move || {
                let mut connection = lock_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin event publication", error))?;
                let command_json = encode(&command, "published event")?;
                if let Some((existing_command, envelope_json)) = transaction
                    .query_row(
                        "SELECT command_json, envelope_json FROM workflow_events WHERE event_id = ?1",
                        [command.event_id.to_string()],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()
                    .map_err(|error| sql_error("read replayed event", error))?
                {
                    if existing_command != command_json {
                        return Err(EventError::Conflict(
                            "event_id was reused with different content".into(),
                        ));
                    }
                    let event: EventEnvelope = decode(&envelope_json, "event envelope")?;
                    let delivery_ids = delivery_ids_for_event(&transaction, command.event_id)?;
                    transaction
                        .commit()
                        .map_err(|error| sql_error("commit replayed event", error))?;
                    return Ok(PublishResult {
                        event,
                        delivery_ids,
                        replayed: true,
                    });
                }

                expire_subscriptions(&transaction, command.recorded_at_ms)?;
                transaction
                    .execute(
                        "INSERT INTO workflow_events (
                            event_id, topic, event_type, source, subject, correlation_id,
                            recorded_at_ms, command_json, envelope_json
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, '')",
                        params![
                            command.event_id.to_string(),
                            command.topic,
                            command.event_type,
                            source_key(command.source),
                            command.subject,
                            command.correlation_id,
                            command.recorded_at_ms,
                            command_json,
                        ],
                    )
                    .map_err(|error| sql_error("append event", error))?;
                let sequence = u64::try_from(transaction.last_insert_rowid())
                    .map_err(|_| EventError::backend("event sequence exceeded u64"))?;
                let event = EventEnvelope { sequence, event: command };
                transaction
                    .execute(
                        "UPDATE workflow_events SET envelope_json = ?1 WHERE event_id = ?2",
                        params![encode(&event, "event envelope")?, event.event.event_id.to_string()],
                    )
                    .map_err(|error| sql_error("finalize event envelope", error))?;
                let subscriptions = active_subscriptions(&transaction)?;
                let mut delivery_ids = Vec::new();
                for subscription in subscriptions {
                    if event.sequence <= subscription.cursor
                        || !scope_matches(subscription.definition.scope, &event)
                        || !subscription.definition.filter.matches(&event)
                        || (subscription.definition.mode == SubscriptionMode::Once
                            && subscription_has_delivery(
                                &transaction,
                                subscription.definition.subscription_id,
                            )?)
                    {
                        continue;
                    }
                    let delivery_id = DeliveryId {
                        subscription_id: subscription.definition.subscription_id,
                        event_id: event.event.event_id,
                    };
                    insert_delivery(
                        &transaction,
                        &delivery_id,
                        &subscription,
                        event.sequence,
                        event.event.recorded_at_ms,
                    )?;
                    delivery_ids.push(delivery_id);
                }
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit event publication", error))?;
                Ok(PublishResult {
                    event,
                    delivery_ids,
                    replayed: false,
                })
            })
            .await
        })
    }

    fn get_event(&self, event_id: EventId) -> EventFuture<'_, Option<EventEnvelope>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                connection
                    .query_row(
                        "SELECT envelope_json FROM workflow_events WHERE event_id = ?1",
                        [event_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| sql_error("read event", error))?
                    .map(|json| decode(&json, "event envelope"))
                    .transpose()
            })
            .await
        })
    }

    fn subscribe(&self, command: CreateSubscription) -> EventFuture<'_, Subscription> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            command.validate()?;
            run_blocking(move || {
                let mut connection = lock_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin subscription", error))?;
                let definition_json = encode(&command, "subscription definition")?;
                if let Some((existing_definition, subscription_json)) = transaction
                    .query_row(
                        "SELECT definition_json, subscription_json FROM workflow_subscriptions
                         WHERE subscription_id = ?1",
                        [command.subscription_id.to_string()],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()
                    .map_err(|error| sql_error("read replayed subscription", error))?
                {
                    if existing_definition != definition_json {
                        return Err(EventError::Conflict(
                            "subscription_id was reused with different content".into(),
                        ));
                    }
                    return decode(&subscription_json, "subscription");
                }
                let latest = latest_event_sequence(&transaction)?;
                let cursor = match command.start_position {
                    StartPosition::Now => latest,
                    StartPosition::Beginning => 0,
                    StartPosition::After { sequence } => sequence,
                };
                let subscription = Subscription {
                    definition: command,
                    status: SubscriptionStatus::Active,
                    cursor,
                    delivery_count: 0,
                };
                insert_subscription(&transaction, &subscription, &definition_json)?;
                enqueue_historical_deliveries(&transaction, &subscription)?;
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit subscription", error))?;
                Ok(subscription)
            })
            .await
        })
    }

    fn get_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> EventFuture<'_, Option<Subscription>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                connection
                    .query_row(
                        "SELECT subscription_json FROM workflow_subscriptions WHERE subscription_id = ?1",
                        [subscription_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| sql_error("read subscription", error))?
                    .map(|json| decode(&json, "subscription"))
                    .transpose()
            })
            .await
        })
    }

    fn cancel_subscription(
        &self,
        subscription_id: SubscriptionId,
        _cancelled_at_ms: i64,
    ) -> EventFuture<'_, Subscription> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                update_subscription_status(
                    &connection,
                    subscription_id,
                    SubscriptionStatus::Cancelled,
                )
            })
            .await
        })
    }

    fn claim_deliveries(&self, command: ClaimDeliveries) -> EventFuture<'_, Vec<Delivery>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_claim(&command.worker_id, command.now_ms, command.lease_until_ms)?;
            run_blocking(move || claim_deliveries_blocking(&connection, command)).await
        })
    }

    fn complete_delivery(&self, command: CompleteDelivery) -> EventFuture<'_, Delivery> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || complete_delivery_blocking(&connection, command)).await
        })
    }

    fn retry_delivery(&self, command: RetryDelivery) -> EventFuture<'_, Delivery> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || retry_delivery_blocking(&connection, command)).await
        })
    }

    fn schedule_once(&self, command: ScheduleOnce) -> EventFuture<'_, Timer> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            command.validate()?;
            run_blocking(move || schedule_once_blocking(&connection, command)).await
        })
    }

    fn claim_due_timers(&self, command: ClaimTimers) -> EventFuture<'_, Vec<Timer>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_claim(&command.worker_id, command.now_ms, command.lease_until_ms)?;
            run_blocking(move || claim_timers_blocking(&connection, command)).await
        })
    }

    fn complete_timer(
        &self,
        timer_id: TimerId,
        worker_id: String,
        event_id: EventId,
    ) -> EventFuture<'_, Timer> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                complete_timer_blocking(&connection, timer_id, &worker_id, event_id)
            })
            .await
        })
    }

    fn cancel_timer(&self, timer_id: TimerId, _cancelled_at_ms: i64) -> EventFuture<'_, Timer> {
        let connection = Arc::clone(&self.connection);
        Box::pin(
            async move { run_blocking(move || cancel_timer_blocking(&connection, timer_id)).await },
        )
    }
}

impl FlowStore for SqliteEventStore {
    fn create(&self, state: FlowRunState) -> FlowFuture<'_, FlowRunState> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            state.checkpoint.validate()?;
            if state.status != FlowRunStatus::Runnable
                || state.revision != 0
                || state.activation_id.is_some()
                || state.lease_owner.is_some()
                || state.lease_until_ms.is_some()
            {
                return Err(FlowError::Invalid(
                    "new flow state must be runnable at revision zero without a lease".into(),
                ));
            }
            run_flow_blocking(move || {
                let connection = lock_flow_connection(&connection)?;
                let encoded = encode_flow(&state, "flow state")?;
                if let Some(existing_json) = connection
                    .query_row(
                        "SELECT state_json FROM workflow_runs WHERE run_id = ?1",
                        [state.run_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| flow_sql_error("read replayed flow state", error))?
                {
                    let existing = decode_flow(&existing_json, "flow state")?;
                    if existing == state {
                        return Ok(existing);
                    }
                    return Err(FlowError::Conflict(
                        "flow run already exists with different state".into(),
                    ));
                }
                connection
                    .execute(
                        "INSERT INTO workflow_runs (
                            run_id, status, revision, lease_owner, lease_until_ms,
                            updated_at_ms, state_json
                         ) VALUES (?1, 'runnable', 0, NULL, NULL, ?2, ?3)",
                        params![state.run_id.to_string(), state.updated_at_ms, encoded],
                    )
                    .map_err(|error| flow_sql_error("insert flow state", error))?;
                Ok(state)
            })
            .await
        })
    }

    fn get(&self, run_id: RunId) -> FlowFuture<'_, Option<FlowRunState>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_flow_blocking(move || {
                let connection = lock_flow_connection(&connection)?;
                connection
                    .query_row(
                        "SELECT state_json FROM workflow_runs WHERE run_id = ?1",
                        [run_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| flow_sql_error("read flow state", error))?
                    .map(|json| decode_flow(&json, "flow state"))
                    .transpose()
            })
            .await
        })
    }

    fn continue_run(&self, command: ContinueFlowRun) -> FlowFuture<'_, FlowRunState> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            command.checkpoint.validate()?;
            validate_effects(&command.effects)?;
            run_flow_blocking(move || continue_flow_blocking(&connection, command)).await
        })
    }

    fn suspend(&self, command: SuspendFlowRun) -> FlowFuture<'_, FlowRunState> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            command.checkpoint.validate()?;
            if command.waits.is_empty() {
                return Err(FlowError::Invalid(
                    "suspended flow requires at least one wait".into(),
                ));
            }
            validate_effects(&command.effects)?;
            run_flow_blocking(move || suspend_flow_blocking(&connection, command)).await
        })
    }

    fn wake(&self, command: WakeFlowRun) -> FlowFuture<'_, FlowRunState> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_flow_blocking(move || wake_flow_blocking(&connection, command)).await
        })
    }

    fn claim_runnable(
        &self,
        worker_id: String,
        now_ms: i64,
        lease_until_ms: i64,
        limit: usize,
    ) -> FlowFuture<'_, Vec<ClaimedActivation>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_flow_claim(&worker_id, now_ms, lease_until_ms)?;
            run_flow_blocking(move || {
                claim_flows_blocking(&connection, &worker_id, now_ms, lease_until_ms, limit)
            })
            .await
        })
    }

    fn claim_effects(
        &self,
        worker_id: String,
        now_ms: i64,
        lease_until_ms: i64,
        limit: usize,
    ) -> FlowFuture<'_, Vec<FlowEffect>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_flow_claim(&worker_id, now_ms, lease_until_ms)?;
            run_flow_blocking(move || {
                claim_effects_blocking(&connection, &worker_id, now_ms, lease_until_ms, limit)
            })
            .await
        })
    }

    fn complete_effect(&self, command: CompleteFlowEffect) -> FlowFuture<'_, FlowEffect> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_flow_blocking(move || complete_effect_blocking(&connection, command)).await
        })
    }

    fn retry_effect(&self, command: RetryFlowEffect) -> FlowFuture<'_, FlowEffect> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_flow_blocking(move || retry_effect_blocking(&connection, command)).await
        })
    }

    fn complete(&self, command: CompleteFlowRun) -> FlowFuture<'_, FlowRunState> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            if !command.status.is_terminal() {
                return Err(FlowError::Invalid(
                    "complete flow requires a terminal status".into(),
                ));
            }
            run_flow_blocking(move || complete_flow_blocking(&connection, command)).await
        })
    }

    fn cancel(&self, run_id: RunId, cancelled_at_ms: i64) -> FlowFuture<'_, FlowRunState> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_flow_blocking(move || cancel_flow_blocking(&connection, run_id, cancelled_at_ms))
                .await
        })
    }
}

fn continue_flow_blocking(
    connection: &Mutex<Connection>,
    command: ContinueFlowRun,
) -> Result<FlowRunState, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow continuation", error))?;
    let mut state = load_flow(&transaction, command.run_id)?;
    require_flow_activation(&state, command.activation_id, command.expected_revision)?;
    let next_revision = state.revision.saturating_add(1);
    persist_flow_effects(
        &transaction,
        command.run_id,
        next_revision,
        &command.effects,
        command.continued_at_ms,
    )?;
    consume_flow_inbox(&transaction, command.run_id, next_revision)?;
    cancel_flow_waits(&transaction, &state.wait_subscription_ids)?;
    state.revision = next_revision;
    state.status = FlowRunStatus::Runnable;
    state.activation_id = None;
    state.checkpoint = command.checkpoint;
    state.wait_subscription_ids.clear();
    state.lease_owner = None;
    state.lease_until_ms = None;
    state.updated_at_ms = command.continued_at_ms;
    persist_flow(&transaction, &state)?;
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow continuation", error))?;
    Ok(state)
}

fn suspend_flow_blocking(
    connection: &Mutex<Connection>,
    command: SuspendFlowRun,
) -> Result<FlowRunState, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow suspension", error))?;
    let mut state = load_flow(&transaction, command.run_id)?;
    require_flow_activation(&state, command.activation_id, command.expected_revision)?;
    cancel_flow_waits(&transaction, &state.wait_subscription_ids)?;
    let mut wait_ids = Vec::with_capacity(command.waits.len());
    for wait in &command.waits {
        if wait.wait_key.trim().is_empty() || wait.wait_key.len() > 256 {
            return Err(FlowError::Invalid(
                "flow wait_key must contain 1 to 256 bytes".into(),
            ));
        }
        wait.subscription.validate().map_err(flow_from_event)?;
        validate_flow_wait(command.run_id, wait)?;
        let definition_json = encode_flow(&wait.subscription, "flow subscription")?;
        if let Some(existing_json) = transaction
            .query_row(
                "SELECT definition_json FROM workflow_subscriptions WHERE subscription_id = ?1",
                [wait.subscription.subscription_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| flow_sql_error("read flow subscription", error))?
        {
            if existing_json != definition_json {
                return Err(FlowError::Conflict(
                    "flow subscription id was reused with different content".into(),
                ));
            }
        } else {
            let cursor = match wait.subscription.start_position {
                StartPosition::Now => {
                    latest_event_sequence(&transaction).map_err(flow_from_event)?
                }
                StartPosition::Beginning => 0,
                StartPosition::After { sequence } => sequence,
            };
            let subscription = Subscription {
                definition: wait.subscription.clone(),
                status: SubscriptionStatus::Active,
                cursor,
                delivery_count: 0,
            };
            insert_subscription(&transaction, &subscription, &definition_json)
                .map_err(flow_from_event)?;
            enqueue_historical_deliveries(&transaction, &subscription).map_err(flow_from_event)?;
        }
        wait_ids.push(wait.subscription.subscription_id);
    }
    let next_revision = state.revision.saturating_add(1);
    persist_flow_effects(
        &transaction,
        command.run_id,
        next_revision,
        &command.effects,
        command.suspended_at_ms,
    )?;
    consume_flow_inbox(&transaction, command.run_id, next_revision)?;
    state.revision = next_revision;
    state.status = FlowRunStatus::WaitingEvent;
    state.activation_id = None;
    state.checkpoint = command.checkpoint;
    state.wait_subscription_ids = wait_ids;
    state.lease_owner = None;
    state.lease_until_ms = None;
    state.updated_at_ms = command.suspended_at_ms;
    persist_flow(&transaction, &state)?;
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow suspension", error))?;
    Ok(state)
}

fn wake_flow_blocking(
    connection: &Mutex<Connection>,
    command: WakeFlowRun,
) -> Result<FlowRunState, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow wake", error))?;
    let mut state = load_flow(&transaction, command.run_id)?;
    if state.status.is_terminal() {
        return Ok(state);
    }
    if !matches!(
        state.status,
        FlowRunStatus::WaitingEvent | FlowRunStatus::Runnable
    ) {
        return Err(FlowError::Conflict(
            "only waiting or already-runnable flows can receive wake events".into(),
        ));
    }
    if !state
        .wait_subscription_ids
        .contains(&command.subscription_id)
    {
        return Err(FlowError::Conflict(
            "wake subscription is not part of the current checkpoint".into(),
        ));
    }
    let event_json = transaction
        .query_row(
            "SELECT envelope_json FROM workflow_events WHERE event_id = ?1",
            [command.event_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| flow_sql_error("load flow wake event", error))?
        .ok_or(FlowError::NotFound)?;
    transaction
        .execute(
            "INSERT OR IGNORE INTO workflow_inbox (
                run_id, event_id, event_json, consumed_revision, received_at_ms
             ) VALUES (?1, ?2, ?3, NULL, ?4)",
            params![
                command.run_id.to_string(),
                command.event_id.to_string(),
                event_json,
                command.woken_at_ms,
            ],
        )
        .map_err(|error| flow_sql_error("append flow inbox", error))?;
    if state.status == FlowRunStatus::WaitingEvent {
        state.revision = state.revision.saturating_add(1);
        state.status = FlowRunStatus::Runnable;
        state.updated_at_ms = command.woken_at_ms;
        persist_flow(&transaction, &state)?;
    }
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow wake", error))?;
    Ok(state)
}

fn claim_flows_blocking(
    connection: &Mutex<Connection>,
    worker_id: &str,
    now_ms: i64,
    lease_until_ms: i64,
    limit: usize,
) -> Result<Vec<ClaimedActivation>, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow claim", error))?;
    let mut statement = transaction
        .prepare(
            "SELECT run_id FROM workflow_runs
             WHERE status = 'runnable'
                OR (status = 'running' AND lease_until_ms IS NOT NULL AND lease_until_ms <= ?1)
             ORDER BY updated_at_ms, run_id LIMIT ?2",
        )
        .map_err(|error| flow_sql_error("prepare flow claim", error))?;
    let ids = statement
        .query_map(
            params![
                now_ms,
                i64::try_from(limit.clamp(1, MAX_CLAIM_BATCH)).unwrap_or(i64::MAX)
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| flow_sql_error("query flow claim", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| flow_sql_error("read flow claim", error))?;
    drop(statement);
    let mut claimed = Vec::with_capacity(ids.len());
    for id in ids {
        let run_id = id
            .parse::<RunId>()
            .map_err(|_| FlowError::Backend("stored flow run id is invalid".into()))?;
        let mut state = load_flow(&transaction, run_id)?;
        state.revision = state.revision.saturating_add(1);
        state.status = FlowRunStatus::Running;
        state.activation_id = Some(ActivationId::new());
        state.lease_owner = Some(worker_id.to_owned());
        state.lease_until_ms = Some(lease_until_ms);
        state.updated_at_ms = now_ms;
        persist_flow(&transaction, &state)?;
        claimed.push(ClaimedActivation {
            inbox: load_flow_inbox(&transaction, run_id)?,
            state,
        });
    }
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow claim", error))?;
    Ok(claimed)
}

fn complete_flow_blocking(
    connection: &Mutex<Connection>,
    command: CompleteFlowRun,
) -> Result<FlowRunState, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow completion", error))?;
    let mut state = load_flow(&transaction, command.run_id)?;
    if state.status.is_terminal() && state.status == command.status {
        return Ok(state);
    }
    require_flow_activation(&state, command.activation_id, command.expected_revision)?;
    let next_revision = state.revision.saturating_add(1);
    consume_flow_inbox(&transaction, command.run_id, next_revision)?;
    cancel_flow_waits(&transaction, &state.wait_subscription_ids)?;
    state.revision = next_revision;
    state.status = command.status;
    state.activation_id = None;
    state.wait_subscription_ids.clear();
    state.lease_owner = None;
    state.lease_until_ms = None;
    state.updated_at_ms = command.completed_at_ms;
    persist_flow(&transaction, &state)?;
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow completion", error))?;
    Ok(state)
}

fn cancel_flow_blocking(
    connection: &Mutex<Connection>,
    run_id: RunId,
    cancelled_at_ms: i64,
) -> Result<FlowRunState, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow cancellation", error))?;
    let mut state = load_flow(&transaction, run_id)?;
    if state.status.is_terminal() {
        return Ok(state);
    }
    cancel_flow_waits(&transaction, &state.wait_subscription_ids)?;
    transaction
        .execute(
            "UPDATE workflow_effects SET status = 'cancelled', lease_owner = NULL,
                lease_until_ms = NULL, last_error = 'flow run was cancelled'
             WHERE run_id = ?1 AND status IN ('pending', 'executing', 'retry_pending')",
            [run_id.to_string()],
        )
        .map_err(|error| flow_sql_error("cancel flow effects", error))?;
    state.revision = state.revision.saturating_add(1);
    state.status = FlowRunStatus::Cancelled;
    state.activation_id = None;
    state.wait_subscription_ids.clear();
    state.lease_owner = None;
    state.lease_until_ms = None;
    state.updated_at_ms = cancelled_at_ms;
    persist_flow(&transaction, &state)?;
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow cancellation", error))?;
    Ok(state)
}

fn validate_effects(effects: &[EffectRequest]) -> Result<(), FlowError> {
    if effects.len() > MAX_CLAIM_BATCH {
        return Err(FlowError::Invalid(format!(
            "one machine yield cannot request more than {MAX_CLAIM_BATCH} effects"
        )));
    }
    for effect in effects {
        effect.validate()?;
    }
    Ok(())
}

fn persist_flow_effects(
    connection: &Connection,
    run_id: RunId,
    revision: u64,
    effects: &[EffectRequest],
    requested_at_ms: i64,
) -> Result<(), FlowError> {
    for (index, effect) in effects.iter().enumerate() {
        let effect_index =
            u32::try_from(index).map_err(|_| FlowError::Invalid("too many flow effects".into()))?;
        connection
            .execute(
                "INSERT INTO workflow_effects (
                    run_id, revision, effect_index, status, effect_json,
                    attempts, next_attempt_at_ms
                 ) VALUES (?1, ?2, ?3, 'pending', ?4, 0, ?5)",
                params![
                    run_id.to_string(),
                    to_flow_i64(revision, "flow revision")?,
                    i64::from(effect_index),
                    encode_flow(effect, "flow effect")?,
                    requested_at_ms,
                ],
            )
            .map_err(|error| flow_sql_error("persist flow effect", error))?;
    }
    Ok(())
}

fn consume_flow_inbox(
    connection: &Connection,
    run_id: RunId,
    revision: u64,
) -> Result<(), FlowError> {
    connection
        .execute(
            "UPDATE workflow_inbox SET consumed_revision = ?1
             WHERE run_id = ?2 AND consumed_revision IS NULL",
            params![to_flow_i64(revision, "flow revision")?, run_id.to_string(),],
        )
        .map_err(|error| flow_sql_error("consume flow inbox", error))?;
    Ok(())
}

fn cancel_flow_waits(
    connection: &Connection,
    subscriptions: &[SubscriptionId],
) -> Result<(), FlowError> {
    for subscription_id in subscriptions {
        let mut subscription =
            load_subscription(connection, *subscription_id).map_err(flow_from_event)?;
        if matches!(
            subscription.status,
            SubscriptionStatus::Active
                | SubscriptionStatus::Delivering
                | SubscriptionStatus::Paused
        ) {
            subscription.status = SubscriptionStatus::Cancelled;
            persist_subscription(connection, &subscription).map_err(flow_from_event)?;
        }
    }
    Ok(())
}

fn claim_effects_blocking(
    connection: &Mutex<Connection>,
    worker_id: &str,
    now_ms: i64,
    lease_until_ms: i64,
    limit: usize,
) -> Result<Vec<FlowEffect>, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow effect claim", error))?;
    let mut statement = transaction
        .prepare(
            "SELECT run_id, revision, effect_index FROM workflow_effects
             WHERE ((status IN ('pending', 'retry_pending') AND next_attempt_at_ms <= ?1)
                OR (status = 'executing' AND lease_until_ms IS NOT NULL AND lease_until_ms <= ?1))
             ORDER BY next_attempt_at_ms, run_id, revision, effect_index LIMIT ?2",
        )
        .map_err(|error| flow_sql_error("prepare flow effect claim", error))?;
    let ids = statement
        .query_map(
            params![
                now_ms,
                i64::try_from(limit.clamp(1, MAX_CLAIM_BATCH)).unwrap_or(i64::MAX)
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(|error| flow_sql_error("query flow effect claim", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| flow_sql_error("read flow effect claim", error))?;
    drop(statement);
    let mut effects = Vec::with_capacity(ids.len());
    for (run_id, revision, effect_index) in ids {
        let effect_id = parse_flow_effect_id(&run_id, revision, effect_index)?;
        transaction
            .execute(
                "UPDATE workflow_effects SET status = 'executing', attempts = attempts + 1,
                    lease_owner = ?1, lease_until_ms = ?2
                 WHERE run_id = ?3 AND revision = ?4 AND effect_index = ?5",
                params![worker_id, lease_until_ms, run_id, revision, effect_index,],
            )
            .map_err(|error| flow_sql_error("claim flow effect", error))?;
        effects.push(load_flow_effect(&transaction, effect_id)?);
    }
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow effect claim", error))?;
    Ok(effects)
}

fn complete_effect_blocking(
    connection: &Mutex<Connection>,
    command: CompleteFlowEffect,
) -> Result<FlowEffect, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow effect completion", error))?;
    let effect = load_flow_effect(&transaction, command.effect_id)?;
    if effect.status == FlowEffectStatus::Completed {
        return Ok(effect);
    }
    ensure_effect_lease(&effect, &command.worker_id)?;
    update_flow_effect_status(
        &transaction,
        command.effect_id,
        FlowEffectStatus::Completed,
        command.completed_at_ms,
        None,
    )?;
    let completed = load_flow_effect(&transaction, command.effect_id)?;
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow effect completion", error))?;
    Ok(completed)
}

fn retry_effect_blocking(
    connection: &Mutex<Connection>,
    command: RetryFlowEffect,
) -> Result<FlowEffect, FlowError> {
    let mut connection = lock_flow_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| flow_sql_error("begin flow effect retry", error))?;
    let effect = load_flow_effect(&transaction, command.effect_id)?;
    let requested_status = if command.dead_letter {
        FlowEffectStatus::DeadLettered
    } else {
        FlowEffectStatus::RetryPending
    };
    if effect.status == requested_status
        && effect.lease_owner.is_none()
        && effect.last_error.as_deref() == Some(command.error.as_str())
    {
        return Ok(effect);
    }
    ensure_effect_lease(&effect, &command.worker_id)?;
    update_flow_effect_status(
        &transaction,
        command.effect_id,
        requested_status,
        command.next_attempt_at_ms,
        Some(&command.error),
    )?;
    let retried = load_flow_effect(&transaction, command.effect_id)?;
    transaction
        .commit()
        .map_err(|error| flow_sql_error("commit flow effect retry", error))?;
    Ok(retried)
}

fn load_flow_effect(
    connection: &Connection,
    effect_id: FlowEffectId,
) -> Result<FlowEffect, FlowError> {
    connection
        .query_row(
            "SELECT status, effect_json, attempts, next_attempt_at_ms, lease_owner,
                lease_until_ms, last_error, completed_at_ms
             FROM workflow_effects WHERE run_id = ?1 AND revision = ?2 AND effect_index = ?3",
            params![
                effect_id.run_id.to_string(),
                to_flow_i64(effect_id.revision, "flow effect revision")?,
                i64::from(effect_id.effect_index),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(|error| flow_sql_error("load flow effect", error))?
        .ok_or(FlowError::NotFound)
        .and_then(
            |(
                status,
                request,
                attempts,
                next_attempt_at_ms,
                lease_owner,
                lease_until_ms,
                last_error,
                completed_at_ms,
            )| {
                Ok(FlowEffect {
                    effect_id,
                    request: decode_flow(&request, "flow effect")?,
                    status: parse_flow_effect_status(&status)?,
                    attempts: u32::try_from(attempts).map_err(|_| {
                        FlowError::Backend("stored flow effect attempts are invalid".into())
                    })?,
                    next_attempt_at_ms,
                    lease_owner,
                    lease_until_ms,
                    last_error,
                    completed_at_ms,
                })
            },
        )
}

fn update_flow_effect_status(
    connection: &Connection,
    effect_id: FlowEffectId,
    status: FlowEffectStatus,
    timestamp_ms: i64,
    error: Option<&str>,
) -> Result<(), FlowError> {
    let completed_at_ms = (status == FlowEffectStatus::Completed).then_some(timestamp_ms);
    let next_attempt_at_ms = if status == FlowEffectStatus::RetryPending {
        timestamp_ms
    } else {
        0
    };
    connection
        .execute(
            "UPDATE workflow_effects SET status = ?1, next_attempt_at_ms = ?2,
                lease_owner = NULL, lease_until_ms = NULL, last_error = ?3,
                completed_at_ms = ?4
             WHERE run_id = ?5 AND revision = ?6 AND effect_index = ?7",
            params![
                flow_effect_status_key(status),
                next_attempt_at_ms,
                error,
                completed_at_ms,
                effect_id.run_id.to_string(),
                to_flow_i64(effect_id.revision, "flow effect revision")?,
                i64::from(effect_id.effect_index),
            ],
        )
        .map_err(|error| flow_sql_error("update flow effect", error))?;
    Ok(())
}

fn ensure_effect_lease(effect: &FlowEffect, worker_id: &str) -> Result<(), FlowError> {
    if effect.status != FlowEffectStatus::Executing
        || effect.lease_owner.as_deref() != Some(worker_id)
    {
        return Err(FlowError::Conflict(
            "flow effect lease is not owned by this worker".into(),
        ));
    }
    Ok(())
}

fn parse_flow_effect_id(
    run_id: &str,
    revision: i64,
    effect_index: i64,
) -> Result<FlowEffectId, FlowError> {
    Ok(FlowEffectId {
        run_id: run_id
            .parse()
            .map_err(|_| FlowError::Backend("stored flow effect run id is invalid".into()))?,
        revision: from_flow_i64(revision, "flow effect revision")?,
        effect_index: u32::try_from(effect_index)
            .map_err(|_| FlowError::Backend("stored flow effect index is invalid".into()))?,
    })
}

fn validate_flow_wait(
    run_id: RunId,
    wait: &agent_core::harness::WaitSpec,
) -> Result<(), FlowError> {
    match (
        &wait.subscription.owner,
        wait.subscription.scope,
        &wait.subscription.delivery,
    ) {
        (
            agent_core::event_runtime::SubscriptionOwner::Run { run_id: owner },
            SubscriptionScope::Run { run_id: scope },
            agent_core::event_runtime::DeliveryTarget::WakeRun {
                run_id: target,
                wait_key,
            },
        ) if *owner == run_id
            && scope == run_id
            && *target == run_id
            && wait_key == &wait.wait_key =>
        {
            Ok(())
        }
        _ => Err(FlowError::Invalid(
            "flow waits must be owned, scoped, and delivered to the same run and wait_key".into(),
        )),
    }
}

fn require_flow_activation(
    state: &FlowRunState,
    activation_id: ActivationId,
    expected_revision: u64,
) -> Result<(), FlowError> {
    if state.status != FlowRunStatus::Running
        || state.activation_id != Some(activation_id)
        || state.revision != expected_revision
    {
        return Err(FlowError::Conflict(
            "flow activation or revision no longer owns the run".into(),
        ));
    }
    Ok(())
}

fn load_flow(connection: &Connection, run_id: RunId) -> Result<FlowRunState, FlowError> {
    let json = connection
        .query_row(
            "SELECT state_json FROM workflow_runs WHERE run_id = ?1",
            [run_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| flow_sql_error("load flow state", error))?
        .ok_or(FlowError::NotFound)?;
    decode_flow(&json, "flow state")
}

fn persist_flow(connection: &Connection, state: &FlowRunState) -> Result<(), FlowError> {
    connection
        .execute(
            "UPDATE workflow_runs SET status = ?1, revision = ?2, lease_owner = ?3,
                lease_until_ms = ?4, updated_at_ms = ?5, state_json = ?6 WHERE run_id = ?7",
            params![
                flow_status_key(state.status),
                to_flow_i64(state.revision, "flow revision")?,
                state.lease_owner,
                state.lease_until_ms,
                state.updated_at_ms,
                encode_flow(state, "flow state")?,
                state.run_id.to_string(),
            ],
        )
        .map_err(|error| flow_sql_error("persist flow state", error))?;
    Ok(())
}

fn load_flow_inbox(
    connection: &Connection,
    run_id: RunId,
) -> Result<Vec<FlowInboxItem>, FlowError> {
    let mut statement = connection
        .prepare(
            "SELECT event_json, consumed_revision FROM workflow_inbox
             WHERE run_id = ?1 AND consumed_revision IS NULL ORDER BY received_at_ms, event_id",
        )
        .map_err(|error| flow_sql_error("prepare flow inbox", error))?;
    let rows = statement
        .query_map([run_id.to_string()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .map_err(|error| flow_sql_error("query flow inbox", error))?;
    rows.map(|row| {
        let (event_json, consumed_revision) =
            row.map_err(|error| flow_sql_error("read flow inbox", error))?;
        Ok(FlowInboxItem {
            event: decode_flow(&event_json, "flow inbox event")?,
            consumed_revision: consumed_revision
                .map(|revision| from_flow_i64(revision, "consumed revision"))
                .transpose()?,
        })
    })
    .collect()
}

fn validate_flow_claim(worker_id: &str, now_ms: i64, lease_until_ms: i64) -> Result<(), FlowError> {
    if worker_id.trim().is_empty() || worker_id.len() > 128 {
        return Err(FlowError::Invalid(
            "flow worker_id must contain 1 to 128 bytes".into(),
        ));
    }
    if lease_until_ms <= now_ms {
        return Err(FlowError::Invalid(
            "flow activation lease must expire after now".into(),
        ));
    }
    Ok(())
}

fn lock_flow_connection(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, FlowError> {
    connection
        .lock()
        .map_err(|_| FlowError::Backend("SQLite flow connection lock was poisoned".into()))
}

fn encode_flow(value: &impl serde::Serialize, label: &str) -> Result<String, FlowError> {
    serde_json::to_string(value)
        .map_err(|error| FlowError::Backend(format!("encode {label}: {error}")))
}

fn decode_flow<T: serde::de::DeserializeOwned>(json: &str, label: &str) -> Result<T, FlowError> {
    serde_json::from_str(json)
        .map_err(|error| FlowError::Backend(format!("decode {label}: {error}")))
}

fn flow_sql_error(operation: &str, error: rusqlite::Error) -> FlowError {
    FlowError::Backend(format!("{operation}: {error}"))
}

fn flow_from_event(error: EventError) -> FlowError {
    match error {
        EventError::Invalid(message) => FlowError::Invalid(message),
        EventError::NotFound => FlowError::NotFound,
        EventError::Conflict(message) => FlowError::Conflict(message),
        EventError::Backend(message) => FlowError::Backend(message),
    }
}

async fn run_flow_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, FlowError> + Send + 'static,
) -> Result<T, FlowError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| FlowError::Backend(format!("flow store worker failed: {error}")))?
}

fn to_flow_i64(value: u64, label: &str) -> Result<i64, FlowError> {
    i64::try_from(value).map_err(|_| FlowError::Backend(format!("{label} exceeds SQLite range")))
}

fn from_flow_i64(value: i64, label: &str) -> Result<u64, FlowError> {
    u64::try_from(value).map_err(|_| FlowError::Backend(format!("stored {label} is invalid")))
}

const fn flow_status_key(status: FlowRunStatus) -> &'static str {
    match status {
        FlowRunStatus::Runnable => "runnable",
        FlowRunStatus::Running => "running",
        FlowRunStatus::WaitingEvent => "waiting_event",
        FlowRunStatus::Completed => "completed",
        FlowRunStatus::Failed => "failed",
        FlowRunStatus::Cancelled => "cancelled",
    }
}

const fn flow_effect_status_key(status: FlowEffectStatus) -> &'static str {
    match status {
        FlowEffectStatus::Pending => "pending",
        FlowEffectStatus::Executing => "executing",
        FlowEffectStatus::RetryPending => "retry_pending",
        FlowEffectStatus::Completed => "completed",
        FlowEffectStatus::DeadLettered => "dead_lettered",
        FlowEffectStatus::Cancelled => "cancelled",
    }
}

fn parse_flow_effect_status(value: &str) -> Result<FlowEffectStatus, FlowError> {
    match value {
        "pending" => Ok(FlowEffectStatus::Pending),
        "executing" => Ok(FlowEffectStatus::Executing),
        "retry_pending" => Ok(FlowEffectStatus::RetryPending),
        "completed" => Ok(FlowEffectStatus::Completed),
        "dead_lettered" => Ok(FlowEffectStatus::DeadLettered),
        "cancelled" => Ok(FlowEffectStatus::Cancelled),
        _ => Err(FlowError::Backend(
            "stored flow effect status is invalid".into(),
        )),
    }
}

fn publish_event_from_row(json: String) -> Result<EventEnvelope, EventError> {
    decode(&json, "event envelope")
}

fn active_subscriptions(connection: &Connection) -> Result<Vec<Subscription>, EventError> {
    let mut statement = connection
        .prepare(
            "SELECT subscription_json FROM workflow_subscriptions WHERE status = 'active'
             ORDER BY created_at_ms, subscription_id",
        )
        .map_err(|error| sql_error("prepare active subscriptions", error))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| sql_error("query active subscriptions", error))?;
    rows.map(|row| {
        decode(
            &row.map_err(|error| sql_error("read active subscription", error))?,
            "subscription",
        )
    })
    .collect()
}

fn insert_subscription(
    connection: &Connection,
    subscription: &Subscription,
    definition_json: &str,
) -> Result<(), EventError> {
    connection
        .execute(
            "INSERT INTO workflow_subscriptions (
                subscription_id, status, cursor, delivery_count, expires_at_ms,
                created_at_ms, definition_json, subscription_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                subscription.definition.subscription_id.to_string(),
                subscription_status_key(subscription.status),
                to_i64(subscription.cursor, "subscription cursor")?,
                i64::from(subscription.delivery_count),
                subscription.definition.expires_at_ms,
                subscription.definition.created_at_ms,
                definition_json,
                encode(subscription, "subscription")?,
            ],
        )
        .map_err(|error| sql_error("insert subscription", error))?;
    Ok(())
}

fn enqueue_historical_deliveries(
    connection: &Connection,
    subscription: &Subscription,
) -> Result<(), EventError> {
    if matches!(subscription.definition.start_position, StartPosition::Now) {
        return Ok(());
    }
    let mut statement = connection
        .prepare("SELECT envelope_json FROM workflow_events WHERE sequence > ?1 ORDER BY sequence")
        .map_err(|error| sql_error("prepare historical events", error))?;
    let rows = statement
        .query_map(
            [to_i64(subscription.cursor, "subscription cursor")?],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| sql_error("query historical events", error))?;
    for row in rows {
        let event = publish_event_from_row(
            row.map_err(|error| sql_error("read historical event", error))?,
        )?;
        if scope_matches(subscription.definition.scope, &event)
            && subscription.definition.filter.matches(&event)
        {
            let delivery_id = DeliveryId {
                subscription_id: subscription.definition.subscription_id,
                event_id: event.event.event_id,
            };
            insert_delivery(
                connection,
                &delivery_id,
                subscription,
                event.sequence,
                event.event.recorded_at_ms,
            )?;
            if subscription.definition.mode == SubscriptionMode::Once {
                break;
            }
        }
    }
    Ok(())
}

fn insert_delivery(
    connection: &Connection,
    delivery_id: &DeliveryId,
    subscription: &Subscription,
    event_sequence: u64,
    next_attempt_at_ms: i64,
) -> Result<(), EventError> {
    connection
        .execute(
            "INSERT OR IGNORE INTO workflow_deliveries (
                subscription_id, event_id, event_sequence, status, attempts,
                next_attempt_at_ms, target_json
             ) VALUES (?1, ?2, ?3, 'pending', 0, ?4, ?5)",
            params![
                delivery_id.subscription_id.to_string(),
                delivery_id.event_id.to_string(),
                to_i64(event_sequence, "event sequence")?,
                next_attempt_at_ms,
                encode(&subscription.definition.delivery, "delivery target")?,
            ],
        )
        .map_err(|error| sql_error("insert delivery", error))?;
    Ok(())
}

fn delivery_ids_for_event(
    connection: &Connection,
    event_id: EventId,
) -> Result<Vec<DeliveryId>, EventError> {
    let mut statement = connection
        .prepare(
            "SELECT subscription_id FROM workflow_deliveries WHERE event_id = ?1
             ORDER BY subscription_id",
        )
        .map_err(|error| sql_error("prepare event deliveries", error))?;
    let rows = statement
        .query_map([event_id.to_string()], |row| row.get::<_, String>(0))
        .map_err(|error| sql_error("query event deliveries", error))?;
    rows.map(|row| {
        let subscription_id = row
            .map_err(|error| sql_error("read event delivery", error))?
            .parse()
            .map_err(|_| EventError::backend("stored subscription id is invalid"))?;
        Ok(DeliveryId {
            subscription_id,
            event_id,
        })
    })
    .collect()
}

fn subscription_has_delivery(
    connection: &Connection,
    subscription_id: SubscriptionId,
) -> Result<bool, EventError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_deliveries WHERE subscription_id = ?1)",
            [subscription_id.to_string()],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|error| sql_error("check once subscription delivery", error))
}

fn claim_deliveries_blocking(
    connection: &Mutex<Connection>,
    command: ClaimDeliveries,
) -> Result<Vec<Delivery>, EventError> {
    let mut connection = lock_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin delivery claim", error))?;
    expire_subscriptions(&transaction, command.now_ms)?;
    let limit = command.limit.clamp(1, MAX_CLAIM_BATCH);
    let mut statement = transaction
        .prepare(
            "SELECT d.subscription_id, d.event_id
             FROM workflow_deliveries d
             JOIN workflow_subscriptions s ON s.subscription_id = d.subscription_id
             WHERE (
                    (d.status IN ('pending', 'retry_pending')
                        AND d.next_attempt_at_ms <= ?1
                        AND s.status = 'active')
                 OR (d.status = 'delivering'
                        AND d.lease_until_ms IS NOT NULL
                        AND d.lease_until_ms <= ?1
                        AND s.status = 'delivering')
             )
             ORDER BY d.next_attempt_at_ms, d.event_sequence
             LIMIT ?2",
        )
        .map_err(|error| sql_error("prepare delivery claim", error))?;
    let rows = statement
        .query_map(
            params![command.now_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| sql_error("query delivery claim", error))?;
    let candidates = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sql_error("read delivery claim", error))?;
    drop(statement);
    let mut seen = HashSet::new();
    let mut deliveries = Vec::new();
    for (subscription_id, event_id) in candidates {
        if !seen.insert(subscription_id.clone()) {
            continue;
        }
        transaction
            .execute(
                "UPDATE workflow_deliveries SET status = 'delivering', attempts = attempts + 1,
                    lease_owner = ?1, lease_until_ms = ?2
                 WHERE subscription_id = ?3 AND event_id = ?4",
                params![
                    command.worker_id,
                    command.lease_until_ms,
                    subscription_id,
                    event_id
                ],
            )
            .map_err(|error| sql_error("claim delivery", error))?;
        transaction
            .execute(
                "UPDATE workflow_subscriptions SET status = 'delivering'
                 WHERE subscription_id = ?1",
                [&subscription_id],
            )
            .map_err(|error| sql_error("mark subscription delivering", error))?;
        deliveries.push(load_delivery_strings(
            &transaction,
            &subscription_id,
            &event_id,
        )?);
    }
    transaction
        .commit()
        .map_err(|error| sql_error("commit delivery claim", error))?;
    Ok(deliveries)
}

fn complete_delivery_blocking(
    connection: &Mutex<Connection>,
    command: CompleteDelivery,
) -> Result<Delivery, EventError> {
    let mut connection = lock_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin delivery completion", error))?;
    let delivery = load_delivery(&transaction, &command.delivery_id)?;
    ensure_delivery_lease(&delivery, &command.worker_id)?;
    transaction
        .execute(
            "UPDATE workflow_deliveries SET status = 'delivered', delivered_at_ms = ?1,
                lease_owner = NULL, lease_until_ms = NULL, last_error = NULL
             WHERE subscription_id = ?2 AND event_id = ?3",
            params![
                command.delivered_at_ms,
                command.delivery_id.subscription_id.to_string(),
                command.delivery_id.event_id.to_string(),
            ],
        )
        .map_err(|error| sql_error("complete delivery", error))?;
    let mut subscription = load_subscription(&transaction, command.delivery_id.subscription_id)?;
    subscription.cursor = subscription.cursor.max(delivery.event_sequence);
    subscription.delivery_count = subscription.delivery_count.saturating_add(1);
    subscription.status = if subscription.definition.mode == SubscriptionMode::Once
        || subscription
            .definition
            .max_deliveries
            .is_some_and(|maximum| subscription.delivery_count >= maximum)
    {
        SubscriptionStatus::Completed
    } else {
        SubscriptionStatus::Active
    };
    persist_subscription(&transaction, &subscription)?;
    let completed = load_delivery(&transaction, &command.delivery_id)?;
    transaction
        .commit()
        .map_err(|error| sql_error("commit delivery completion", error))?;
    Ok(completed)
}

fn retry_delivery_blocking(
    connection: &Mutex<Connection>,
    command: RetryDelivery,
) -> Result<Delivery, EventError> {
    let mut connection = lock_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin delivery retry", error))?;
    let delivery = load_delivery(&transaction, &command.delivery_id)?;
    ensure_delivery_lease(&delivery, &command.worker_id)?;
    let status = if command.dead_letter {
        DeliveryStatus::DeadLettered
    } else {
        DeliveryStatus::RetryPending
    };
    transaction
        .execute(
            "UPDATE workflow_deliveries SET status = ?1, next_attempt_at_ms = ?2,
                last_error = ?3, lease_owner = NULL, lease_until_ms = NULL
             WHERE subscription_id = ?4 AND event_id = ?5",
            params![
                delivery_status_key(status),
                command.next_attempt_at_ms,
                command.error,
                command.delivery_id.subscription_id.to_string(),
                command.delivery_id.event_id.to_string(),
            ],
        )
        .map_err(|error| sql_error("retry delivery", error))?;
    update_subscription_status_tx(
        &transaction,
        command.delivery_id.subscription_id,
        if command.dead_letter {
            SubscriptionStatus::DeadLettered
        } else {
            SubscriptionStatus::Active
        },
    )?;
    let retried = load_delivery(&transaction, &command.delivery_id)?;
    transaction
        .commit()
        .map_err(|error| sql_error("commit delivery retry", error))?;
    Ok(retried)
}

fn schedule_once_blocking(
    connection: &Mutex<Connection>,
    command: ScheduleOnce,
) -> Result<Timer, EventError> {
    let connection = lock_connection(connection)?;
    let definition_json = encode(&command, "timer definition")?;
    if let Some((existing_definition, timer_json)) = connection
        .query_row(
            "SELECT definition_json, timer_json FROM workflow_timers WHERE timer_id = ?1",
            [command.timer_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|error| sql_error("read replayed timer", error))?
    {
        if existing_definition != definition_json {
            return Err(EventError::Conflict(
                "timer_id was reused with different content".into(),
            ));
        }
        return decode(&timer_json, "timer");
    }
    let timer = Timer {
        definition: command,
        status: TimerStatus::Pending,
        lease_owner: None,
        lease_until_ms: None,
        fired_event_id: None,
    };
    connection
        .execute(
            "INSERT INTO workflow_timers (
                timer_id, fire_at_ms, status, definition_json, timer_json
             ) VALUES (?1, ?2, 'pending', ?3, ?4)",
            params![
                timer.definition.timer_id.to_string(),
                timer.definition.fire_at_ms,
                definition_json,
                encode(&timer, "timer")?,
            ],
        )
        .map_err(|error| sql_error("insert timer", error))?;
    Ok(timer)
}

fn claim_timers_blocking(
    connection: &Mutex<Connection>,
    command: ClaimTimers,
) -> Result<Vec<Timer>, EventError> {
    let mut connection = lock_connection(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin timer claim", error))?;
    let limit = command.limit.clamp(1, MAX_CLAIM_BATCH);
    let mut statement = transaction
        .prepare(
            "SELECT timer_id FROM workflow_timers
             WHERE status IN ('pending', 'firing') AND fire_at_ms <= ?1
               AND (lease_until_ms IS NULL OR lease_until_ms <= ?1)
             ORDER BY fire_at_ms, timer_id LIMIT ?2",
        )
        .map_err(|error| sql_error("prepare timer claim", error))?;
    let ids = statement
        .query_map(
            params![command.now_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| sql_error("query timer claim", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sql_error("read timer claim", error))?;
    drop(statement);
    let mut timers = Vec::new();
    for id in ids {
        let mut timer = load_timer_string(&transaction, &id)?;
        timer.status = TimerStatus::Firing;
        timer.lease_owner = Some(command.worker_id.clone());
        timer.lease_until_ms = Some(command.lease_until_ms);
        persist_timer(&transaction, &timer)?;
        timers.push(timer);
    }
    transaction
        .commit()
        .map_err(|error| sql_error("commit timer claim", error))?;
    Ok(timers)
}

fn complete_timer_blocking(
    connection: &Mutex<Connection>,
    timer_id: TimerId,
    worker_id: &str,
    event_id: EventId,
) -> Result<Timer, EventError> {
    let connection = lock_connection(connection)?;
    let mut timer = load_timer(&connection, timer_id)?;
    if timer.status == TimerStatus::Fired && timer.fired_event_id == Some(event_id) {
        return Ok(timer);
    }
    if timer.status != TimerStatus::Firing || timer.lease_owner.as_deref() != Some(worker_id) {
        return Err(EventError::Conflict(
            "timer lease is not owned by this worker".into(),
        ));
    }
    if timer.definition.event.event_id != event_id {
        return Err(EventError::Conflict(
            "timer completed with a different event id".into(),
        ));
    }
    timer.status = TimerStatus::Fired;
    timer.lease_owner = None;
    timer.lease_until_ms = None;
    timer.fired_event_id = Some(event_id);
    persist_timer(&connection, &timer)?;
    Ok(timer)
}

fn cancel_timer_blocking(
    connection: &Mutex<Connection>,
    timer_id: TimerId,
) -> Result<Timer, EventError> {
    let connection = lock_connection(connection)?;
    let mut timer = load_timer(&connection, timer_id)?;
    if timer.status == TimerStatus::Fired {
        return Err(EventError::Conflict(
            "a fired timer cannot be cancelled".into(),
        ));
    }
    timer.status = TimerStatus::Cancelled;
    timer.lease_owner = None;
    timer.lease_until_ms = None;
    persist_timer(&connection, &timer)?;
    Ok(timer)
}

fn load_subscription(
    connection: &Connection,
    subscription_id: SubscriptionId,
) -> Result<Subscription, EventError> {
    let json = connection
        .query_row(
            "SELECT subscription_json FROM workflow_subscriptions WHERE subscription_id = ?1",
            [subscription_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| sql_error("load subscription", error))?
        .ok_or(EventError::NotFound)?;
    decode(&json, "subscription")
}

fn persist_subscription(
    connection: &Connection,
    subscription: &Subscription,
) -> Result<(), EventError> {
    connection
        .execute(
            "UPDATE workflow_subscriptions SET status = ?1, cursor = ?2,
                delivery_count = ?3, subscription_json = ?4 WHERE subscription_id = ?5",
            params![
                subscription_status_key(subscription.status),
                to_i64(subscription.cursor, "subscription cursor")?,
                i64::from(subscription.delivery_count),
                encode(subscription, "subscription")?,
                subscription.definition.subscription_id.to_string(),
            ],
        )
        .map_err(|error| sql_error("persist subscription", error))?;
    Ok(())
}

fn update_subscription_status(
    connection: &Mutex<Connection>,
    subscription_id: SubscriptionId,
    status: SubscriptionStatus,
) -> Result<Subscription, EventError> {
    let connection = lock_connection(connection)?;
    let mut subscription = load_subscription(&connection, subscription_id)?;
    subscription.status = status;
    persist_subscription(&connection, &subscription)?;
    Ok(subscription)
}

fn update_subscription_status_tx(
    connection: &Connection,
    subscription_id: SubscriptionId,
    status: SubscriptionStatus,
) -> Result<(), EventError> {
    let mut subscription = load_subscription(connection, subscription_id)?;
    subscription.status = status;
    persist_subscription(connection, &subscription)
}

fn expire_subscriptions(connection: &Connection, now_ms: i64) -> Result<(), EventError> {
    let mut statement = connection
        .prepare(
            "SELECT subscription_id FROM workflow_subscriptions
             WHERE status IN ('active', 'delivering') AND expires_at_ms IS NOT NULL
               AND expires_at_ms <= ?1",
        )
        .map_err(|error| sql_error("prepare expired subscriptions", error))?;
    let ids = statement
        .query_map([now_ms], |row| row.get::<_, String>(0))
        .map_err(|error| sql_error("query expired subscriptions", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sql_error("read expired subscriptions", error))?;
    drop(statement);
    for id in ids {
        let subscription_id = id
            .parse()
            .map_err(|_| EventError::backend("stored subscription id is invalid"))?;
        update_subscription_status_tx(connection, subscription_id, SubscriptionStatus::Expired)?;
    }
    Ok(())
}

fn load_delivery(connection: &Connection, id: &DeliveryId) -> Result<Delivery, EventError> {
    load_delivery_strings(
        connection,
        &id.subscription_id.to_string(),
        &id.event_id.to_string(),
    )
}

fn load_delivery_strings(
    connection: &Connection,
    subscription_id: &str,
    event_id: &str,
) -> Result<Delivery, EventError> {
    connection
        .query_row(
            "SELECT event_sequence, status, attempts, next_attempt_at_ms, target_json,
                    lease_owner, lease_until_ms, last_error, delivered_at_ms
             FROM workflow_deliveries WHERE subscription_id = ?1 AND event_id = ?2",
            params![subscription_id, event_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                ))
            },
        )
        .optional()
        .map_err(|error| sql_error("load delivery", error))?
        .ok_or(EventError::NotFound)
        .and_then(|row| {
            Ok(Delivery {
                delivery_id: DeliveryId {
                    subscription_id: subscription_id
                        .parse()
                        .map_err(|_| EventError::backend("stored subscription id is invalid"))?,
                    event_id: event_id
                        .parse()
                        .map_err(|_| EventError::backend("stored event id is invalid"))?,
                },
                target: decode(&row.4, "delivery target")?,
                event_sequence: from_i64(row.0, "event sequence")?,
                status: parse_delivery_status(&row.1)?,
                attempts: u32::try_from(row.2)
                    .map_err(|_| EventError::backend("stored delivery attempts are invalid"))?,
                next_attempt_at_ms: row.3,
                lease_owner: row.5,
                lease_until_ms: row.6,
                last_error: row.7,
                delivered_at_ms: row.8,
            })
        })
}

fn ensure_delivery_lease(delivery: &Delivery, worker_id: &str) -> Result<(), EventError> {
    if delivery.status != DeliveryStatus::Delivering
        || delivery.lease_owner.as_deref() != Some(worker_id)
    {
        return Err(EventError::Conflict(
            "delivery lease is not owned by this worker".into(),
        ));
    }
    Ok(())
}

fn load_timer(connection: &Connection, timer_id: TimerId) -> Result<Timer, EventError> {
    load_timer_string(connection, &timer_id.to_string())
}

fn load_timer_string(connection: &Connection, timer_id: &str) -> Result<Timer, EventError> {
    let json = connection
        .query_row(
            "SELECT timer_json FROM workflow_timers WHERE timer_id = ?1",
            [timer_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| sql_error("load timer", error))?
        .ok_or(EventError::NotFound)?;
    decode(&json, "timer")
}

fn persist_timer(connection: &Connection, timer: &Timer) -> Result<(), EventError> {
    connection
        .execute(
            "UPDATE workflow_timers SET status = ?1, lease_owner = ?2, lease_until_ms = ?3,
                fired_event_id = ?4, timer_json = ?5 WHERE timer_id = ?6",
            params![
                timer_status_key(timer.status),
                timer.lease_owner,
                timer.lease_until_ms,
                timer.fired_event_id.map(|id| id.to_string()),
                encode(timer, "timer")?,
                timer.definition.timer_id.to_string(),
            ],
        )
        .map_err(|error| sql_error("persist timer", error))?;
    Ok(())
}

fn latest_event_sequence(connection: &Connection) -> Result<u64, EventError> {
    let value = connection
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM workflow_events",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| sql_error("read latest event sequence", error))?;
    from_i64(value, "latest event sequence")
}

fn scope_matches(scope: SubscriptionScope, event: &EventEnvelope) -> bool {
    match scope {
        SubscriptionScope::Global => true,
        SubscriptionScope::Run { run_id } => {
            event.event.subject.as_deref() == Some(&format!("run/{run_id}"))
        }
    }
}

fn validate_claim(worker_id: &str, now_ms: i64, lease_until_ms: i64) -> Result<(), EventError> {
    if worker_id.trim().is_empty() || worker_id.len() > 128 {
        return Err(EventError::invalid("worker_id must contain 1 to 128 bytes"));
    }
    if lease_until_ms <= now_ms {
        return Err(EventError::invalid("claim lease must expire after now"));
    }
    Ok(())
}

fn connect(path: &Path) -> Result<Connection, EventError> {
    let connection =
        Connection::open(path).map_err(|error| sql_error("open event database", error))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| sql_error("set event database timeout", error))?;
    connection
        .execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")
        .map_err(|error| sql_error("configure event database", error))?;
    Ok(connection)
}

fn migrate(connection: &Connection) -> Result<(), EventError> {
    connection
        .execute_batch(
            "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS workflow_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            topic TEXT NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            subject TEXT,
            correlation_id TEXT,
            recorded_at_ms INTEGER NOT NULL,
            command_json TEXT NOT NULL,
            envelope_json TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS workflow_events_match_idx
            ON workflow_events(topic, event_type, correlation_id, sequence);
         CREATE TABLE IF NOT EXISTS workflow_subscriptions (
            subscription_id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL,
            cursor INTEGER NOT NULL,
            delivery_count INTEGER NOT NULL,
            expires_at_ms INTEGER,
            created_at_ms INTEGER NOT NULL,
            definition_json TEXT NOT NULL,
            subscription_json TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS workflow_subscriptions_active_idx
            ON workflow_subscriptions(status, expires_at_ms, created_at_ms);
         CREATE TABLE IF NOT EXISTS workflow_deliveries (
            subscription_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_sequence INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            next_attempt_at_ms INTEGER NOT NULL,
            target_json TEXT NOT NULL,
            lease_owner TEXT,
            lease_until_ms INTEGER,
            last_error TEXT,
            delivered_at_ms INTEGER,
            PRIMARY KEY (subscription_id, event_id),
            FOREIGN KEY (subscription_id) REFERENCES workflow_subscriptions(subscription_id),
            FOREIGN KEY (event_id) REFERENCES workflow_events(event_id)
         );
         CREATE INDEX IF NOT EXISTS workflow_deliveries_claim_idx
            ON workflow_deliveries(status, next_attempt_at_ms, lease_until_ms);
         CREATE TABLE IF NOT EXISTS workflow_timers (
            timer_id TEXT PRIMARY KEY NOT NULL,
            fire_at_ms INTEGER NOT NULL,
            status TEXT NOT NULL,
            lease_owner TEXT,
            lease_until_ms INTEGER,
            fired_event_id TEXT,
            definition_json TEXT NOT NULL,
            timer_json TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS workflow_timers_due_idx
            ON workflow_timers(status, fire_at_ms, lease_until_ms);
         CREATE TABLE IF NOT EXISTS workflow_runs (
            run_id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            lease_owner TEXT,
            lease_until_ms INTEGER,
            updated_at_ms INTEGER NOT NULL,
            state_json TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS workflow_runs_runnable_idx
            ON workflow_runs(status, lease_until_ms, updated_at_ms);
         CREATE TABLE IF NOT EXISTS workflow_inbox (
            run_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_json TEXT NOT NULL,
            consumed_revision INTEGER,
            received_at_ms INTEGER NOT NULL,
            PRIMARY KEY (run_id, event_id),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
            FOREIGN KEY (event_id) REFERENCES workflow_events(event_id)
         );
         CREATE INDEX IF NOT EXISTS workflow_inbox_pending_idx
            ON workflow_inbox(run_id, consumed_revision, received_at_ms);
         CREATE TABLE IF NOT EXISTS workflow_effects (
            run_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            effect_index INTEGER NOT NULL,
            status TEXT NOT NULL,
            effect_json TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            next_attempt_at_ms INTEGER NOT NULL,
            lease_owner TEXT,
            lease_until_ms INTEGER,
            last_error TEXT,
            completed_at_ms INTEGER,
            PRIMARY KEY (run_id, revision, effect_index),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
         );
         CREATE INDEX IF NOT EXISTS workflow_effects_pending_idx
            ON workflow_effects(status, next_attempt_at_ms, lease_until_ms);
         CREATE TABLE IF NOT EXISTS workflow_jobs (
            job_id TEXT PRIMARY KEY NOT NULL,
            run_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL,
            next_attempt_at_ms INTEGER NOT NULL,
            lease_owner TEXT,
            lease_until_ms INTEGER,
            notification_status TEXT NOT NULL,
            notification_next_attempt_at_ms INTEGER NOT NULL,
            notification_lease_owner TEXT,
            notification_lease_until_ms INTEGER,
            dedupe_json TEXT NOT NULL,
            command_json TEXT NOT NULL,
            record_json TEXT NOT NULL,
            UNIQUE (run_id, kind, idempotency_key)
         );
         CREATE INDEX IF NOT EXISTS workflow_jobs_claim_idx
            ON workflow_jobs(status, next_attempt_at_ms, lease_until_ms);
         CREATE INDEX IF NOT EXISTS workflow_jobs_notification_idx
            ON workflow_jobs(notification_status, notification_next_attempt_at_ms,
                notification_lease_until_ms);
         COMMIT;",
        )
        .map_err(|error| sql_error("create event runtime schema", error))
}

fn lock_connection(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, EventError> {
    connection
        .lock()
        .map_err(|_| EventError::backend("SQLite event connection lock was poisoned"))
}

fn encode(value: &impl serde::Serialize, label: &str) -> Result<String, EventError> {
    serde_json::to_string(value)
        .map_err(|error| EventError::backend(format!("encode {label}: {error}")))
}

fn decode<T: serde::de::DeserializeOwned>(json: &str, label: &str) -> Result<T, EventError> {
    serde_json::from_str(json)
        .map_err(|error| EventError::backend(format!("decode {label}: {error}")))
}

fn sql_error(operation: &str, error: rusqlite::Error) -> EventError {
    EventError::backend(format!("{operation}: {error}"))
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, EventError> + Send + 'static,
) -> Result<T, EventError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| EventError::backend(format!("event store worker failed: {error}")))?
}

fn to_i64(value: u64, label: &str) -> Result<i64, EventError> {
    i64::try_from(value).map_err(|_| EventError::backend(format!("{label} exceeds SQLite range")))
}

fn from_i64(value: i64, label: &str) -> Result<u64, EventError> {
    u64::try_from(value).map_err(|_| EventError::backend(format!("stored {label} is invalid")))
}

const fn source_key(source: agent_core::event_runtime::EventSource) -> &'static str {
    use agent_core::event_runtime::EventSource;
    match source {
        EventSource::System => "system",
        EventSource::Gateway => "gateway",
        EventSource::Agent => "agent",
        EventSource::ToolWorker => "tool_worker",
        EventSource::JobWorker => "job_worker",
        EventSource::Timer => "timer",
    }
}

const fn subscription_status_key(status: SubscriptionStatus) -> &'static str {
    match status {
        SubscriptionStatus::Active => "active",
        SubscriptionStatus::Delivering => "delivering",
        SubscriptionStatus::Completed => "completed",
        SubscriptionStatus::Paused => "paused",
        SubscriptionStatus::Expired => "expired",
        SubscriptionStatus::Cancelled => "cancelled",
        SubscriptionStatus::DeadLettered => "dead_lettered",
    }
}

const fn delivery_status_key(status: DeliveryStatus) -> &'static str {
    match status {
        DeliveryStatus::Pending => "pending",
        DeliveryStatus::Delivering => "delivering",
        DeliveryStatus::Delivered => "delivered",
        DeliveryStatus::RetryPending => "retry_pending",
        DeliveryStatus::DeadLettered => "dead_lettered",
    }
}

fn parse_delivery_status(value: &str) -> Result<DeliveryStatus, EventError> {
    match value {
        "pending" => Ok(DeliveryStatus::Pending),
        "delivering" => Ok(DeliveryStatus::Delivering),
        "delivered" => Ok(DeliveryStatus::Delivered),
        "retry_pending" => Ok(DeliveryStatus::RetryPending),
        "dead_lettered" => Ok(DeliveryStatus::DeadLettered),
        _ => Err(EventError::backend("stored delivery status is invalid")),
    }
}

const fn timer_status_key(status: TimerStatus) -> &'static str {
    match status {
        TimerStatus::Pending => "pending",
        TimerStatus::Firing => "firing",
        TimerStatus::Fired => "fired",
        TimerStatus::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use agent_core::event_runtime::{
        DeliveryTarget, EventFilter, EventSource, PayloadPredicate, SubscriptionOwner,
    };
    use agent_core::harness::{
        CheckpointCodec, CheckpointEnvelope, CompleteFlowRun, FlowRunState, FlowRunStatus,
        FlowStore, SuspendFlowRun, WaitSpec, WakeFlowRun,
    };
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn published(event_id: EventId, topic: &str, recorded_at_ms: i64) -> PublishEvent {
        PublishEvent {
            event_id,
            topic: topic.into(),
            event_type: topic.into(),
            schema_version: 1,
            source: EventSource::Gateway,
            subject: None,
            correlation_id: Some("approval-1".into()),
            causation_id: None,
            occurred_at_ms: recorded_at_ms,
            recorded_at_ms,
            payload: json!({"decision": "allow"}),
        }
    }

    fn subscription(start_position: StartPosition) -> CreateSubscription {
        CreateSubscription {
            subscription_id: SubscriptionId::new(),
            owner: SubscriptionOwner::System {
                component: "test".into(),
            },
            scope: SubscriptionScope::Global,
            filter: EventFilter {
                topics: vec!["approval.*".into()],
                payload: vec![PayloadPredicate::Eq {
                    path: "/decision".into(),
                    value: json!("allow"),
                }],
                ..EventFilter::default()
            },
            delivery: DeliveryTarget::RustHandler {
                handler: "approval".into(),
            },
            mode: SubscriptionMode::Once,
            start_position,
            expires_at_ms: None,
            max_deliveries: Some(1),
            created_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn persists_matches_claims_and_deduplicates_events() {
        let directory = tempdir().expect("temporary directory should exist");
        let store = SqliteEventStore::open(directory.path().join("events.sqlite3"), "events:test")
            .await
            .expect("store should open");
        let subscription = store
            .subscribe(subscription(StartPosition::Now))
            .await
            .expect("subscription should persist");
        let command = published(EventId::new(), "approval.resolved", 10);
        let first = store
            .publish(command.clone())
            .await
            .expect("event should publish");
        let replay = store
            .publish(command)
            .await
            .expect("event replay should succeed");
        assert_eq!(first.event, replay.event);
        assert!(replay.replayed);
        assert_eq!(first.delivery_ids.len(), 1);

        let claimed = store
            .claim_deliveries(ClaimDeliveries {
                worker_id: "worker-1".into(),
                now_ms: 10,
                lease_until_ms: 1_010,
                limit: 10,
            })
            .await
            .expect("delivery should claim");
        assert_eq!(claimed.len(), 1);
        store
            .complete_delivery(CompleteDelivery {
                delivery_id: claimed[0].delivery_id.clone(),
                worker_id: "worker-1".into(),
                delivered_at_ms: 11,
            })
            .await
            .expect("delivery should complete");
        let loaded = store
            .get_subscription(subscription.definition.subscription_id)
            .await
            .expect("subscription should load")
            .expect("subscription should exist");
        assert_eq!(loaded.status, SubscriptionStatus::Completed);
    }

    #[tokio::test]
    async fn beginning_subscription_replays_history_and_timer_is_restart_safe() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("events.sqlite3");
        let store = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should open");
        store
            .publish(published(EventId::new(), "approval.resolved", 5))
            .await
            .expect("historical event should publish");
        store
            .subscribe(subscription(StartPosition::Beginning))
            .await
            .expect("historical subscription should persist");
        assert_eq!(
            store
                .claim_deliveries(ClaimDeliveries {
                    worker_id: "history".into(),
                    now_ms: 6,
                    lease_until_ms: 100,
                    limit: 10
                })
                .await
                .expect("history should claim")
                .len(),
            1
        );

        let timer_event = published(EventId::new(), "timer.fired", 20);
        let timer = store
            .schedule_once(ScheduleOnce {
                timer_id: TimerId::new(),
                fire_at_ms: 20,
                event: PublishEvent {
                    source: EventSource::Timer,
                    ..timer_event
                },
                created_at_ms: 1,
            })
            .await
            .expect("timer should persist");
        drop(store);
        let reopened = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should reopen");
        let claimed = reopened
            .claim_due_timers(ClaimTimers {
                worker_id: "timer-worker".into(),
                now_ms: 20,
                lease_until_ms: 1_020,
                limit: 10,
            })
            .await
            .expect("timer should claim");
        assert_eq!(claimed.len(), 1);
        let result = reopened
            .publish(claimed[0].definition.event.clone())
            .await
            .expect("timer event should publish");
        reopened
            .complete_timer(
                timer.definition.timer_id,
                "timer-worker".into(),
                result.event.event.event_id,
            )
            .await
            .expect("timer should complete");
    }

    #[tokio::test]
    async fn suspended_flow_wakes_and_resumes_after_store_reopen() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("events.sqlite3");
        let run_id = RunId::new();
        let checkpoint = |step| CheckpointEnvelope {
            agent_kind: "test-machine".into(),
            schema_version: 1,
            codec: CheckpointCodec::Json,
            payload: json!({"step": step}),
        };
        let store = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should open");
        store
            .create(FlowRunState {
                run_id,
                revision: 0,
                status: FlowRunStatus::Runnable,
                activation_id: None,
                checkpoint: checkpoint(0),
                wait_subscription_ids: Vec::new(),
                lease_owner: None,
                lease_until_ms: None,
                updated_at_ms: 1,
            })
            .await
            .expect("flow should persist");
        let claimed = store
            .claim_runnable("flow-worker".into(), 2, 1_002, 1)
            .await
            .expect("flow should claim");
        let activation = claimed.into_iter().next().expect("one flow should claim");
        let activation_id = activation
            .state
            .activation_id
            .expect("claimed flow should have an activation");
        let subscription_id = SubscriptionId::new();
        store
            .suspend(SuspendFlowRun {
                run_id,
                activation_id,
                expected_revision: activation.state.revision,
                checkpoint: checkpoint(1),
                waits: vec![WaitSpec {
                    wait_key: "approval".into(),
                    subscription: CreateSubscription {
                        subscription_id,
                        owner: SubscriptionOwner::Run { run_id },
                        scope: SubscriptionScope::Run { run_id },
                        filter: EventFilter {
                            topics: vec!["approval.resolved".into()],
                            correlation_id: Some("approval-1".into()),
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
                        created_at_ms: 3,
                    },
                }],
                effects: Vec::new(),
                suspended_at_ms: 3,
            })
            .await
            .expect("flow should suspend");
        drop(store);

        let reopened = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should reopen");
        let event = PublishEvent {
            subject: Some(format!("run/{run_id}")),
            ..published(EventId::new(), "approval.resolved", 4)
        };
        let published = reopened
            .publish(event)
            .await
            .expect("wake event should publish");
        assert_eq!(published.delivery_ids.len(), 1);
        let delivery = reopened
            .claim_deliveries(ClaimDeliveries {
                worker_id: "event-worker".into(),
                now_ms: 4,
                lease_until_ms: 1_004,
                limit: 1,
            })
            .await
            .expect("wake delivery should claim")
            .into_iter()
            .next()
            .expect("one wake delivery should exist");
        reopened
            .wake(WakeFlowRun {
                run_id,
                subscription_id: delivery.delivery_id.subscription_id,
                event_id: delivery.delivery_id.event_id,
                woken_at_ms: 4,
            })
            .await
            .expect("event should wake flow");
        reopened
            .complete_delivery(CompleteDelivery {
                delivery_id: delivery.delivery_id,
                worker_id: "event-worker".into(),
                delivered_at_ms: 4,
            })
            .await
            .expect("wake delivery should complete");

        let resumed = reopened
            .claim_runnable("flow-worker-2".into(), 5, 1_005, 1)
            .await
            .expect("woken flow should claim")
            .into_iter()
            .next()
            .expect("woken flow should be runnable");
        assert_eq!(resumed.inbox.len(), 1);
        assert_eq!(resumed.inbox[0].event.event.topic, "approval.resolved");
        let resumed_activation = resumed
            .state
            .activation_id
            .expect("resumed flow should have an activation");
        let completed = reopened
            .complete(CompleteFlowRun {
                run_id,
                activation_id: resumed_activation,
                expected_revision: resumed.state.revision,
                status: FlowRunStatus::Completed,
                completed_at_ms: 6,
            })
            .await
            .expect("resumed flow should complete");
        assert_eq!(completed.status, FlowRunStatus::Completed);
        assert!(
            reopened
                .claim_runnable("flow-worker-3".into(), 7, 1_007, 1)
                .await
                .expect("terminal flow query should succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn flow_effect_outbox_reclaims_retries_and_completes_after_reopen() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("events.sqlite3");
        let run_id = RunId::new();
        let checkpoint = CheckpointEnvelope {
            agent_kind: "test-machine".into(),
            schema_version: 1,
            codec: CheckpointCodec::Json,
            payload: json!({"step": 0}),
        };
        let store = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should open");
        store
            .create(FlowRunState {
                run_id,
                revision: 0,
                status: FlowRunStatus::Runnable,
                activation_id: None,
                checkpoint: checkpoint.clone(),
                wait_subscription_ids: Vec::new(),
                lease_owner: None,
                lease_until_ms: None,
                updated_at_ms: 1,
            })
            .await
            .expect("flow should persist");
        let activation = store
            .claim_runnable("flow-worker".into(), 2, 1_002, 1)
            .await
            .expect("flow should claim")
            .into_iter()
            .next()
            .expect("one flow should claim");
        store
            .continue_run(ContinueFlowRun {
                run_id,
                activation_id: activation
                    .state
                    .activation_id
                    .expect("activation should exist"),
                expected_revision: activation.state.revision,
                checkpoint,
                effects: vec![EffectRequest::PublishEvent {
                    command: PublishEvent {
                        source: EventSource::Agent,
                        subject: Some(format!("run/{run_id}")),
                        ..published(EventId::new(), "agent.effect", 3)
                    },
                }],
                continued_at_ms: 3,
            })
            .await
            .expect("continuation and effect should commit atomically");
        let first = store
            .claim_effects("effect-worker-1".into(), 3, 4, 1)
            .await
            .expect("effect should claim")
            .into_iter()
            .next()
            .expect("one effect should exist");
        assert_eq!(first.attempts, 1);
        drop(store);

        let reopened = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should reopen");
        let reclaimed = reopened
            .claim_effects("effect-worker-2".into(), 5, 10, 1)
            .await
            .expect("expired effect lease should reclaim")
            .into_iter()
            .next()
            .expect("one reclaimed effect should exist");
        assert_eq!(reclaimed.effect_id, first.effect_id);
        assert_eq!(reclaimed.attempts, 2);
        let retried = reopened
            .retry_effect(RetryFlowEffect {
                effect_id: reclaimed.effect_id,
                worker_id: "effect-worker-2".into(),
                next_attempt_at_ms: 20,
                error: "temporary outage".into(),
                dead_letter: false,
            })
            .await
            .expect("effect should schedule a retry");
        assert_eq!(retried.status, FlowEffectStatus::RetryPending);
        assert!(
            reopened
                .claim_effects("effect-worker-3".into(), 19, 30, 1)
                .await
                .expect("early retry query should succeed")
                .is_empty()
        );
        let retry = reopened
            .claim_effects("effect-worker-3".into(), 20, 30, 1)
            .await
            .expect("due retry should claim")
            .into_iter()
            .next()
            .expect("one due retry should exist");
        let completed = reopened
            .complete_effect(CompleteFlowEffect {
                effect_id: retry.effect_id,
                worker_id: "effect-worker-3".into(),
                completed_at_ms: 21,
            })
            .await
            .expect("effect should complete");
        assert_eq!(completed.status, FlowEffectStatus::Completed);
        assert!(
            reopened
                .claim_effects("effect-worker-4".into(), 100, 200, 1)
                .await
                .expect("completed effect query should succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn expired_delivery_lease_is_reclaimed_after_reopen() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("events.sqlite3");
        let store = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should open");
        store
            .subscribe(subscription(StartPosition::Now))
            .await
            .expect("subscription should persist");
        store
            .publish(published(EventId::new(), "approval.resolved", 2))
            .await
            .expect("event should publish");
        let first = store
            .claim_deliveries(ClaimDeliveries {
                worker_id: "event-worker-1".into(),
                now_ms: 2,
                lease_until_ms: 3,
                limit: 1,
            })
            .await
            .expect("delivery should claim")
            .into_iter()
            .next()
            .expect("one delivery should exist");
        drop(store);

        let reopened = SqliteEventStore::open(&path, "events:test")
            .await
            .expect("store should reopen");
        let reclaimed = reopened
            .claim_deliveries(ClaimDeliveries {
                worker_id: "event-worker-2".into(),
                now_ms: 4,
                lease_until_ms: 10,
                limit: 1,
            })
            .await
            .expect("expired delivery should reclaim")
            .into_iter()
            .next()
            .expect("one reclaimed delivery should exist");
        assert_eq!(reclaimed.delivery_id, first.delivery_id);
        assert_eq!(reclaimed.attempts, 2);
    }
}
