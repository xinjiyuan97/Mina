use super::SqliteEventStore;
use crate::operator_fabric::{FabricState, storage};
use agent_core::operator_fabric::*;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

impl OperatorFabric for SqliteEventStore {
    fn capabilities(&self) -> FabricCapabilities {
        FabricCapabilities {
            durable: true,
            ..crate::operator_fabric::MemoryFabric::default().capabilities()
        }
    }

    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        let connection = self.connection.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut connection = connection.lock().map_err(storage)?;
                transact(&mut connection, command)
            })
            .await
            .map_err(storage)?
        })
    }
}

fn transact(
    connection: &mut Connection,
    command: FabricCommand,
) -> Result<FabricReply, FabricError> {
    let read_only = matches!(
        command,
        FabricCommand::Replay { .. }
            | FabricCommand::Offset { .. }
            | FabricCommand::Receipt(_)
            | FabricCommand::DeadLetters { .. }
    );
    let behavior = if read_only {
        TransactionBehavior::Deferred
    } else {
        TransactionBehavior::Immediate
    };
    let transaction = connection
        .transaction_with_behavior(behavior)
        .map_err(storage)?;
    let encoded: Option<String> = transaction
        .query_row(
            "SELECT state_json FROM workflow_operator_fabric_v1 WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    let mut state: FabricState = match encoded {
        Some(encoded) => serde_json::from_str(&encoded).map_err(storage)?,
        None => FabricState::default(),
    };
    let reply = state.apply(command)?;
    if !read_only {
        let encoded = serde_json::to_string(&state).map_err(storage)?;
        transaction.execute("INSERT INTO workflow_operator_fabric_v1 (singleton, state_json) VALUES (1, ?1) ON CONFLICT(singleton) DO UPDATE SET state_json = excluded.state_json", [encoded]).map_err(storage)?;
    }
    transaction.commit().map_err(storage)?;
    Ok(reply)
}
