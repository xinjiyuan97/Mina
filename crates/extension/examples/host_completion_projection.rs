//! Offline host integration: typed graph bindings and a separate SQLite projection.
//! Run with `cargo run -p agent-extension --features sqlite --example host_completion_projection -- <directory>`.
use agent_core::{agent::*, contracts::host::RunCompletion, contracts::*};
use agent_extension::store::{SqliteEventStore, SqliteOpenOptions};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use serde_json::json;
use std::{path::Path, sync::Arc, time::Duration};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const CONSUMER: &str = "session-finalization-v1";

pub fn open_host(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
         CREATE TABLE IF NOT EXISTS admissions (
           tenant TEXT NOT NULL, run_id TEXT NOT NULL, session_id TEXT NOT NULL,
           PRIMARY KEY(tenant, run_id));
         CREATE TABLE IF NOT EXISTS final_messages (
           tenant TEXT NOT NULL, run_id TEXT NOT NULL, session_id TEXT NOT NULL,
           completion_json TEXT NOT NULL, PRIMARY KEY(tenant, run_id));
         CREATE TABLE IF NOT EXISTS projection_ledger (
           consumer TEXT NOT NULL, completion_id TEXT NOT NULL, completion_json TEXT NOT NULL,
           PRIMARY KEY(consumer, completion_id));",
    )?;
    Ok(connection)
}

/// The host owns this transaction. No Mina call or await occurs while it is open.
/// Unknown admissions and conflicting retries fail without advancing Mina's ACK.
pub fn stage_completion<'a>(
    host: &'a mut Connection,
    completion: &RunCompletion,
) -> Result<Transaction<'a>> {
    if completion.schema_version != 1
        || !matches!(
            completion.status.as_str(),
            "completed" | "failed" | "cancelled"
        )
    {
        return Err("unsupported completion".into());
    }
    let tx = host.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let run_id = completion.run_id.to_string();
    let session: String = tx.query_row(
        "SELECT session_id FROM admissions WHERE tenant=?1 AND run_id=?2",
        params![completion.tenant, run_id],
        |row| row.get(0),
    )?;
    let encoded = serde_json::to_string(completion)?;
    let inserted = tx.execute(
        "INSERT INTO projection_ledger VALUES (?1,?2,?3)
         ON CONFLICT(consumer,completion_id) DO NOTHING",
        params![CONSUMER, completion.completion_id, encoded],
    )?;
    if inserted == 1 {
        tx.execute(
            "INSERT INTO final_messages VALUES (?1,?2,?3,?4)",
            params![completion.tenant, run_id, session, encoded],
        )?;
    } else {
        let existing: String = tx.query_row(
            "SELECT completion_json FROM projection_ledger WHERE consumer=?1 AND completion_id=?2",
            params![CONSUMER, completion.completion_id],
            |row| row.get(0),
        )?;
        if existing != encoded {
            return Err("completion identity reused with different content".into());
        }
    }
    Ok(tx)
}

pub async fn project_pending(manager: &RunManager, host: &mut Connection) -> Result<usize> {
    let mut count = 0;
    loop {
        let offset = manager.completion_offset(CONSUMER).await?;
        let page = manager.list_completions(offset, 32).await?;
        if page.items.is_empty() {
            return Ok(count);
        }
        for (sequence, completion) in page.items {
            stage_completion(host, &completion)?.commit()?;
            // A crash here re-delivers the fact; the host ledger deduplicates it.
            manager
                .acknowledge_completion(CONSUMER, sequence, &completion.completion_id)
                .await?;
            count += 1;
        }
    }
}

struct OfflineModel;
impl ModelPort for OfflineModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(futures_util::stream::iter([
            ModelEvent::TextDelta {
                delta: "offline answer".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let directory = std::env::args_os()
        .nth(1)
        .ok_or("provide an output directory")?;
    let directory = Path::new(&directory);
    std::fs::create_dir_all(directory)?;
    let fabric = Arc::new(
        SqliteEventStore::open_with_options(
            directory.join("mina.db"),
            "host-example",
            SqliteOpenOptions::default(),
        )
        .await?,
    );
    let manager = RunManager::new("local", fabric.clone());
    let mut host = open_host(&directory.join("session.db"))?;
    // On startup, reconcile earlier completions before accepting new work.
    project_pending(&manager, &mut host).await?;
    let bindings = HostBindings {
        contract_id: "mina.assistant".into(),
        version: 1,
        values: [
            ("system_prompt".into(), json!("Be concise.")),
            ("context".into(), json!({"prior_messages":[]})),
            ("model_name".into(), json!("offline")),
            ("max_output_tokens".into(), json!(128)),
        ]
        .into(),
    };
    let policy = HostPolicy {
        allowed_tools: Default::default(),
        allowed_models: ["offline".into()].into(),
        max_steps: 4,
        max_output_tokens: 128,
        timeout_ms: 5000,
        review_level: 0,
        allow_run_adf: false,
    };
    let agent = AgentBuilder::new(
        OperatorRegistry::builtins(),
        AgentResources::new(fabric)
            .with_agent_id("demo-agent")
            .model("primary", "offline-v1", Arc::new(OfflineModel)),
    )
    .build_with_bindings(
        AgentDefinition::from_yaml(include_str!("../../../config/agents/host-bound.yaml"))?,
        bindings,
        policy.clone(),
    )?;
    let run = RunId::new();
    // Persist the host-owned mapping before admitting the same stable RunId.
    host.execute(
        "INSERT INTO admissions VALUES (?1,?2,?3)",
        params!["local", run.to_string(), "demo-session"],
    )?;
    agent.engine.start("local", run, json!("hello")).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if manager.completion(run).await?.is_some() {
                break Ok::<_, agent_core::contracts::fabric::FabricError>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    agent.engine.stop_workers().await;
    let completion = manager.completion(run).await?.ok_or("missing completion")?;
    if completion.status != "completed" {
        return Err(format!("offline run did not complete: {:?}", completion.error).into());
    }
    project_pending(&manager, &mut host).await?;
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let page = manager.inspect_run(run, cursor.as_deref(), 32).await?;
        cursor = page.next_cursor.clone();
        pages.push(page);
        if cursor.is_none() {
            break;
        }
    }
    std::fs::write(
        directory.join("inspection.json"),
        serde_json::to_vec_pretty(&json!({"diagnostic_only":true,"pages":pages}))?,
    )?;
    // A history export requires no maintenance barrier or recovery lease changes.
    let history = manager.export_agent_history("demo-agent").await?;
    history.verify()?;
    std::fs::write(
        directory.join("agent-history.json"),
        serde_json::to_vec_pretty(&history)?,
    )?;
    println!(
        "Projected Run {run}; agent-history.json contains finished Runs only and is not a recovery backup."
    );
    Ok(())
}
