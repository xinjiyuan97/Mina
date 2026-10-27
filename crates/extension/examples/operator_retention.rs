use agent_core::{contracts::BlobId, contracts::fabric::*};
use agent_extension::store::SqliteEventStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let path = arguments
        .next()
        .ok_or("usage: operator_retention SNAPSHOT_DB NOW_MS GRACE_MS [ROOTS_JSON]")?;
    if !std::path::Path::new(&path).is_file() {
        return Err("snapshot database does not exist".into());
    }
    let now_ms = arguments.next().ok_or("NOW_MS required")?.parse()?;
    let grace_ms = arguments.next().ok_or("GRACE_MS required")?.parse()?;
    let external_roots: Vec<BlobId> = match arguments.next() {
        Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
        None => Vec::new(),
    };
    if arguments.next().is_some() {
        return Err("unexpected argument".into());
    }
    let store = SqliteEventStore::open(path, "maintenance").await?;
    let FabricReply::Retention(report) = store
        .execute(FabricCommand::InspectRetention {
            now_ms,
            grace_ms,
            external_roots,
        })
        .await?
    else {
        return Err("unexpected response".into());
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
