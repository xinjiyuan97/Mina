use super::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BlobCollectionReport {
    pub dry_run: bool,
    pub artifacts: usize,
    pub files: usize,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn put(store: &FilesystemBlobStore, value: serde_json::Value) -> BlobId {
        store
            .put(PutBlob {
                media_type: "application/json".into(),
                name: None,
                data: serde_json::to_vec(&value).unwrap(),
                created_at_ms: 0,
            })
            .await
            .unwrap()
            .blob_id
    }

    fn future() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 100_000
    }

    #[tokio::test]
    async fn collection_preserves_chains_and_cleans_only_old_managed_files() {
        let directory = tempfile::tempdir().unwrap();
        let store = FilesystemBlobStore::new(directory.path());
        let old = put(&store, serde_json::json!({"delta":"hello"})).await;
        let head = put(&store, serde_json::json!({"previous":old})).await;
        let orphan = put(&store, serde_json::json!({"unused":true})).await;
        // Filesystem modification times protect even a backdated client upload.
        assert_eq!(
            store
                .collect_artifacts(&[], future() - 100_000, 1000, false)
                .await
                .unwrap()
                .files,
            0
        );
        let partial = BlobId::new();
        let partial_path = store.root.join(format!(".{partial}.blob.tmp"));
        tokio::fs::write(&partial_path, b"partial").await.unwrap();
        let unrelated = store.root.join("keep.txt");
        tokio::fs::write(&unrelated, b"keep").await.unwrap();
        let preview = store
            .collect_artifacts(&[head], future(), 1000, true)
            .await
            .unwrap();
        assert_eq!(preview.artifacts, 2);
        assert_eq!(preview.files, 3);
        assert!(store.get(orphan).await.unwrap().is_some());
        let actual = store
            .collect_artifacts(&[head], future(), 1000, false)
            .await
            .unwrap();
        assert_eq!(actual.files, preview.files);
        assert!(store.get(head).await.unwrap().is_some());
        assert!(store.get(old).await.unwrap().is_some());
        assert!(store.get(orphan).await.unwrap().is_none());
        assert!(!partial_path.exists());
        assert!(unrelated.exists());
        assert_eq!(
            store
                .collect_artifacts(&[head], future(), 1000, false)
                .await
                .unwrap()
                .files,
            0
        );
    }

    #[tokio::test]
    async fn invalid_reachable_object_prevents_all_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let store = FilesystemBlobStore::new(directory.path());
        let broken = put(&store, serde_json::json!({})).await;
        let orphan = put(&store, serde_json::json!({})).await;
        tokio::fs::write(store.data_path(broken), b"!")
            .await
            .unwrap();
        assert!(
            store
                .collect_artifacts(&[broken], future(), 1000, false)
                .await
                .is_err()
        );
        assert!(store.get(orphan).await.unwrap().is_some());
    }
}

fn backend(error: impl std::fmt::Display) -> BlobStoreError {
    BlobStoreError::backend(error.to_string())
}

fn references(value: &serde_json::Value, pending: &mut Vec<BlobId>) {
    match value {
        serde_json::Value::String(s) => {
            if let Ok(id) = s.parse() {
                pending.push(id);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                references(value, pending);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                references(value, pending);
            }
        }
        _ => {}
    }
}

impl FilesystemBlobStore {
    /// Collect old unreferenced objects and interrupted-upload files.
    ///
    /// The host MUST quiesce all writers and root changes (including other store
    /// instances/processes) before taking the roots snapshot, until this returns.
    /// Supply both Fabric ArtifactRoots and session/attachment roots. This method
    /// serializes IO with clones of this store but cannot lock another database.
    /// Planning validates the entire reachable graph before deleting any file.
    /// Filesystem deletion is retryable, not an atomic multi-file transaction.
    pub async fn collect_artifacts(
        &self,
        roots: &[BlobId],
        now_ms: i64,
        grace_ms: i64,
        dry_run: bool,
    ) -> Result<BlobCollectionReport, BlobStoreError> {
        if grace_ms <= 0 {
            return Err(backend("grace must be positive"));
        }
        let _guard = self.maintenance.write().await;
        let mut entries = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BlobCollectionReport {
                    dry_run,
                    ..Default::default()
                });
            }
            Err(e) => return Err(backend(e)),
        };
        let mut objects: HashMap<BlobId, Vec<(PathBuf, u64)>> = HashMap::new();
        let mut pending = roots.to_vec();
        while let Some(entry) = entries.next_entry().await.map_err(backend)? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let stem = name
                .strip_suffix(".blob")
                .or_else(|| name.strip_suffix(".json"))
                .or_else(|| {
                    name.strip_prefix('.')
                        .and_then(|n| n.strip_suffix(".blob.tmp"))
                })
                .or_else(|| {
                    name.strip_prefix('.')
                        .and_then(|n| n.strip_suffix(".json.tmp"))
                });
            let Some(id) = stem.and_then(|s| s.parse::<BlobId>().ok()) else {
                continue;
            };
            // Refuse symlinks and other special files in the managed namespace.
            if !entry.file_type().await.map_err(backend)?.is_file() {
                return Err(backend("non-regular managed blob file"));
            }
            let metadata = entry.metadata().await.map_err(backend)?;
            let modified = metadata
                .modified()
                .map_err(backend)?
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(backend)?
                .as_millis();
            let modified = i64::try_from(modified).map_err(backend)?;
            if now_ms.saturating_sub(modified) < grace_ms {
                pending.push(id);
            }
            objects
                .entry(id)
                .or_default()
                .push((entry.path(), metadata.len()));
        }
        // Client timestamps can also extend retention, never shorten filesystem age.
        let mut metadata = HashMap::new();
        for id in objects.keys() {
            match tokio::fs::read(self.metadata_path(*id)).await {
                Ok(bytes) => {
                    let value: BlobMetadata = serde_json::from_slice(&bytes).map_err(backend)?;
                    if value.blob_id != *id {
                        return Err(backend("blob metadata identity mismatch"));
                    }
                    if now_ms.saturating_sub(value.created_at_ms) < grace_ms {
                        pending.push(*id);
                    }
                    metadata.insert(*id, value);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(backend(e)),
            }
        }
        let mut marked = HashSet::new();
        while let Some(id) = pending.pop() {
            if !marked.insert(id) {
                continue;
            }
            // Roots may belong to another store. A local partial object must fail closed.
            if !objects.contains_key(&id) {
                continue;
            }
            let Some(meta) = metadata.get(&id) else {
                return Err(backend("referenced or young blob incomplete"));
            };
            let bytes = tokio::fs::read(self.data_path(id)).await.map_err(backend)?;
            if bytes.len() as u64 != meta.size_bytes {
                return Err(backend("blob size mismatch"));
            }
            if meta.media_type == "application/json" || meta.media_type.ends_with("+json") {
                let value = serde_json::from_slice(&bytes).map_err(backend)?;
                let mut children = Vec::new();
                references(&value, &mut children);
                pending.extend(children);
            }
        }
        let mut report = BlobCollectionReport {
            dry_run,
            ..Default::default()
        };
        let mut remove = Vec::new();
        for (id, files) in objects {
            if marked.contains(&id) {
                continue;
            }
            report.artifacts += 1;
            for (path, size) in files {
                report.files += 1;
                report.bytes = report.bytes.saturating_add(size);
                remove.push(path);
            }
        }
        if !dry_run {
            for path in remove {
                tokio::fs::remove_file(path).await.map_err(backend)?;
            }
        }
        Ok(report)
    }
}
