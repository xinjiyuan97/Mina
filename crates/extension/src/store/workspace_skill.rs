use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use agent_core::skill::{
    ComponentDescriptor, DeleteSkillPackage, PutSkillPackage, SkillDescriptor, SkillFuture,
    SkillLocator, SkillManifest, SkillPackage, SkillPackageRef, SkillStore, SkillStoreCapabilities,
    SkillStoreError, SkillStoreQuery,
};
use sha2::{Digest, Sha256};

use crate::workspace::{
    DirectoryEntry, FileKind, ListRequest, ReadRequest, WorkspaceError, WorkspaceErrorKind,
    WorkspaceFs, WorkspacePath,
};

const MAX_PACKAGE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RESOURCE_DEPTH: usize = 8;
const MAX_DISCOVERY_DEPTH: usize = 6;
const MAX_DISCOVERY_ENTRIES: usize = 10_000;
const MAX_PACKAGES: usize = 128;
const PAGE_SIZE: usize = 256;
const PENDING_INSTALL_MARKER: &str = ".mina-skill-pending";

/// Read-only SkillStore over a portable WorkspaceFs. Browser builds mount an
/// OpfsWorkspaceFs below a dedicated prefix; native tests can use the same
/// package validation and digest semantics with NativeWorkspaceFs.
pub struct WorkspaceSkillStore {
    workspace: Arc<dyn WorkspaceFs>,
    identity: String,
}

impl WorkspaceSkillStore {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>, identity: impl Into<String>) -> Self {
        Self {
            workspace,
            identity: identity.into(),
        }
    }

    async fn package_directories(&self) -> Result<Vec<WorkspacePath>, SkillStoreError> {
        let mut pending = VecDeque::from([(WorkspacePath::root(), 0_usize)]);
        let mut packages = Vec::new();
        let mut discovered = 0_usize;
        while let Some((directory, depth)) = pending.pop_front() {
            let entries = match self.list_all(directory.clone()).await {
                Ok(entries) => entries,
                Err(error)
                    if directory.is_root() && matches!(error, WorkspaceReadError::NotFound) =>
                {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error.into_skill_error()),
            };
            discovered = discovered.saturating_add(entries.len());
            if discovered > MAX_DISCOVERY_ENTRIES {
                return Err(SkillStoreError::InvalidPackage(
                    "skill store contains too many entries".into(),
                ));
            }
            let has_manifest = entries.iter().any(|entry| {
                entry.kind == FileKind::File && entry.path.file_name() == Some("skill.toml")
            });
            let has_instruction = entries.iter().any(|entry| {
                entry.kind == FileKind::File && entry.path.file_name() == Some("SKILL.md")
            });
            let pending_install = entries.iter().any(|entry| {
                entry.kind == FileKind::File
                    && entry.path.file_name() == Some(PENDING_INSTALL_MARKER)
            });
            if pending_install {
                continue;
            }
            if has_manifest || has_instruction {
                if !has_manifest || !has_instruction {
                    return Err(SkillStoreError::InvalidPackage(format!(
                        "{} must contain both skill.toml and SKILL.md",
                        directory
                    )));
                }
                packages.push(directory);
                if packages.len() > MAX_PACKAGES {
                    return Err(SkillStoreError::InvalidPackage(
                        "skill store contains too many packages".into(),
                    ));
                }
                continue;
            }
            if depth >= MAX_DISCOVERY_DEPTH {
                continue;
            }
            for entry in entries {
                if entry.kind == FileKind::Directory
                    && !entry
                        .path
                        .file_name()
                        .is_some_and(|name| name.starts_with(".installing-"))
                {
                    pending.push_back((entry.path, depth + 1));
                }
            }
        }
        packages.sort();
        Ok(packages)
    }

    async fn load_directory(
        &self,
        directory: &WorkspacePath,
    ) -> Result<SkillPackage, SkillStoreError> {
        let manifest_path = directory
            .join("skill.toml")
            .map_err(|_| SkillStoreError::UnsafePath)?;
        let instruction_path = directory
            .join("SKILL.md")
            .map_err(|_| SkillStoreError::UnsafePath)?;
        let manifest_bytes = self.read_limited(manifest_path).await?;
        let manifest: SkillManifest = toml::from_str(
            std::str::from_utf8(&manifest_bytes)
                .map_err(|_| SkillStoreError::InvalidPackage("skill.toml is not UTF-8".into()))?,
        )
        .map_err(|error| SkillStoreError::InvalidPackage(format!("invalid skill.toml: {error}")))?;
        let instruction_bytes = self.read_limited(instruction_path).await?;
        let instructions = String::from_utf8(instruction_bytes.clone())
            .map_err(|_| SkillStoreError::InvalidPackage("SKILL.md is not UTF-8".into()))?;
        if instructions.trim().is_empty() {
            return Err(SkillStoreError::InvalidPackage(
                "SKILL.md must not be empty".into(),
            ));
        }
        validate_manifest(&manifest)?;

        let mut resources = BTreeMap::new();
        let mut total =
            u64::try_from(manifest_bytes.len() + instruction_bytes.len()).unwrap_or(u64::MAX);
        self.collect_resources(directory, &mut resources, &mut total)
            .await?;
        for declared in &manifest.resources {
            let normalized = normalize_resource_path(&declared.path)?;
            if !resources.contains_key(&normalized) {
                return Err(SkillStoreError::InvalidPackage(format!(
                    "declared resource {normalized} does not exist"
                )));
            }
        }
        let digest = package_digest(&manifest_bytes, &instruction_bytes, &resources);
        Ok(SkillPackage {
            manifest,
            instructions,
            resources,
            digest,
            store_identity: self.identity.clone(),
        })
    }

    async fn collect_resources(
        &self,
        package_root: &WorkspacePath,
        resources: &mut BTreeMap<String, Vec<u8>>,
        total: &mut u64,
    ) -> Result<(), SkillStoreError> {
        let mut pending = VecDeque::from([(package_root.clone(), 0_usize)]);
        while let Some((directory, depth)) = pending.pop_front() {
            if depth > MAX_RESOURCE_DEPTH {
                return Err(SkillStoreError::InvalidPackage(
                    "resource nesting is too deep".into(),
                ));
            }
            let entries = self
                .list_all(directory)
                .await
                .map_err(WorkspaceReadError::into_skill_error)?;
            for entry in entries {
                match entry.kind {
                    FileKind::Directory => pending.push_back((entry.path, depth + 1)),
                    FileKind::File => {
                        let relative = relative_path(package_root, &entry.path)?;
                        if relative == "skill.toml" || relative == "SKILL.md" {
                            continue;
                        }
                        let bytes = self.read_limited(entry.path).await?;
                        *total = total.saturating_add(bytes.len() as u64);
                        if *total > MAX_PACKAGE_BYTES {
                            return Err(SkillStoreError::InvalidPackage(
                                "skill package exceeds size limit".into(),
                            ));
                        }
                        resources.insert(relative, bytes);
                    }
                    FileKind::Symlink | FileKind::Other => {
                        return Err(SkillStoreError::UnsafePath);
                    }
                }
            }
        }
        Ok(())
    }

    async fn list_all(
        &self,
        path: WorkspacePath,
    ) -> Result<Vec<DirectoryEntry>, WorkspaceReadError> {
        let mut cursor = None;
        let mut entries = Vec::new();
        loop {
            let page = self
                .workspace
                .list(ListRequest {
                    path: path.clone(),
                    cursor,
                    limit: PAGE_SIZE,
                })
                .await
                .map_err(WorkspaceReadError::from)?;
            entries.extend(page.entries);
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
            if entries.len() > MAX_DISCOVERY_ENTRIES {
                return Err(WorkspaceReadError::Backend(
                    "workspace directory contains too many entries".into(),
                ));
            }
        }
        Ok(entries)
    }

    async fn read_limited(&self, path: WorkspacePath) -> Result<Vec<u8>, SkillStoreError> {
        self.workspace
            .read(ReadRequest {
                path,
                offset: 0,
                length: None,
                max_bytes: MAX_PACKAGE_BYTES,
            })
            .await
            .map(|content| content.bytes)
            .map_err(|error| match error.kind() {
                WorkspaceErrorKind::InvalidPath => SkillStoreError::UnsafePath,
                WorkspaceErrorKind::NotFound => {
                    SkillStoreError::InvalidPackage("skill package file is missing".into())
                }
                WorkspaceErrorKind::TooLarge => {
                    SkillStoreError::InvalidPackage("skill package exceeds size limit".into())
                }
                _ => SkillStoreError::backend(error.safe_message()),
            })
    }
}

impl SkillStore for WorkspaceSkillStore {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor {
            identity: self.identity.clone(),
            kind: "workspace".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn capabilities(&self) -> SkillStoreCapabilities {
        SkillStoreCapabilities {
            readable: true,
            writable: false,
            deletable: false,
        }
    }

    fn list(&self, query: SkillStoreQuery) -> SkillFuture<'_, Vec<SkillDescriptor>> {
        Box::pin(async move {
            let mut descriptors = Vec::new();
            for directory in self.package_directories().await? {
                let package = self.load_directory(&directory).await?;
                if !query.skill_ids.is_empty()
                    && !query.skill_ids.contains(&package.manifest.skill_id)
                {
                    continue;
                }
                descriptors.push(SkillDescriptor {
                    skill_id: package.manifest.skill_id,
                    version: package.manifest.version,
                    description: package.manifest.description,
                    digest: package.digest,
                    store_identity: package.store_identity,
                    activation: package.manifest.activation,
                });
            }
            descriptors.sort_by(|left, right| {
                (&left.skill_id, &left.version, &left.digest).cmp(&(
                    &right.skill_id,
                    &right.version,
                    &right.digest,
                ))
            });
            Ok(descriptors)
        })
    }

    fn get(&self, locator: SkillLocator) -> SkillFuture<'_, Option<SkillPackage>> {
        Box::pin(async move {
            let (wanted_id, wanted_version, wanted_digest) = match locator {
                SkillLocator::IdVersion { skill_id, version } => (skill_id, version, None),
                SkillLocator::Locked {
                    skill_id,
                    version,
                    digest,
                } => (skill_id, version, Some(digest)),
            };
            let mut found = None;
            for directory in self.package_directories().await? {
                let package = self.load_directory(&directory).await?;
                if package.manifest.skill_id == wanted_id
                    && package.manifest.version == wanted_version
                {
                    if wanted_digest
                        .as_ref()
                        .is_some_and(|digest| digest != &package.digest)
                    {
                        continue;
                    }
                    if found.is_some() {
                        return Err(SkillStoreError::DigestConflict);
                    }
                    found = Some(package);
                }
            }
            Ok(found)
        })
    }

    fn put(&self, _command: PutSkillPackage) -> SkillFuture<'_, SkillPackageRef> {
        Box::pin(async { Err(SkillStoreError::ReadOnly) })
    }

    fn delete(&self, _command: DeleteSkillPackage) -> SkillFuture<'_, ()> {
        Box::pin(async { Err(SkillStoreError::ReadOnly) })
    }
}

enum WorkspaceReadError {
    NotFound,
    Backend(String),
}

impl WorkspaceReadError {
    fn into_skill_error(self) -> SkillStoreError {
        match self {
            Self::NotFound => SkillStoreError::InvalidPackage("skill directory is missing".into()),
            Self::Backend(message) => SkillStoreError::backend(message),
        }
    }
}

impl From<WorkspaceError> for WorkspaceReadError {
    fn from(error: WorkspaceError) -> Self {
        if error.kind() == WorkspaceErrorKind::NotFound {
            Self::NotFound
        } else {
            Self::Backend(error.safe_message().into())
        }
    }
}

fn validate_manifest(manifest: &SkillManifest) -> Result<(), SkillStoreError> {
    if manifest.skill_id.0.trim().is_empty()
        || manifest.version.trim().is_empty()
        || manifest.description.trim().is_empty()
    {
        return Err(SkillStoreError::InvalidPackage(
            "skill id, version, and description must not be empty".into(),
        ));
    }
    if manifest.compatibility.contract_version != 1 {
        return Err(SkillStoreError::InvalidPackage(
            "unsupported skill contract version".into(),
        ));
    }
    Ok(())
}

fn normalize_resource_path(path: &str) -> Result<String, SkillStoreError> {
    let path = WorkspacePath::parse(path).map_err(|_| SkillStoreError::UnsafePath)?;
    if path.is_root() {
        return Err(SkillStoreError::UnsafePath);
    }
    Ok(path.storage_key().to_owned())
}

fn relative_path(
    package_root: &WorkspacePath,
    path: &WorkspacePath,
) -> Result<String, SkillStoreError> {
    let root = package_root.storage_key();
    let source = path.storage_key();
    let relative = if root.is_empty() {
        source
    } else {
        source
            .strip_prefix(root)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or(SkillStoreError::UnsafePath)?
    };
    normalize_resource_path(relative)
}

fn package_digest(
    manifest: &[u8],
    instructions: &[u8],
    resources: &BTreeMap<String, Vec<u8>>,
) -> String {
    let mut hasher = Sha256::new();
    digest_entry(&mut hasher, "skill.toml", manifest);
    digest_entry(&mut hasher, "SKILL.md", instructions);
    for (path, bytes) in resources {
        digest_entry(&mut hasher, path, bytes);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn digest_entry(hasher: &mut Sha256, path: &str, bytes: &[u8]) {
    hasher.update(path.len().to_le_bytes());
    hasher.update(path.as_bytes());
    hasher.update(bytes.len().to_le_bytes());
    hasher.update(bytes);
}

#[cfg(all(test, feature = "workspace-native"))]
mod tests {
    use std::{fs, path::Path, sync::Arc};

    use agent_core::skill::{
        ResolveSkillsRequest, SkillId, SkillLocator, SkillOrchestrator, SkillStore, SkillStoreQuery,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::workspace::NativeWorkspaceFs;

    fn create_skill(root: &Path) {
        create_skill_at(&root.join("browser-helper"));
    }

    fn create_skill_at(package: &Path) {
        fs::create_dir_all(package.join("references")).expect("create skill directories");
        fs::write(
            package.join("skill.toml"),
            r#"
skill_id = "browser-helper"
version = "1.0.0"
description = "Helps with browser Skill tests"
required_tools = ["read_skill_resource"]
activation = { type = "routable", hints = ["skill-test"] }
resources = [{ path = "references/guide.md", media_type = "text/markdown" }]
"#,
        )
        .expect("write manifest");
        fs::write(package.join("SKILL.md"), "Follow the browser Skill guide.")
            .expect("write instructions");
        fs::write(
            package.join("references/guide.md"),
            "Use the locked resource.",
        )
        .expect("write resource");
    }

    fn store(root: &Path) -> Arc<WorkspaceSkillStore> {
        let workspace = NativeWorkspaceFs::new(root).expect("native workspace");
        Arc::new(WorkspaceSkillStore::new(
            Arc::new(workspace),
            "test-opfs-skills",
        ))
    }

    #[tokio::test]
    async fn lists_routes_and_reads_digest_locked_package_resources() {
        let directory = tempdir().expect("temporary skill store");
        create_skill(directory.path());
        let store = store(directory.path());

        let listed = store
            .list(SkillStoreQuery::default())
            .await
            .expect("list skills");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].skill_id, SkillId("browser-helper".into()));

        let orchestrator = SkillOrchestrator::new(store.clone());
        let resolved = orchestrator
            .resolve(ResolveSkillsRequest {
                current_input: "please run the skill-test workflow".into(),
                max_skills: 4,
                ..ResolveSkillsRequest::default()
            })
            .await
            .expect("route skill");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].activation_reason, "rule_router");

        let package = orchestrator
            .load_locked(&resolved[0].locked)
            .await
            .expect("load exact locked package");
        assert_eq!(
            package.resources.get("references/guide.md"),
            Some(&b"Use the locked resource.".to_vec())
        );

        let mismatch = store
            .get(SkillLocator::Locked {
                skill_id: SkillId("browser-helper".into()),
                version: "1.0.0".into(),
                digest: "sha256:not-the-package".into(),
            })
            .await
            .expect("digest mismatch is a clean miss");
        assert!(mismatch.is_none());
    }

    #[tokio::test]
    async fn ignores_staging_and_pending_install_directories() {
        let directory = tempdir().expect("temporary skill store");
        create_skill_at(&directory.path().join(".installing-stale"));
        let pending = directory.path().join("packages/pending");
        create_skill_at(&pending);
        fs::write(pending.join(PENDING_INSTALL_MARKER), "pending").expect("write pending marker");

        let listed = store(directory.path())
            .list(SkillStoreQuery::default())
            .await
            .expect("list ignores incomplete managed installs");
        assert!(listed.is_empty());
    }
}
