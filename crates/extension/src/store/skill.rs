use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use agent_core::skill::{
    ComponentDescriptor, DeleteSkillPackage, PutSkillPackage, SkillDescriptor, SkillFuture,
    SkillLocator, SkillManifest, SkillPackage, SkillPackageRef, SkillStore, SkillStoreCapabilities,
    SkillStoreError, SkillStoreQuery,
};
use sha2::{Digest, Sha256};

const MAX_PACKAGE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RESOURCE_DEPTH: usize = 8;

#[derive(Debug, Clone)]
pub struct FilesystemSkillStore {
    root: PathBuf,
    identity: String,
}

impl FilesystemSkillStore {
    pub fn new(root: impl Into<PathBuf>, identity: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            identity: identity.into(),
        }
    }

    fn package_directories(&self) -> Result<Vec<PathBuf>, SkillStoreError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(&self.root)
            .map_err(|error| SkillStoreError::backend(format!("read skill root: {error}")))?;
        let mut directories = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|error| SkillStoreError::backend(format!("read skill entry: {error}")))?;
            let path = entry.path();
            if path.is_dir() && path.join("skill.toml").is_file() {
                directories.push(path);
            }
        }
        directories.sort();
        Ok(directories)
    }

    fn load_directory(&self, directory: &Path) -> Result<SkillPackage, SkillStoreError> {
        ensure_under_root(&self.root, directory)?;
        let manifest_bytes = read_limited(&directory.join("skill.toml"), MAX_PACKAGE_BYTES)?;
        let manifest: SkillManifest = toml::from_str(
            std::str::from_utf8(&manifest_bytes)
                .map_err(|_| SkillStoreError::InvalidPackage("skill.toml is not UTF-8".into()))?,
        )
        .map_err(|error| SkillStoreError::InvalidPackage(format!("invalid skill.toml: {error}")))?;
        let instruction_bytes = read_limited(&directory.join("SKILL.md"), MAX_PACKAGE_BYTES)?;
        let instructions = String::from_utf8(instruction_bytes.clone())
            .map_err(|_| SkillStoreError::InvalidPackage("SKILL.md is not UTF-8".into()))?;
        if instructions.trim().is_empty() {
            return Err(SkillStoreError::InvalidPackage(
                "SKILL.md must not be empty".into(),
            ));
        }

        let mut resources = BTreeMap::new();
        let mut total =
            u64::try_from(manifest_bytes.len() + instruction_bytes.len()).unwrap_or(u64::MAX);
        collect_resources(directory, directory, &mut resources, &mut total, 0)?;
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
        for declared in &manifest.resources {
            let normalized = normalized_relative_path(Path::new(&declared.path))?;
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
}

impl SkillStore for FilesystemSkillStore {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor {
            identity: self.identity.clone(),
            kind: "filesystem".into(),
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
            for directory in self.package_directories()? {
                let package = self.load_directory(&directory)?;
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
            for directory in self.package_directories()? {
                let package = self.load_directory(&directory)?;
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

fn collect_resources(
    package_root: &Path,
    directory: &Path,
    resources: &mut BTreeMap<String, Vec<u8>>,
    total: &mut u64,
    depth: usize,
) -> Result<(), SkillStoreError> {
    if depth > MAX_RESOURCE_DEPTH {
        return Err(SkillStoreError::InvalidPackage(
            "resource nesting is too deep".into(),
        ));
    }
    for entry in fs::read_dir(directory)
        .map_err(|error| SkillStoreError::backend(format!("read package directory: {error}")))?
    {
        let entry = entry
            .map_err(|error| SkillStoreError::backend(format!("read package entry: {error}")))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| SkillStoreError::backend(format!("read package metadata: {error}")))?;
        if metadata.file_type().is_symlink() {
            return Err(SkillStoreError::UnsafePath);
        }
        if metadata.is_dir() {
            collect_resources(package_root, &path, resources, total, depth + 1)?;
            continue;
        }
        let relative = path
            .strip_prefix(package_root)
            .map_err(|_| SkillStoreError::UnsafePath)?;
        let relative = normalized_relative_path(relative)?;
        if relative == "skill.toml" || relative == "SKILL.md" {
            continue;
        }
        *total = total.saturating_add(metadata.len());
        if *total > MAX_PACKAGE_BYTES {
            return Err(SkillStoreError::InvalidPackage(
                "skill package exceeds size limit".into(),
            ));
        }
        resources.insert(relative, read_limited(&path, MAX_PACKAGE_BYTES)?);
    }
    Ok(())
}

fn normalized_relative_path(path: &Path) -> Result<String, SkillStoreError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            _ => return Err(SkillStoreError::UnsafePath),
        }
    }
    Ok(parts.join("/"))
}

fn ensure_under_root(root: &Path, path: &Path) -> Result<(), SkillStoreError> {
    let root = root
        .canonicalize()
        .map_err(|error| SkillStoreError::backend(format!("canonicalize skill root: {error}")))?;
    let path = path
        .canonicalize()
        .map_err(|error| SkillStoreError::backend(format!("canonicalize skill path: {error}")))?;
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(SkillStoreError::UnsafePath)
    }
}

fn read_limited(path: &Path, limit: u64) -> Result<Vec<u8>, SkillStoreError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        SkillStoreError::InvalidPackage(format!("missing {}: {error}", path.display()))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(SkillStoreError::UnsafePath);
    }
    if metadata.len() > limit {
        return Err(SkillStoreError::InvalidPackage(format!(
            "{} exceeds size limit",
            path.display()
        )));
    }
    fs::read(path)
        .map_err(|error| SkillStoreError::backend(format!("read {}: {error}", path.display())))
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

#[cfg(test)]
mod tests {
    use agent_core::skill::{SkillId, SkillLocator, SkillStore, SkillStoreQuery};
    use tempfile::tempdir;

    use super::*;

    fn create_skill(root: &Path) {
        let package = root.join("rust-helper");
        fs::create_dir_all(package.join("references"))
            .expect("skill directories should be created");
        fs::write(
            package.join("skill.toml"),
            r#"
skill_id = "rust-helper"
version = "1.0.0"
description = "Helps with Rust tasks"
required_tools = ["read_text_file"]
activation = { type = "routable", hints = ["rust"] }
"#,
        )
        .expect("manifest should be written");
        fs::write(package.join("SKILL.md"), "Use Rust conventions.")
            .expect("instructions should be written");
        fs::write(
            package.join("references/guide.md"),
            "Prefer explicit errors.",
        )
        .expect("resource should be written");
    }

    #[tokio::test]
    async fn filesystem_store_lists_and_loads_digest_locked_packages() {
        let directory = tempdir().expect("temporary directory should exist");
        create_skill(directory.path());
        let store = FilesystemSkillStore::new(directory.path(), "test-skills");

        let listed = store
            .list(SkillStoreQuery::default())
            .await
            .expect("list should succeed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].skill_id, SkillId("rust-helper".into()));
        let package = store
            .get(SkillLocator::Locked {
                skill_id: listed[0].skill_id.clone(),
                version: listed[0].version.clone(),
                digest: listed[0].digest.clone(),
            })
            .await
            .expect("locked get should succeed")
            .expect("package should exist");
        assert_eq!(package.store_identity, "test-skills");
        assert!(package.resources.contains_key("references/guide.md"));

        let mismatch = store
            .get(SkillLocator::Locked {
                skill_id: SkillId("rust-helper".into()),
                version: "1.0.0".into(),
                digest: "sha256:not-the-package".into(),
            })
            .await
            .expect("digest mismatch is a clean miss");
        assert!(mismatch.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_store_rejects_symlinked_resources() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().expect("temporary directory should exist");
        create_skill(directory.path());
        symlink(
            directory.path().join("rust-helper/SKILL.md"),
            directory.path().join("rust-helper/references/link.md"),
        )
        .expect("symlink should be created");
        let store = FilesystemSkillStore::new(directory.path(), "test-skills");
        let error = store
            .list(SkillStoreQuery::default())
            .await
            .expect_err("symlink should be rejected");
        assert!(matches!(error, SkillStoreError::UnsafePath));
    }
}
