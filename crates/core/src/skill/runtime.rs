use std::{collections::BTreeSet, sync::Arc};

use super::{
    ComponentDescriptor, SkillActivation, SkillDescriptor, SkillId, SkillLocator, SkillPackage,
    SkillStore, SkillStoreError, SkillStoreQuery,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedSkill {
    pub skill_id: SkillId,
    pub version: String,
    pub digest: String,
    pub store_identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillLock {
    pub skills: Vec<LockedSkill>,
    pub compiler_version: u32,
    pub compiled_instruction_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSkill {
    pub locked: LockedSkill,
    pub instruction: String,
    pub requested_tools: Vec<String>,
    pub activation_reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResolveSkillsRequest {
    pub explicit: Vec<(SkillId, String)>,
    pub profile_defaults: Vec<(SkillId, String)>,
    pub current_input: String,
    pub max_skills: usize,
}

pub struct SkillOrchestrator {
    store: Arc<dyn SkillStore>,
}

impl SkillOrchestrator {
    pub fn new(store: Arc<dyn SkillStore>) -> Self {
        Self { store }
    }

    #[must_use]
    pub fn descriptor(&self) -> ComponentDescriptor {
        self.store.descriptor()
    }

    pub async fn resolve(
        &self,
        request: ResolveSkillsRequest,
    ) -> Result<Vec<ResolvedSkill>, SkillRuntimeError> {
        let mut selected = Vec::new();
        let mut seen = BTreeSet::new();
        for (skill_id, version) in request.explicit {
            if seen.insert((skill_id.clone(), version.clone())) {
                selected.push((skill_id, version, "explicit".to_owned()));
            }
        }
        for (skill_id, version) in request.profile_defaults {
            if seen.insert((skill_id.clone(), version.clone())) {
                selected.push((skill_id, version, "profile_default".to_owned()));
            }
        }

        if selected.len() < request.max_skills {
            let input = request.current_input.to_lowercase();
            for descriptor in self.store.list(SkillStoreQuery::default()).await? {
                let SkillActivation::Routable { hints } = &descriptor.activation else {
                    continue;
                };
                if hints
                    .iter()
                    .any(|hint| input.contains(&hint.to_lowercase()))
                    && seen.insert((descriptor.skill_id.clone(), descriptor.version.clone()))
                {
                    selected.push((
                        descriptor.skill_id,
                        descriptor.version,
                        "rule_router".to_owned(),
                    ));
                    if selected.len() >= request.max_skills {
                        break;
                    }
                }
            }
        }

        if selected.len() > request.max_skills {
            return Err(SkillRuntimeError::BudgetExceeded);
        }
        let mut resolved = Vec::new();
        for (skill_id, version, activation_reason) in selected {
            let package = self
                .store
                .get(SkillLocator::IdVersion {
                    skill_id: skill_id.clone(),
                    version: version.clone(),
                })
                .await?
                .ok_or_else(|| SkillRuntimeError::NotFound(skill_id.clone(), version.clone()))?;
            resolved.push(resolve_package(package, activation_reason));
        }
        Ok(resolved)
    }

    pub async fn load_locked(
        &self,
        locked: &LockedSkill,
    ) -> Result<SkillPackage, SkillRuntimeError> {
        self.store
            .get(SkillLocator::Locked {
                skill_id: locked.skill_id.clone(),
                version: locked.version.clone(),
                digest: locked.digest.clone(),
            })
            .await?
            .ok_or(SkillRuntimeError::LockedArtifactMissing)
    }

    pub async fn list(&self) -> Result<Vec<SkillDescriptor>, SkillRuntimeError> {
        self.store
            .list(SkillStoreQuery::default())
            .await
            .map_err(Into::into)
    }
}

fn resolve_package(package: SkillPackage, activation_reason: String) -> ResolvedSkill {
    let mut requested_tools = package.manifest.required_tools.clone();
    requested_tools.extend(package.manifest.optional_tools.clone());
    requested_tools.sort();
    requested_tools.dedup();
    ResolvedSkill {
        locked: LockedSkill {
            skill_id: package.manifest.skill_id,
            version: package.manifest.version,
            digest: package.digest,
            store_identity: package.store_identity,
        },
        instruction: package.instructions,
        requested_tools,
        activation_reason,
    }
}

#[derive(Debug, Error)]
pub enum SkillRuntimeError {
    #[error(transparent)]
    Store(#[from] SkillStoreError),
    #[error("skill {0:?}@{1} was not found")]
    NotFound(SkillId, String),
    #[error("locked skill artifact is unavailable or has a different digest")]
    LockedArtifactMissing,
    #[error("selected skills exceed the configured budget")]
    BudgetExceeded,
}
