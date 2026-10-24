use super::tools_pipeline::EngineProfile;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub(super) struct Rollout {
    basis_points: u16,
    allowlist: BTreeSet<String>,
}

impl Default for Rollout {
    fn default() -> Self {
        Self {
            basis_points: 10_000,
            allowlist: BTreeSet::new(),
        }
    }
}

impl Rollout {
    pub fn parse(ratio: Option<&str>, allowlist: Option<&str>) -> Result<Self, String> {
        let basis_points = ratio
            .unwrap_or("10000")
            .parse::<u16>()
            .map_err(|_| "invalid MINA_OPERATOR_BASIS_POINTS".to_string())?;
        if basis_points > 10_000 {
            return Err("invalid MINA_OPERATOR_BASIS_POINTS".into());
        }
        let allowlist: BTreeSet<String> = allowlist
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect();
        if allowlist.len() > 1024 || allowlist.iter().any(|entry| entry.len() > 256) {
            return Err("MINA_OPERATOR_ALLOWLIST exceeds bounds".into());
        }
        Ok(Self {
            basis_points,
            allowlist,
        })
    }

    pub fn configured() -> Result<Self, Box<dyn std::error::Error>> {
        fn optional(name: &str) -> Result<Option<String>, std::env::VarError> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(error) => Err(error),
            }
        }
        Ok(Self::parse(
            optional("MINA_OPERATOR_BASIS_POINTS")?.as_deref(),
            optional("MINA_OPERATOR_ALLOWLIST")?.as_deref(),
        )?)
    }

    pub fn select(&self, configured: EngineProfile, identity: &str) -> EngineProfile {
        let digest = Sha256::digest(format!("mina:rollout:v1:{identity}"));
        let bucket = u64::from_be_bytes(digest[..8].try_into().expect("sha256 prefix")) % 10_000;
        if configured != EngineProfile::Legacy
            && (self.allowlist.contains(identity) || bucket < u64::from(self.basis_points))
        {
            configured
        } else {
            EngineProfile::Legacy
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p5_routing_is_deterministic_and_monotonic() {
        let small = Rollout::parse(Some("1000"), None).expect("config");
        let large = Rollout::parse(Some("5000"), None).expect("config");
        let mut selected = 0;
        for index in 0..10_000 {
            let identity = format!("session-{index}");
            let route = small.select(EngineProfile::Tools, &identity);
            assert_eq!(route, small.select(EngineProfile::Tools, &identity));
            if route == EngineProfile::Tools {
                selected += 1;
                assert_eq!(large.select(EngineProfile::Tools, &identity), route);
            }
        }
        assert!((850..1150).contains(&selected), "selected={selected}");
    }

    #[test]
    fn p5_allowlist_and_rollback_never_enable_implicit_operator() {
        let rollout = Rollout::parse(Some("0"), Some("a,b")).expect("config");
        assert_eq!(
            rollout.select(EngineProfile::Tools, "a"),
            EngineProfile::Tools
        );
        assert_eq!(
            rollout.select(EngineProfile::Tools, "c"),
            EngineProfile::Legacy
        );
        assert_eq!(
            rollout.select(EngineProfile::Legacy, "a"),
            EngineProfile::Legacy
        );
        assert_eq!(
            Rollout::default().select(EngineProfile::Text, "c"),
            EngineProfile::Text
        );
        for invalid in ["-1", "10001", "NaN", "1.5"] {
            assert!(Rollout::parse(Some(invalid), None).is_err());
        }
    }
}
