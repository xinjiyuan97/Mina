use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt, fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

/// Validated, transport-independent configuration for an agent harness.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    agent: AgentConfig,
    orchestration: OrchestrationConfig,
    default_model: String,
    models: BTreeMap<String, ModelConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHarnessConfig {
    #[serde(default)]
    agent: AgentConfig,
    #[serde(default)]
    orchestration: OrchestrationConfig,
    default_model: String,
    models: BTreeMap<String, ModelConfig>,
}

impl HarnessConfig {
    pub fn from_toml_str(source: &str) -> Result<Self, ConfigError> {
        source.parse()
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let source = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;

        Self::from_toml_str(&source)
    }

    #[must_use]
    pub fn default_model_name(&self) -> &str {
        &self.default_model
    }

    #[must_use]
    pub const fn agent(&self) -> &AgentConfig {
        &self.agent
    }

    #[must_use]
    pub const fn orchestration(&self) -> &OrchestrationConfig {
        &self.orchestration
    }

    #[must_use]
    pub fn default_model(&self) -> &ModelConfig {
        match self.models.get(&self.default_model) {
            Some(model) => model,
            None => unreachable!("validated config always contains its default model"),
        }
    }

    #[must_use]
    pub fn model(&self, name: &str) -> Option<&ModelConfig> {
        self.models.get(name)
    }

    #[must_use]
    pub fn models(&self) -> &BTreeMap<String, ModelConfig> {
        &self.models
    }

    fn normalize_and_validate(&mut self) -> Result<(), ConfigError> {
        self.agent.normalize_and_validate("agent")?;
        self.orchestration.normalize_and_validate("orchestration")?;
        validate_non_empty("default_model", &self.default_model)?;

        if self.models.is_empty() {
            return Err(ConfigError::validation(
                "models",
                "at least one model profile is required",
            ));
        }

        if !self.models.contains_key(&self.default_model) {
            return Err(ConfigError::validation(
                "default_model",
                format!("model profile `{}` does not exist", self.default_model),
            ));
        }

        for (name, model) in &mut self.models {
            validate_non_empty("models.<name>", name)?;
            model.normalize_and_validate(&format!("models.{name}"))?;
        }

        Ok(())
    }
}

impl FromStr for HarnessConfig {
    type Err = ConfigError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        let raw: RawHarnessConfig = toml::from_str(source)?;
        let mut config = Self {
            agent: raw.agent,
            orchestration: raw.orchestration,
            default_model: raw.default_model,
            models: raw.models,
        };
        config.normalize_and_validate()?;
        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationConfig {
    #[serde(default = "default_skill_directory")]
    pub skill_directory: PathBuf,
    #[serde(default)]
    pub default_skills: Vec<ConfiguredSkill>,
    #[serde(default = "default_max_skills")]
    pub max_skills: usize,
    #[serde(default = "default_true")]
    pub memory_enabled: bool,
    #[serde(default = "default_memory_scope")]
    pub memory_scope: String,
    #[serde(default = "default_max_memories")]
    pub max_memories: usize,
    #[serde(default)]
    pub compressor: ContextStrategy,
    #[serde(default = "default_context_policy_version")]
    pub context_policy_version: u32,
    #[serde(default = "default_reserved_tool_schema_tokens")]
    pub reserved_tool_schema_tokens: u64,
    #[serde(default = "default_max_skill_tokens")]
    pub max_skill_tokens: u64,
    #[serde(default = "default_max_memory_tokens")]
    pub max_memory_tokens: u64,
    #[serde(default = "default_max_history_tokens")]
    pub max_history_tokens: u64,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            skill_directory: default_skill_directory(),
            default_skills: Vec::new(),
            max_skills: default_max_skills(),
            memory_enabled: true,
            memory_scope: default_memory_scope(),
            max_memories: default_max_memories(),
            compressor: ContextStrategy::default(),
            context_policy_version: default_context_policy_version(),
            reserved_tool_schema_tokens: default_reserved_tool_schema_tokens(),
            max_skill_tokens: default_max_skill_tokens(),
            max_memory_tokens: default_max_memory_tokens(),
            max_history_tokens: default_max_history_tokens(),
        }
    }
}

impl OrchestrationConfig {
    fn normalize_and_validate(&self, field: &str) -> Result<(), ConfigError> {
        if self.max_skills == 0 || self.max_skills > 64 {
            return Err(ConfigError::validation(
                format!("{field}.max_skills"),
                "must be between 1 and 64",
            ));
        }
        if self.max_memories == 0 || self.max_memories > 1_000 {
            return Err(ConfigError::validation(
                format!("{field}.max_memories"),
                "must be between 1 and 1000",
            ));
        }
        validate_non_empty(&format!("{field}.memory_scope"), &self.memory_scope)?;
        if self.context_policy_version == 0 {
            return Err(ConfigError::validation(
                format!("{field}.context_policy_version"),
                "must be greater than zero",
            ));
        }
        for (index, skill) in self.default_skills.iter().enumerate() {
            validate_non_empty(
                &format!("{field}.default_skills[{index}].skill_id"),
                &skill.skill_id,
            )?;
            validate_non_empty(
                &format!("{field}.default_skills[{index}].version"),
                &skill.version,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredSkill {
    pub skill_id: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContextStrategy {
    NoopFail,
    SlidingWindow,
    #[default]
    Hybrid,
}

fn default_skill_directory() -> PathBuf {
    PathBuf::from("skills")
}

const fn default_max_skills() -> usize {
    8
}

const fn default_true() -> bool {
    true
}

fn default_memory_scope() -> String {
    "global".into()
}

const fn default_max_memories() -> usize {
    8
}

const fn default_context_policy_version() -> u32 {
    1
}

const fn default_reserved_tool_schema_tokens() -> u64 {
    4_096
}

const fn default_max_skill_tokens() -> u64 {
    16_384
}

const fn default_max_memory_tokens() -> u64 {
    16_384
}

const fn default_max_history_tokens() -> u64 {
    64_000
}

/// Agent implementation selected by the runtime composition root.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default)]
    pub kind: AgentKind,
    #[serde(default = "default_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_max_steps")]
    pub max_steps: u32,
    #[serde(default = "default_run_timeout_seconds")]
    pub run_timeout_seconds: u64,
    #[serde(default = "default_model_timeout_seconds")]
    pub model_timeout_seconds: u64,
    #[serde(default = "default_tool_timeout_seconds")]
    pub tool_timeout_seconds: u64,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            kind: AgentKind::default(),
            system_prompt: default_system_prompt(),
            max_steps: default_max_steps(),
            run_timeout_seconds: default_run_timeout_seconds(),
            model_timeout_seconds: default_model_timeout_seconds(),
            tool_timeout_seconds: default_tool_timeout_seconds(),
        }
    }
}

impl AgentConfig {
    fn normalize_and_validate(&self, field: &str) -> Result<(), ConfigError> {
        if matches!(self.kind, AgentKind::SingleTurn | AgentKind::AgentLoop) {
            validate_non_empty(&format!("{field}.system_prompt"), &self.system_prompt)?;
        }
        if self.max_steps == 0 {
            return Err(ConfigError::validation(
                format!("{field}.max_steps"),
                "must be greater than zero",
            ));
        }
        if self.max_steps > 64 {
            return Err(ConfigError::validation(
                format!("{field}.max_steps"),
                "must not exceed 64",
            ));
        }
        validate_timeout_seconds(
            &format!("{field}.run_timeout_seconds"),
            self.run_timeout_seconds,
        )?;
        validate_timeout_seconds(
            &format!("{field}.model_timeout_seconds"),
            self.model_timeout_seconds,
        )?;
        validate_timeout_seconds(
            &format!("{field}.tool_timeout_seconds"),
            self.tool_timeout_seconds,
        )?;
        Ok(())
    }
}

/// The echo implementation is a development fallback; `single-turn` invokes
/// the model once, while `agent-loop` can continue across bounded tool calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentKind {
    #[default]
    Echo,
    SingleTurn,
    AgentLoop,
}

fn default_system_prompt() -> String {
    "You are a helpful agent. Complete the user's task accurately and concisely.".into()
}

const fn default_max_steps() -> u32 {
    8
}

const fn default_run_timeout_seconds() -> u64 {
    300
}

const fn default_model_timeout_seconds() -> u64 {
    120
}

const fn default_tool_timeout_seconds() -> u64 {
    30
}

/// Common, provider-independent model metadata.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub model: String,
    #[serde(default)]
    pub modalities: Modalities,
    #[serde(default)]
    pub context_window: Option<u32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    pub provider: ProviderConfig,
}

impl ModelConfig {
    fn normalize_and_validate(&mut self, field: &str) -> Result<(), ConfigError> {
        validate_non_empty(&format!("{field}.model"), &self.model)?;
        self.modalities.validate(&format!("{field}.modalities"))?;

        if self.context_window == Some(0) {
            return Err(ConfigError::validation(
                format!("{field}.context_window"),
                "must be greater than zero",
            ));
        }

        if self.max_output_tokens == Some(0) {
            return Err(ConfigError::validation(
                format!("{field}.max_output_tokens"),
                "must be greater than zero",
            ));
        }

        if let (Some(context_window), Some(max_output_tokens)) =
            (self.context_window, self.max_output_tokens)
            && max_output_tokens > context_window
        {
            return Err(ConfigError::validation(
                format!("{field}.max_output_tokens"),
                "must not exceed context_window",
            ));
        }

        self.provider
            .normalize_and_validate(&format!("{field}.provider"))
    }
}

/// Input and output modalities supported by a model profile.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Modalities {
    #[serde(default = "default_text_modality")]
    pub input: BTreeSet<Modality>,
    #[serde(default = "default_text_modality")]
    pub output: BTreeSet<Modality>,
}

impl Default for Modalities {
    fn default() -> Self {
        Self {
            input: default_text_modality(),
            output: default_text_modality(),
        }
    }
}

impl Modalities {
    fn validate(&self, field: &str) -> Result<(), ConfigError> {
        if self.input.is_empty() {
            return Err(ConfigError::validation(
                format!("{field}.input"),
                "at least one input modality is required",
            ));
        }

        if self.output.is_empty() {
            return Err(ConfigError::validation(
                format!("{field}.output"),
                "at least one output modality is required",
            ));
        }

        Ok(())
    }
}

fn default_text_modality() -> BTreeSet<Modality> {
    BTreeSet::from([Modality::Text])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Modality {
    Text,
    Image,
    Audio,
    Video,
    Document,
}

/// Provider-specific settings. New providers are added as new variants while
/// `ModelConfig` and the rest of the harness keep using provider-neutral data.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ProviderConfig {
    #[serde(rename = "openai-compatible")]
    OpenAiCompatible {
        base_url: Url,
        #[serde(default)]
        api_key: Option<SecretSource>,
        #[serde(default)]
        organization: Option<String>,
        #[serde(default)]
        project: Option<String>,
    },
}

impl ProviderConfig {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::OpenAiCompatible { .. } => "openai-compatible",
        }
    }

    #[must_use]
    pub fn base_url(&self) -> &Url {
        match self {
            Self::OpenAiCompatible { base_url, .. } => base_url,
        }
    }

    #[must_use]
    pub fn api_key(&self) -> Option<&SecretSource> {
        match self {
            Self::OpenAiCompatible { api_key, .. } => api_key.as_ref(),
        }
    }

    fn normalize_and_validate(&mut self, field: &str) -> Result<(), ConfigError> {
        match self {
            Self::OpenAiCompatible {
                base_url,
                api_key,
                organization,
                project,
            } => {
                validate_base_url(&format!("{field}.base_url"), base_url)?;
                normalize_base_url(base_url);

                if let Some(api_key) = api_key {
                    api_key.validate(&format!("{field}.api_key"))?;
                }
                validate_optional_non_empty(&format!("{field}.organization"), organization)?;
                validate_optional_non_empty(&format!("{field}.project"), project)?;
            }
        }

        Ok(())
    }
}

/// A secret can be read from an environment variable or supplied literally.
/// Literal values are useful for local compatible services but should not be
/// committed to source control.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum SecretSource {
    Environment { env: String },
    Literal(String),
}

impl SecretSource {
    pub fn resolve(&self) -> Result<SecretString, ConfigError> {
        match self {
            Self::Environment { env: name } => env::var(name).map(SecretString).map_err(|source| {
                ConfigError::EnvironmentVariable {
                    name: name.clone(),
                    source,
                }
            }),
            Self::Literal(value) => Ok(SecretString(value.clone())),
        }
    }

    pub fn resolve_with(
        &self,
        lookup: impl FnOnce(&str) -> Option<String>,
    ) -> Result<SecretString, ConfigError> {
        match self {
            Self::Environment { env: name } => {
                lookup(name)
                    .map(SecretString)
                    .ok_or_else(|| ConfigError::EnvironmentVariable {
                        name: name.clone(),
                        source: env::VarError::NotPresent,
                    })
            }
            Self::Literal(value) => Ok(SecretString(value.clone())),
        }
    }

    fn validate(&self, field: &str) -> Result<(), ConfigError> {
        match self {
            Self::Environment { env } => validate_non_empty(&format!("{field}.env"), env),
            Self::Literal(value) => validate_non_empty(field, value),
        }
    }
}

impl fmt::Debug for SecretSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment { env } => formatter
                .debug_struct("Environment")
                .field("env", env)
                .finish(),
            Self::Literal(_) => formatter.write_str("Literal(<redacted>)"),
        }
    }
}

/// Resolved secret value with redacted `Debug` output.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString(<redacted>)")
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config `{path}`: {source}", path = path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid TOML: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("invalid config at `{field}`: {message}")]
    Validation { field: String, message: String },

    #[error("environment variable `{name}` could not be read: {source}")]
    EnvironmentVariable {
        name: String,
        #[source]
        source: env::VarError,
    },
}

impl ConfigError {
    fn validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            message: message.into(),
        }
    }
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::validation(field, "must not be empty"));
    }
    Ok(())
}

fn validate_optional_non_empty(field: &str, value: &Option<String>) -> Result<(), ConfigError> {
    if let Some(value) = value {
        validate_non_empty(field, value)?;
    }
    Ok(())
}

fn validate_timeout_seconds(field: &str, value: u64) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::validation(field, "must be greater than zero"));
    }
    if value > 3_600 {
        return Err(ConfigError::validation(field, "must not exceed 3600"));
    }
    Ok(())
}

fn validate_base_url(field: &str, base_url: &Url) -> Result<(), ConfigError> {
    if !matches!(base_url.scheme(), "http" | "https") {
        return Err(ConfigError::validation(
            field,
            "scheme must be http or https",
        ));
    }

    if base_url.host_str().is_none() {
        return Err(ConfigError::validation(field, "host is required"));
    }

    if !base_url.username().is_empty() || base_url.password().is_some() {
        return Err(ConfigError::validation(
            field,
            "credentials must not be embedded in the URL",
        ));
    }

    if base_url.query().is_some() || base_url.fragment().is_some() {
        return Err(ConfigError::validation(
            field,
            "query and fragment are not allowed",
        ));
    }

    Ok(())
}

fn normalize_base_url(base_url: &mut Url) {
    if !base_url.path().ends_with('/') {
        let normalized_path = format!("{}/", base_url.path());
        base_url.set_path(&normalized_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
default_model = "primary"

[models.primary]
model = "gpt-4.1"
context_window = 128000
max_output_tokens = 16384

[models.primary.modalities]
input = ["text", "image", "document"]
output = ["text"]

[models.primary.provider]
type = "openai-compatible"
base_url = "https://api.openai.com/v1"
api_key = "sk-config-file-value"
organization = "org-example"
"#;

    #[test]
    fn maps_and_normalizes_openai_compatible_config() {
        let config = HarnessConfig::from_toml_str(VALID_CONFIG).expect("config should parse");
        let model = config.default_model();

        assert_eq!(config.default_model_name(), "primary");
        assert_eq!(model.model, "gpt-4.1");
        assert!(model.modalities.input.contains(&Modality::Image));
        assert_eq!(model.provider.kind(), "openai-compatible");
        assert_eq!(
            model.provider.base_url().as_str(),
            "https://api.openai.com/v1/"
        );

        let api_key = model
            .provider
            .api_key()
            .expect("api key source should exist")
            .resolve()
            .expect("literal value should resolve");
        assert_eq!(api_key.expose_secret(), "sk-config-file-value");
        assert!(!format!("{api_key:?}").contains("sk-config-file-value"));
    }

    #[test]
    fn maps_single_turn_agent_configuration() {
        let source = VALID_CONFIG.replacen(
            "default_model = \"primary\"",
            r#"default_model = "primary"

[agent]
kind = "single-turn"
system_prompt = "Complete one task.""#,
            1,
        );
        let config = HarnessConfig::from_toml_str(&source).expect("config should parse");

        assert_eq!(config.agent().kind, AgentKind::SingleTurn);
        assert_eq!(config.agent().system_prompt, "Complete one task.");
        assert_eq!(config.agent().max_steps, 8);
        assert_eq!(config.agent().run_timeout_seconds, 300);
        assert_eq!(config.agent().model_timeout_seconds, 120);
        assert_eq!(config.agent().tool_timeout_seconds, 30);
    }

    #[test]
    fn validates_agent_timeouts() {
        let source = VALID_CONFIG.replacen(
            "default_model = \"primary\"",
            r#"default_model = "primary"

[agent]
run_timeout_seconds = 0"#,
            1,
        );
        let error = HarnessConfig::from_toml_str(&source).expect_err("zero timeout must fail");

        assert!(error.to_string().contains("agent.run_timeout_seconds"));
    }

    #[test]
    fn defaults_to_text_modalities() {
        let config = HarnessConfig::from_toml_str(
            r#"
default_model = "local"

[models.local]
model = "local-model"

[models.local.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:11434/v1"
"#,
        )
        .expect("config should parse");

        let model = config.default_model();
        assert_eq!(model.modalities.input, BTreeSet::from([Modality::Text]));
        assert_eq!(model.modalities.output, BTreeSet::from([Modality::Text]));
        assert!(model.provider.api_key().is_none());
    }

    #[test]
    fn rejects_an_unknown_default_model() {
        let error = HarnessConfig::from_toml_str(
            r#"
default_model = "missing"

[models.primary]
model = "gpt-4.1"

[models.primary.provider]
type = "openai-compatible"
base_url = "https://api.openai.com/v1"
"#,
        )
        .expect_err("unknown default must fail");

        assert!(error.to_string().contains("does not exist"));
    }

    #[test]
    fn redacts_literal_api_keys() {
        let source = SecretSource::Literal("sk-do-not-log".into());

        assert!(!format!("{source:?}").contains("sk-do-not-log"));
    }
}
