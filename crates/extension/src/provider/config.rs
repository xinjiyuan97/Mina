use std::{collections::BTreeSet, env, fmt};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

/// Validated model profile consumed by concrete provider adapters.
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
    pub fn normalize_and_validate(&mut self, field: &str) -> Result<(), ProviderConfigError> {
        validate_non_empty(&format!("{field}.model"), &self.model)?;
        self.modalities.validate(&format!("{field}.modalities"))?;
        if self.context_window == Some(0) {
            return Err(ProviderConfigError::validation(
                format!("{field}.context_window"),
                "must be greater than zero",
            ));
        }
        if self.max_output_tokens == Some(0) {
            return Err(ProviderConfigError::validation(
                format!("{field}.max_output_tokens"),
                "must be greater than zero",
            ));
        }
        if let (Some(context_window), Some(max_output_tokens)) =
            (self.context_window, self.max_output_tokens)
            && max_output_tokens > context_window
        {
            return Err(ProviderConfigError::validation(
                format!("{field}.max_output_tokens"),
                "must not exceed context_window",
            ));
        }
        self.provider
            .normalize_and_validate(&format!("{field}.provider"))
    }
}

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
    fn validate(&self, field: &str) -> Result<(), ProviderConfigError> {
        if self.input.is_empty() {
            return Err(ProviderConfigError::validation(
                format!("{field}.input"),
                "at least one input modality is required",
            ));
        }
        if self.output.is_empty() {
            return Err(ProviderConfigError::validation(
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpenAiProtocol {
    #[default]
    ChatCompletions,
    Responses,
}

/// Provider-owned configuration. Harness selects profiles but does not own
/// adapter-specific fields or secret resolution.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ProviderConfig {
    #[serde(rename = "openai-compatible")]
    OpenAiCompatible {
        base_url: Url,
        #[serde(default)]
        protocol: OpenAiProtocol,
        #[serde(default)]
        api_key: Option<SecretSource>,
        #[serde(default)]
        organization: Option<String>,
        #[serde(default)]
        project: Option<String>,
    },
    Anthropic {
        base_url: Url,
        api_key: SecretSource,
        #[serde(default = "default_anthropic_version")]
        version: String,
    },
}

impl ProviderConfig {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::OpenAiCompatible { .. } => "openai-compatible",
            Self::Anthropic { .. } => "anthropic",
        }
    }

    #[must_use]
    pub const fn protocol(&self) -> &'static str {
        match self {
            Self::OpenAiCompatible {
                protocol: OpenAiProtocol::ChatCompletions,
                ..
            } => "chat-completions",
            Self::OpenAiCompatible {
                protocol: OpenAiProtocol::Responses,
                ..
            } => "responses",
            Self::Anthropic { .. } => "messages",
        }
    }

    #[must_use]
    pub fn base_url(&self) -> &Url {
        match self {
            Self::OpenAiCompatible { base_url, .. } | Self::Anthropic { base_url, .. } => base_url,
        }
    }

    #[must_use]
    pub fn api_key(&self) -> Option<&SecretSource> {
        match self {
            Self::OpenAiCompatible { api_key, .. } => api_key.as_ref(),
            Self::Anthropic { api_key, .. } => Some(api_key),
        }
    }

    fn normalize_and_validate(&mut self, field: &str) -> Result<(), ProviderConfigError> {
        match self {
            Self::OpenAiCompatible {
                base_url,
                protocol: _,
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
            Self::Anthropic {
                base_url,
                api_key,
                version,
            } => {
                validate_base_url(&format!("{field}.base_url"), base_url)?;
                normalize_base_url(base_url);
                api_key.validate(&format!("{field}.api_key"))?;
                validate_non_empty(&format!("{field}.version"), version)?;
            }
        }
        Ok(())
    }
}

fn default_anthropic_version() -> String {
    "2023-06-01".into()
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum SecretSource {
    Environment { env: String },
    Literal(String),
}

impl SecretSource {
    pub fn resolve(&self) -> Result<SecretString, ProviderConfigError> {
        self.resolve_with(|name| env::var(name).ok())
    }

    pub fn resolve_with(
        &self,
        lookup: impl FnOnce(&str) -> Option<String>,
    ) -> Result<SecretString, ProviderConfigError> {
        match self {
            Self::Environment { env: name } => lookup(name)
                .map(SecretString)
                .ok_or_else(|| ProviderConfigError::EnvironmentVariable { name: name.clone() }),
            Self::Literal(value) => Ok(SecretString(value.clone())),
        }
    }

    fn validate(&self, field: &str) -> Result<(), ProviderConfigError> {
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
pub enum ProviderConfigError {
    #[error("invalid provider config at `{field}`: {message}")]
    Validation { field: String, message: String },
    #[error("environment variable `{name}` could not be read")]
    EnvironmentVariable { name: String },
}

impl ProviderConfigError {
    fn validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            message: message.into(),
        }
    }
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), ProviderConfigError> {
    if value.trim().is_empty() {
        return Err(ProviderConfigError::validation(field, "must not be empty"));
    }
    Ok(())
}

fn validate_optional_non_empty(
    field: &str,
    value: &Option<String>,
) -> Result<(), ProviderConfigError> {
    if let Some(value) = value {
        validate_non_empty(field, value)?;
    }
    Ok(())
}

fn validate_base_url(field: &str, base_url: &Url) -> Result<(), ProviderConfigError> {
    if !matches!(base_url.scheme(), "http" | "https") {
        return Err(ProviderConfigError::validation(
            field,
            "scheme must be http or https",
        ));
    }
    if base_url.host_str().is_none() {
        return Err(ProviderConfigError::validation(field, "host is required"));
    }
    if !base_url.username().is_empty() || base_url.password().is_some() {
        return Err(ProviderConfigError::validation(
            field,
            "credentials must not be embedded in the URL",
        ));
    }
    if base_url.query().is_some() || base_url.fragment().is_some() {
        return Err(ProviderConfigError::validation(
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
