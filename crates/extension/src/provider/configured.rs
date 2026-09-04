use std::sync::Arc;

use agent_core::{
    context::TokenEstimator,
    harness::{BlobStore, ModelEventStream, ModelPort, ModelRequest},
};
use agent_harness::{ModelConfig, OpenAiProtocol, ProviderConfig};

use super::{
    AdapterConfigError, AnthropicMessagesProvider, OpenAiCompatibleProvider,
    OpenAiResponsesProvider,
};

/// Runtime-selected provider adapter behind the provider-neutral `ModelPort`.
///
/// Applications construct this once from configuration; protocol branching
/// stays inside the extension layer.
pub enum ConfiguredModelProvider {
    OpenAiChat(OpenAiCompatibleProvider),
    OpenAiResponses(OpenAiResponsesProvider),
    AnthropicMessages(AnthropicMessagesProvider),
}

impl ConfiguredModelProvider {
    pub fn from_model_config(model: &ModelConfig) -> Result<Self, AdapterConfigError> {
        match &model.provider {
            ProviderConfig::OpenAiCompatible { protocol, .. } => match protocol {
                OpenAiProtocol::ChatCompletions => Ok(Self::OpenAiChat(
                    OpenAiCompatibleProvider::from_model_config(model)?,
                )),
                OpenAiProtocol::Responses => Ok(Self::OpenAiResponses(
                    OpenAiResponsesProvider::from_model_config(model)?,
                )),
            },
            ProviderConfig::Anthropic { .. } => Ok(Self::AnthropicMessages(
                AnthropicMessagesProvider::from_model_config(model)?,
            )),
            _ => Err(AdapterConfigError::UnsupportedProvider),
        }
    }

    #[must_use]
    pub fn with_blob_store(self, blob_store: Arc<dyn BlobStore>) -> Self {
        match self {
            Self::OpenAiChat(provider) => Self::OpenAiChat(provider.with_blob_store(blob_store)),
            Self::OpenAiResponses(provider) => {
                Self::OpenAiResponses(provider.with_blob_store(blob_store))
            }
            Self::AnthropicMessages(provider) => {
                Self::AnthropicMessages(provider.with_blob_store(blob_store))
            }
        }
    }

    #[must_use]
    pub fn with_token_estimator(self, estimator: Arc<dyn TokenEstimator>) -> Self {
        match self {
            Self::OpenAiChat(provider) => {
                Self::OpenAiChat(provider.with_token_estimator(estimator))
            }
            Self::OpenAiResponses(provider) => {
                Self::OpenAiResponses(provider.with_token_estimator(estimator))
            }
            Self::AnthropicMessages(provider) => {
                Self::AnthropicMessages(provider.with_token_estimator(estimator))
            }
        }
    }
}

impl ModelPort for ConfiguredModelProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        match self {
            Self::OpenAiChat(provider) => provider.stream(request),
            Self::OpenAiResponses(provider) => provider.stream(request),
            Self::AnthropicMessages(provider) => provider.stream(request),
        }
    }
}
