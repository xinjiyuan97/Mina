use tokio_util::sync::CancellationToken;

/// Cloneable cooperative cancellation signal shared by the Harness, Agent,
/// model invocations, and tool executions for one run.
#[derive(Debug, Clone, Default)]
pub struct RunCancellation {
    token: CancellationToken,
}

impl RunCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// Clone the underlying signal when crossing into an infrastructure port.
    #[must_use]
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}
