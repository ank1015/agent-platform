use agent_contracts::{EventId, OperationId, SessionId, WaitId};

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("session {0} was not found")]
    SessionNotFound(SessionId),
    #[error("session {0} already exists")]
    SessionAlreadyExists(SessionId),
    #[error("event {0} was not found")]
    EventNotFound(EventId),
    #[error("operation {0} was not found")]
    OperationNotFound(OperationId),
    #[error("wait {0} was not found")]
    WaitNotFound(WaitId),
    #[error("the idempotency key was already used with a different request")]
    IdempotencyConflict,
    #[error("the processing claim is no longer authoritative")]
    StaleClaim,
    #[error("the wait cannot be resolved in its current mode or after its deadline")]
    WaitNotResolvable,
    #[error("the blocked processing event or revision no longer matches")]
    StaleProcessingRetry,
    #[error("the update cursor is older than retained updates")]
    UpdateCursorExpired,
    #[error("invalid stored {kind}: {message}")]
    InvalidStoredJson { kind: &'static str, message: String },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}

#[derive(Debug, thiserror::Error)]
pub enum HandlerCommitError {
    #[error("the processing claim is no longer authoritative")]
    StaleClaim,
    #[error("handler outcome transaction rolled back and may be retried: {0}")]
    Retryable(StoreError),
    #[error("handler outcome was rejected: {0}")]
    Rejected(StoreError),
    #[error("handler outcome commit acknowledgement is uncertain: {0}")]
    Uncertain(StoreError),
}

impl HandlerCommitError {
    pub(crate) fn from_commit(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(database) = &error {
            let retryable = database
                .code()
                .is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01"));
            let error = StoreError::Database(error);
            return if retryable {
                Self::Retryable(error)
            } else {
                Self::Rejected(error)
            };
        }
        Self::Uncertain(StoreError::Database(error))
    }

    pub fn into_store_error(self) -> StoreError {
        match self {
            Self::StaleClaim => StoreError::StaleClaim,
            Self::Retryable(error) | Self::Rejected(error) | Self::Uncertain(error) => error,
        }
    }
}

impl From<StoreError> for HandlerCommitError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::StaleClaim => Self::StaleClaim,
            StoreError::Database(ref database) if retryable_database_error(database) => {
                Self::Retryable(error)
            }
            StoreError::Database(_) => Self::Rejected(error),
            _ => Self::Rejected(error),
        }
    }
}

impl From<sqlx::Error> for HandlerCommitError {
    fn from(error: sqlx::Error) -> Self {
        Self::from(StoreError::Database(error))
    }
}

fn retryable_database_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(error) => error.code().is_some_and(|code| {
            matches!(
                code.as_ref(),
                "40001" | "40P01" | "55P03" | "57014" | "53300" | "57P01"
            ) || code.starts_with("08")
        }),
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        _ => false,
    }
}
