use thiserror::Error;
use tokio::{sync::AcquireError, time::error::Elapsed};

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("failed to acquire connection from pool")]
    Acquire(#[from] AcquireError),

    #[error("timeout while connecting")]
    Timeout(#[from] Elapsed),

    #[error("failed to connect to the database: {0}")]
    PoolConnect(String),
}
