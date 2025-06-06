use std::sync::Arc;

use crate::{async_pool::error::ConnectionError, structure::GValue};

use thiserror::Error;

#[cfg(feature = "async_gremlin")]
use mobc;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum GremlinError {
    #[error("data store disconnected: {0}")]
    Generic(String),

    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),

    #[error(transparent)]
    Pool(#[from] r2d2::Error),

    #[error("Got wrong type {0:?}")]
    WrongType(GValue),

    #[error("Cast error: {0}")]
    Cast(String),

    #[error("JSON error: {0}")]
    Json(String),

    #[error("Request error: {0:?} ")]
    Request((i16, String)),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("Invalid header name: {0:?}")]
    InvalidHeaderName(String),

    #[cfg(feature = "async_gremlin")]
    #[error(transparent)]
    WebSocketAsync(#[from] Arc<async_tungstenite::tungstenite::Error>),
    #[cfg(feature = "async_gremlin")]
    #[error(transparent)]
    WebSocketPoolAsync(#[from] Arc<mobc::Error<GremlinError>>),
    #[cfg(feature = "async_gremlin")]
    #[error(transparent)]
    ChannelSend(#[from] futures::channel::mpsc::SendError),
    #[error(transparent)]
    Uuid(#[from] uuid::Error),

    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
}

#[cfg(feature = "async_gremlin")]
impl From<mobc::Error<GremlinError>> for GremlinError {
    fn from(e: mobc::Error<GremlinError>) -> GremlinError {
        match e {
            mobc::Error::Inner(e) => e,
            other => GremlinError::WebSocketPoolAsync(Arc::new(other)),
        }
    }
}
