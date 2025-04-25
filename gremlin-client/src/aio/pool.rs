use crate::aio::connection::Conn;
use crate::async_pool::manager::Manager;
use crate::connection::ConnectionOptions;
use crate::error::GremlinError;
use crate::message::{message_with_args, message_with_args_and_uuid, message_with_args_v2};
use crate::{GValue, GraphSON};
use base64::encode;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug)]
pub(crate) struct GremlinConnectionManager {
    options: RwLock<ConnectionOptions>,
}

impl GremlinConnectionManager {
    pub(crate) fn new(options: ConnectionOptions) -> GremlinConnectionManager {
        GremlinConnectionManager {
            options: RwLock::new(options),
        }
    }
}

impl Manager for GremlinConnectionManager {
    type Connection = Conn;
    type ConnectOption = ConnectionOptions;
    type Error = GremlinError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let opts = self
            .options
            .read()
            .expect("failed to read connection options")
            .clone();
        Conn::connect(opts).await
    }

    /// Check if connection is still valid (should be quick)
    async fn should_recycle(
        &self,
        conn: &Self::Connection,
        _status: &crate::async_pool::state::ConnectionState,
    ) -> Result<(), Self::Error> {
        // TODO: Add a lighter check if possible, e.g., TCP socket status?
        // For now, assume it's valid if Conn thinks it is.
        if conn.is_valid() {
            Ok(())
        } else {
            // TODO: Consider a more specific error type
            Err(GremlinError::Generic("Connection is not valid".into()))
        }
    }

    async fn check(&self, mut conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        let mut args = HashMap::new();
        let options = self
            .options
            .read()
            .expect("failed to read connection options")
            .clone();

        args.insert(
            String::from("gremlin"),
            GValue::String("g.inject(0)".into()),
        );
        args.insert(
            String::from("language"),
            GValue::String(String::from("gremlin-groovy")),
        );
        let args = options.serializer.write(&GValue::from(args))?;

        let message = match options.serializer {
            GraphSON::V2 => message_with_args_v2(String::from("eval"), String::default(), args),
            GraphSON::V3 => message_with_args(String::from("eval"), String::default(), args),
        };

        let id = message.id().clone();
        let msg = serde_json::to_string(&message).map_err(GremlinError::from)?;

        let content_type = options.serializer.content_type();

        let payload = String::from("") + content_type + &msg;
        let mut binary = payload.into_bytes();
        binary.insert(0, content_type.len() as u8);

        let (response, _receiver) = conn.send(id, binary).await?;

        match response.status.code {
            200 | 206 | 204 => Ok(conn),
            407 => match &options.credentials {
                Some(c) => {
                    let mut args = HashMap::new();
                    args.insert(
                        String::from("sasl"),
                        GValue::String(encode(&format!("\0{}\0{}", c.username, c.password))),
                    );
                    let args = options.serializer.write(&GValue::from(args))?;
                    let message = message_with_args_and_uuid(
                        String::from("authentication"),
                        String::from("traversal"),
                        response.request_id,
                        args,
                    );
                    let id = message.id().clone();
                    let msg = serde_json::to_string(&message).map_err(GremlinError::from)?;
                    let content_type = options.serializer.content_type();
                    let payload = String::from("") + content_type + &msg;
                    let mut binary = payload.into_bytes();
                    binary.insert(0, content_type.len() as u8);
                    let (response, _receiver) = conn.send(id, binary).await?;
                    match response.status.code {
                        200 | 206 | 204 | 401 => Ok(conn),
                        _ => Err(GremlinError::Request((
                            response.status.code,
                            response.status.message,
                        ))),
                    }
                }
                None => Err(GremlinError::Request((
                    response.status.code,
                    response.status.message,
                ))),
            },
            _ => Err(GremlinError::Request((
                response.status.code,
                response.status.message,
            ))),
        }
    }

    fn set_connect_option(&self, options: Self::ConnectOption) {
        *self
            .options
            .write()
            .expect("failed to write to connection options") = options;
    }
}
