use crate::aio::pool::GremlinConnectionManager;
use crate::aio::GResultSet;
use crate::async_pool::config::{PoolConfig, Timeouts};
use crate::async_pool::Pool;
use crate::io::GraphSON;
use crate::message::{
    message_with_args, message_with_args_and_uuid, message_with_args_v2, Message,
};
use crate::process::traversal::Bytecode;
use crate::GValue;
use crate::ToGValue;
use crate::{ConnectionOptions, GremlinError, GremlinResult};
use base64::encode;
use futures::future::{BoxFuture, FutureExt};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

use super::connection::Conn;

pub type SessionedClient = GremlinClient;

impl SessionedClient {
    pub async fn close_session(&mut self) -> GremlinResult<GResultSet> {
        if let Some(session_name) = self.session.take() {
            let mut args = HashMap::new();
            args.insert(String::from("session"), GValue::from(session_name.clone()));
            let args = self.options.serializer.write(&GValue::from(args))?;

            let processor = "session".to_string();

            let message = match self.options.serializer {
                GraphSON::V2 => message_with_args_v2(String::from("close"), processor, args),
                GraphSON::V3 => message_with_args(String::from("close"), processor, args),
            };

            let mut conn = self.pool.get().await?;
            self.send_message_new(&mut conn, message).await
        } else {
            Err(GremlinError::Generic("No session to close".to_string()))
        }
    }
}

#[derive(Clone)]
pub struct GremlinClient {
    pool: Pool<GremlinConnectionManager>,
    session: Option<String>,
    alias: Option<String>,
    pub(crate) options: ConnectionOptions,
}

impl GremlinClient {
    pub async fn connect<T>(options: T) -> GremlinResult<GremlinClient>
    where
        T: Into<ConnectionOptions>,
    {
        let opts = options.into();
        let pool_size = opts.pool_size;
        let manager = GremlinConnectionManager::new(opts.clone());

        let pool = Pool::builder()
            .pool_config(
                PoolConfig::new(pool_size, 1)
                    .with_timeouts(
                        Timeouts::default()
                            .with_acquire(opts.pool_acquire_timeout)
                            .with_connect(opts.pool_connect_timeout)
                            .with_idle(opts.pool_idle_timeout),
                    )
                    .with_pool_maintenance_interval(opts.pool_maintenance_interval),
            )
            .build(manager)
            .await?;

        Ok(GremlinClient {
            pool,
            session: None,
            alias: None,
            options: opts,
        })
    }

    pub async fn create_session(&mut self, name: String) -> GremlinResult<SessionedClient> {
        let manager = GremlinConnectionManager::new(self.options.clone());
        Ok(SessionedClient {
            pool: Pool::builder()
                .pool_config(PoolConfig::new(1, 1))
                .build(manager)
                .await?,
            session: Some(name),
            alias: None,
            options: self.options.clone(),
        })
    }

    /// Return a cloned client with the provided alias
    pub fn alias<T>(&self, alias: T) -> GremlinClient
    where
        T: Into<String>,
    {
        let mut cloned = self.clone();
        cloned.alias = Some(alias.into());
        cloned
    }

    pub async fn execute<T>(
        &self,
        script: T,
        params: &[(&str, &dyn ToGValue)],
    ) -> GremlinResult<GResultSet>
    where
        T: Into<String>,
    {
        let mut args = HashMap::new();

        args.insert(String::from("gremlin"), GValue::String(script.into()));
        args.insert(
            String::from("language"),
            GValue::String(String::from("gremlin-groovy")),
        );

        let aliases = self
            .alias
            .clone()
            .map(|s| {
                let mut map = HashMap::new();
                map.insert(String::from("g"), GValue::String(s));
                map
            })
            .unwrap_or_else(HashMap::new);

        args.insert(String::from("aliases"), GValue::from(aliases));

        let bindings: HashMap<String, GValue> = params
            .iter()
            .map(|(k, v)| (String::from(*k), v.to_gvalue()))
            .collect();

        args.insert(String::from("bindings"), GValue::from(bindings));

        if let Some(session_name) = &self.session {
            args.insert(String::from("session"), GValue::from(session_name.clone()));
        }

        let args = self.options.serializer.write(&GValue::from(args))?;

        let processor = if self.session.is_some() {
            "session".to_string()
        } else {
            String::default()
        };

        let message = match self.options.serializer {
            GraphSON::V2 => message_with_args_v2(String::from("eval"), processor, args),
            GraphSON::V3 => message_with_args(String::from("eval"), processor, args),
        };

        let mut conn = self.pool.get().await?;

        self.send_message_new(&mut conn, message).await
    }

    pub(crate) fn send_message_new<'a, T: Serialize>(
        &'a self,
        conn: &'a mut Conn,
        msg: Message<T>,
    ) -> BoxFuture<'a, GremlinResult<GResultSet>> {
        let id = msg.id().clone();
        let message = self.build_message(msg).unwrap();

        async move {
            let content_type = self.options.serializer.content_type();
            let payload = String::from("") + content_type + &message;
            let mut binary = payload.into_bytes();
            binary.insert(0, content_type.len() as u8);

            let (response, receiver) = conn.send(id, binary).await?;

            let (response, results) = match response.status.code {
                200 | 206 => {
                    let results: VecDeque<GValue> = self
                        .options
                        .deserializer
                        .read(&response.result.data)?
                        .map(|v| v.into())
                        .unwrap_or_else(VecDeque::new);
                    Ok((response, results))
                }
                204 => Ok((response, VecDeque::new())),
                407 => match &self.options.credentials {
                    Some(c) => {
                        let mut args = HashMap::new();

                        args.insert(
                            String::from("sasl"),
                            GValue::String(encode(&format!("\0{}\0{}", c.username, c.password))),
                        );

                        let args = self.options.serializer.write(&GValue::from(args))?;
                        let message = message_with_args_and_uuid(
                            String::from("authentication"),
                            String::from("traversal"),
                            response.request_id,
                            args,
                        );

                        return self.send_message_new(conn, message).await;
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
            }?;

            Ok(GResultSet::new(self.clone(), results, response, receiver))
        }
        .boxed()
    }

    pub(crate) async fn submit_traversal(&self, bytecode: &Bytecode) -> GremlinResult<GResultSet> {
        let mut args = HashMap::new();

        args.insert(String::from("gremlin"), GValue::Bytecode(bytecode.clone()));

        let aliases = self
            .alias
            .clone()
            .or_else(|| Some(String::from("g")))
            .map(|s| {
                let mut map = HashMap::new();
                map.insert(String::from("g"), GValue::String(s));
                map
            })
            .unwrap_or_else(HashMap::new);

        args.insert(String::from("aliases"), GValue::from(aliases));

        let args = self.options.serializer.write(&GValue::from(args))?;

        let message = message_with_args(String::from("bytecode"), String::from("traversal"), args);

        let mut conn = self.pool.get().await?;

        self.send_message_new(&mut conn, message).await
    }

    fn build_message<T: Serialize>(&self, msg: Message<T>) -> GremlinResult<String> {
        serde_json::to_string(&msg).map_err(GremlinError::from)
    }

    pub fn set_connect_option(&self, connect_option: ConnectionOptions) {
        self.pool.set_connect_option(connect_option);
    }

    pub fn get_connect_option(&self) -> ConnectionOptions {
        self.pool.get_connect_option()
    }
}
