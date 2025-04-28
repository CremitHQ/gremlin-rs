use std::{net::TcpStream, sync::Arc, time::Duration};

use crate::{async_pool::config::Timeouts, GraphSON, GremlinError, GremlinResult};
use native_tls::TlsConnector;
use tungstenite::{
    client::{uri_mode, IntoClientRequest},
    client_tls_with_config,
    http::{HeaderName, HeaderValue},
    protocol::WebSocketConfig,
    stream::{MaybeTlsStream, Mode, NoDelay},
    Connector, Message, WebSocket,
};

struct ConnectionStream(WebSocket<MaybeTlsStream<TcpStream>>);

impl std::fmt::Debug for ConnectionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Connection")
    }
}

impl ConnectionStream {
    fn connect(options: ConnectionOptions) -> GremlinResult<Self> {
        let connector = match options.tls_options.as_ref() {
            Some(option) => Some(Connector::NativeTls(
                option
                    .tls_connector()
                    .map_err(|e| GremlinError::Generic(e.to_string()))?,
            )),
            _ => None,
        };

        let mut request = options
            .websocket_url()
            .into_client_request()
            .map_err(|e| GremlinError::Generic(e.to_string()))?;
        if let Some(headers) = &options.headers {
            for (name, value) in headers {
                request.headers_mut().insert(
                    HeaderName::from_bytes(name.as_bytes())
                        .map_err(|e| GremlinError::Generic(e.to_string()))?,
                    HeaderValue::from_str(value)
                        .map_err(|e| GremlinError::Generic(e.to_string()))?,
                );
            }
        }

        let uri = request
            .uri()
            .host()
            .ok_or_else(|| GremlinError::Generic("No Hostname".into()))?;
        let mode = uri_mode(request.uri()).map_err(|e| GremlinError::Generic(e.to_string()))?;
        let port = request.uri().port_u16().unwrap_or(match mode {
            Mode::Plain => 80,
            Mode::Tls => 443,
        });
        let mut stream = TcpStream::connect((uri, port))
            .map_err(|e| GremlinError::Generic(format!("Unable to connect {e:?}")))?;
        NoDelay::set_nodelay(&mut stream, true)
            .map_err(|e| GremlinError::Generic(e.to_string()))?;

        let websocket_config = options
            .websocket_options
            .as_ref()
            .map(WebSocketConfig::from);

        let (client, _response) =
            client_tls_with_config(request, stream, websocket_config, connector)
                .map_err(|e| GremlinError::Generic(e.to_string()))?;

        Ok(ConnectionStream(client))
    }

    fn send(&mut self, payload: Vec<u8>) -> GremlinResult<()> {
        self.0
            .write_message(Message::Binary(payload))
            .map_err(GremlinError::from)
    }

    fn recv(&mut self) -> GremlinResult<Vec<u8>> {
        match self.0.read_message()? {
            Message::Binary(binary) => Ok(binary),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Connection {
    stream: ConnectionStream,
    broken: bool,
}

impl Into<ConnectionOptions> for (&str, u16) {
    fn into(self) -> ConnectionOptions {
        ConnectionOptions {
            host: String::from(self.0),
            port: self.1,
            ..Default::default()
        }
    }
}

impl Into<ConnectionOptions> for &str {
    fn into(self) -> ConnectionOptions {
        ConnectionOptions {
            host: String::from(self),
            ..Default::default()
        }
    }
}

pub struct ConnectionOptionsBuilder(ConnectionOptions);

impl ConnectionOptionsBuilder {
    pub fn host<T>(mut self, host: T) -> Self
    where
        T: Into<String>,
    {
        self.0.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.0.port = port;
        self
    }

    pub fn pool_size(mut self, pool_size: u32) -> Self {
        self.0.pool_size = pool_size;
        self
    }

    /// Only applicable to async client. By default a connection is checked on each return to the pool (None)
    /// This allows setting an interval of how often it is checked on return.
    pub fn pool_healthcheck_interval(
        mut self,
        pool_healthcheck_interval: Option<Duration>,
    ) -> Self {
        self.0.pool_healthcheck_interval = pool_healthcheck_interval;
        self
    }

    /// Both the sync and async pool providers use a default of 30 seconds,
    /// Async pool interprets `None` as no timeout. Sync pool maps `None` to the default value
    pub fn pool_connection_timeout(mut self, pool_connection_timeout: Option<Duration>) -> Self {
        self.0.pool_get_connection_timeout = pool_connection_timeout;
        self
    }

    pub fn build(self) -> ConnectionOptions {
        self.0
    }

    pub fn credentials(mut self, username: &str, password: &str) -> Self {
        self.0.credentials = Some(Credentials {
            username: String::from(username),
            password: String::from(password),
        });
        self
    }

    pub fn ssl(mut self, ssl: bool) -> Self {
        self.0.ssl = ssl;
        self
    }

    pub fn tls_options(mut self, options: TlsOptions) -> Self {
        self.0.tls_options = Some(options);
        self
    }

    pub fn websocket_options(mut self, options: WebSocketOptions) -> Self {
        self.0.websocket_options = Some(options);
        self
    }

    pub fn serializer(mut self, serializer: GraphSON) -> Self {
        self.0.serializer = serializer;
        self
    }

    pub fn deserializer(mut self, deserializer: GraphSON) -> Self {
        self.0.deserializer = deserializer;
        self
    }

    /// Set multiple headers for the WebSocket handshake.
    pub fn headers<T, U>(mut self, headers: impl IntoIterator<Item = (T, U)>) -> Self
    where
        T: Into<String>,
        U: Into<String>,
    {
        self.0.headers = Some(
            headers
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }

    /// Add a single header for the WebSocket handshake.
    pub fn header<T, U>(mut self, name: T, value: U) -> Self
    where
        T: Into<String>,
        U: Into<String>,
    {
        if self.0.headers.is_none() {
            self.0.headers = Some(Vec::new());
        }
        self.0
            .headers
            .as_mut()
            .unwrap()
            .push((name.into(), value.into()));
        self
    }

    pub fn pool_create_timeout(mut self, pool_create_timeout: Option<Duration>) -> Self {
        self.0.pool_acquire_timeout = pool_create_timeout;
        self
    }

    pub fn pool_idle_timeout(mut self, pool_idle_timeout: Option<Duration>) -> Self {
        self.0.pool_idle_timeout = pool_idle_timeout;
        self
    }

    pub fn pool_maintenance_interval(
        mut self,
        pool_maintenance_interval: Option<Duration>,
    ) -> Self {
        self.0.pool_maintenance_interval = pool_maintenance_interval;
        self
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) pool_size: u32,
    pub(crate) pool_healthcheck_interval: Option<Duration>,
    pub(crate) pool_get_connection_timeout: Option<Duration>,
    pub(crate) pool_acquire_timeout: Option<Duration>,
    pub(crate) pool_connect_timeout: Option<Duration>,
    pub(crate) pool_idle_timeout: Option<Duration>,
    pub(crate) pool_maintenance_interval: Option<Duration>,
    pub(crate) credentials: Option<Credentials>,
    pub(crate) ssl: bool,
    pub(crate) tls_options: Option<TlsOptions>,
    pub(crate) serializer: GraphSON,
    pub(crate) deserializer: GraphSON,
    pub(crate) websocket_options: Option<WebSocketOptions>,
    pub(crate) headers: Option<Vec<(String, String)>>,
}

#[derive(Clone, Debug)]
pub(crate) struct Credentials {
    pub(crate) username: String,
    pub(crate) password: String,
}

#[derive(Clone, Debug)]
pub struct TlsOptions {
    pub accept_invalid_certs: bool,
}

#[derive(Clone, Debug)]
pub struct WebSocketOptions {
    /// The maximum size of a message. `None` means no size limit. The default value is 64 MiB.
    pub(crate) max_message_size: Option<usize>,
    /// The maximum size of a single message frame. `None` means no size limit. The limit is for
    /// frame payload NOT including the frame header. The default value is 16 MiB.
    pub(crate) max_frame_size: Option<usize>,
}

impl WebSocketOptions {
    pub fn builder() -> WebSocketOptionsBuilder {
        WebSocketOptionsBuilder(Self::default())
    }
}

impl Default for WebSocketOptions {
    fn default() -> Self {
        Self {
            max_message_size: Some(64 << 20),
            max_frame_size: Some(16 << 20),
        }
    }
}

impl From<WebSocketOptions> for tungstenite::protocol::WebSocketConfig {
    fn from(value: WebSocketOptions) -> Self {
        (&value).into()
    }
}

impl From<&WebSocketOptions> for tungstenite::protocol::WebSocketConfig {
    fn from(value: &WebSocketOptions) -> Self {
        let mut config = tungstenite::protocol::WebSocketConfig::default();
        config.max_message_size = value.max_message_size;
        config.max_frame_size = value.max_frame_size;
        config
    }
}

pub struct WebSocketOptionsBuilder(WebSocketOptions);

impl WebSocketOptionsBuilder {
    pub fn build(self) -> WebSocketOptions {
        self.0
    }

    pub fn max_message_size(mut self, max_message_size: Option<usize>) -> Self {
        self.0.max_message_size = max_message_size;
        self
    }

    pub fn max_frame_size(mut self, max_frame_size: Option<usize>) -> Self {
        self.0.max_frame_size = max_frame_size;
        self
    }
}

impl Default for ConnectionOptions {
    fn default() -> ConnectionOptions {
        ConnectionOptions {
            host: String::from("localhost"),
            port: 8182,
            pool_size: 10,
            pool_get_connection_timeout: Some(Duration::from_secs(30)),
            pool_healthcheck_interval: None,
            credentials: None,
            ssl: false,
            tls_options: None,
            serializer: GraphSON::V3,
            deserializer: GraphSON::V3,
            websocket_options: None,
            headers: None,
            pool_acquire_timeout: None,
            pool_idle_timeout: None,
            pool_connect_timeout: None,
            pool_maintenance_interval: None,
        }
    }
}

impl ConnectionOptions {
    pub fn builder() -> ConnectionOptionsBuilder {
        ConnectionOptionsBuilder(ConnectionOptions::default())
    }

    pub fn websocket_url(&self) -> String {
        let protocol = if self.ssl { "wss" } else { "ws" };
        format!("{}://{}:{}/gremlin", protocol, self.host, self.port)
    }

    pub fn into_builder(self) -> ConnectionOptionsBuilder {
        ConnectionOptionsBuilder(self)
    }
}

impl Connection {
    pub fn connect<T>(options: T) -> GremlinResult<Connection>
    where
        T: Into<ConnectionOptions>,
    {
        Ok(Connection {
            stream: ConnectionStream::connect(options.into())?,
            broken: false,
        })
    }

    pub fn send(&mut self, payload: Vec<u8>) -> GremlinResult<()> {
        self.stream.send(payload).map_err(|e| {
            if let GremlinError::WebSocket(_) = e {
                self.broken = true;
            }
            e
        })
    }

    pub fn recv(&mut self) -> GremlinResult<Vec<u8>> {
        self.stream.recv().map_err(|e| {
            if let GremlinError::WebSocket(_) = e {
                self.broken = true
            }
            e
        })
    }

    pub fn is_broken(&self) -> bool {
        self.broken
    }
}

impl TlsOptions {
    pub(crate) fn tls_connector(&self) -> native_tls::Result<TlsConnector> {
        TlsConnector::builder()
            .danger_accept_invalid_certs(self.accept_invalid_certs)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_should_connect() {
        Connection::connect(("localhost", 8182)).unwrap();
    }

    #[test]
    fn connection_option_build_url() {
        let options = ConnectionOptions {
            host: "localhost".into(),
            port: 8182,
            ssl: false,
            ..Default::default()
        };

        assert_eq!(options.websocket_url(), "ws://localhost:8182/gremlin");

        let options = ConnectionOptions {
            host: "localhost".into(),
            port: 8182,
            ssl: true,
            ..Default::default()
        };

        assert_eq!(options.websocket_url(), "wss://localhost:8182/gremlin");
    }
}
