//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `kafka::client`.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};

#[cfg(feature = "security")]
use openssl::ssl::{Error as SslError, HandshakeError, SslConnector};

use crate::codecs::{FromByte, ToByte};
use crate::error::Result;
use crate::protocol::{ApiVersionsRequest, ApiVersionsResponse, BrokerApiVersions};

// --------------------------------------------------------------------

/// Security relevant configuration options for `KafkaClient`.
// This will be expanded in the future. See #51.
#[cfg(feature = "security")]
pub struct SecurityConfig {
    connector: SslConnector,
    verify_hostname: bool,
}

#[cfg(feature = "security")]
impl SecurityConfig {
    /// In the future this will also support a kerbos via #51.
    #[must_use]
    pub fn new(connector: SslConnector) -> Self {
        SecurityConfig {
            connector,
            verify_hostname: true,
        }
    }

    /// Initiates a client-side TLS session with/without performing hostname verification.
    #[must_use]
    pub fn with_hostname_verification(self, verify_hostname: bool) -> SecurityConfig {
        SecurityConfig {
            verify_hostname,
            ..self
        }
    }
}

#[cfg(feature = "security")]
impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SecurityConfig {{ verify_hostname: {} }}",
            self.verify_hostname
        )
    }
}

// --------------------------------------------------------------------

struct Pooled<T> {
    last_checkout: Instant,
    item: T,
}

impl<T> Pooled<T> {
    fn new(last_checkout: Instant, item: T) -> Self {
        Pooled {
            last_checkout,
            item,
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pooled {{ last_checkout: {:?}, item: {:?} }}",
            self.last_checkout, self.item
        )
    }
}

#[derive(Debug)]
pub struct Config {
    client_id: String,
    rw_timeout: Option<Duration>,
    idle_timeout: Duration,
    #[cfg(feature = "security")]
    security_config: Option<SecurityConfig>,
}

impl Config {
    #[cfg(not(feature = "security"))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        let mut conn = KafkaConnection::new(id, host, self.rw_timeout)?;
        conn.negotiate_api_versions(id as i32, &self.client_id)?;
        debug!("Established: {:?}", conn);
        Ok(conn)
    }

    #[cfg(feature = "security")]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        let mut conn = KafkaConnection::new(
            id,
            host,
            self.rw_timeout,
            self.security_config
                .as_ref()
                .map(|c| (c.connector.clone(), c.verify_hostname)),
        )
        ?;
        conn.negotiate_api_versions(id as i32, &self.client_id)?;
        debug!("Established: {:?}", conn);
        Ok(conn)
    }
}

#[derive(Debug)]
struct State {
    num_conns: u32,
}

impl State {
    fn new() -> State {
        State { num_conns: 0 }
    }

    fn next_conn_id(&mut self) -> u32 {
        let c = self.num_conns;
        self.num_conns = self.num_conns.wrapping_add(1);
        c
    }
}

#[derive(Debug)]
pub struct Connections {
    conns: HashMap<String, Pooled<KafkaConnection>>,
    state: State,
    config: Config,
}

impl Connections {
    #[cfg(not(feature = "security"))]
    pub fn new(client_id: String, rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                client_id,
                rw_timeout,
                idle_timeout,
            },
        }
    }

    #[cfg(feature = "security")]
    pub fn new(client_id: String, rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Self::new_with_security(client_id, rw_timeout, idle_timeout, None)
    }

    #[cfg(feature = "security")]
    pub fn new_with_security(
        client_id: String,
        rw_timeout: Option<Duration>,
        idle_timeout: Duration,
        security: Option<SecurityConfig>,
    ) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                client_id,
                rw_timeout,
                idle_timeout,
                security_config: security,
            },
        }
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.config.client_id = client_id;
    }

    pub fn set_idle_timeout(&mut self, idle_timeout: Duration) {
        self.config.idle_timeout = idle_timeout;
    }

    pub fn idle_timeout(&self) -> Duration {
        self.config.idle_timeout
    }

    pub fn get_conn<'a>(&'a mut self, host: &str, now: Instant) -> Result<&'a mut KafkaConnection> {
        match self.conns.entry(host.to_owned()) {
            Entry::Occupied(entry) => {
                let conn = entry.into_mut();
                if now.duration_since(conn.last_checkout) >= self.config.idle_timeout {
                    debug!("Idle timeout reached: {:?}", conn.item);
                    let new_conn = self.config.new_conn(self.state.next_conn_id(), host)?;
                    let _ = conn.item.shutdown();
                    conn.item = new_conn;
                }
                conn.last_checkout = now;
                Ok(&mut conn.item)
            }
            Entry::Vacant(entry) => {
                let cid = self.state.next_conn_id();
                Ok(&mut entry.insert(Pooled::new(now, self.config.new_conn(cid, host)?)).item)
            }
        }
    }

    pub fn get_conn_any(&mut self, now: Instant) -> Option<&mut KafkaConnection> {
        for (host, conn) in &mut self.conns {
            if now.duration_since(conn.last_checkout) >= self.config.idle_timeout {
                debug!("Idle timeout reached: {:?}", conn.item);
                let new_conn_id = self.state.next_conn_id();
                let new_conn = match self.config.new_conn(new_conn_id, host.as_str()) {
                    Ok(new_conn) => {
                        let _ = conn.item.shutdown();
                        new_conn
                    }
                    Err(e) => {
                        warn!("Failed to establish connection to {}: {:?}", host, e);
                        continue;
                    }
                };
                conn.item = new_conn;
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            return Some(kconn);
        }
        None
    }
}

// --------------------------------------------------------------------

trait IsSecured {
    fn is_secured(&self) -> bool;
}

#[cfg(not(feature = "security"))]
type KafkaStream = TcpStream;

#[cfg(not(feature = "security"))]
impl IsSecured for KafkaStream {
    fn is_secured(&self) -> bool {
        false
    }
}

#[cfg(feature = "security")]
use self::openssled::KafkaStream;

#[cfg(feature = "security")]
mod openssled {
    use std::io::{self, Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::time::Duration;

    use openssl::ssl::SslStream;

    use super::IsSecured;

    pub enum KafkaStream {
        Plain(TcpStream),
        Ssl(SslStream<TcpStream>),
    }

    impl IsSecured for KafkaStream {
        fn is_secured(&self) -> bool {
            matches!(self, KafkaStream::Ssl(_))
        }
    }

    impl KafkaStream {
        fn get_ref(&self) -> &TcpStream {
            match *self {
                KafkaStream::Plain(ref s) => s,
                KafkaStream::Ssl(ref s) => s.get_ref(),
            }
        }

        pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
            self.get_ref().set_read_timeout(dur)
        }

        pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
            self.get_ref().set_write_timeout(dur)
        }

        pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
            self.get_ref().shutdown(how)
        }
    }

    impl Read for KafkaStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.read(buf),
                KafkaStream::Ssl(ref mut s) => s.read(buf),
            }
        }
    }

    impl Write for KafkaStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.write(buf),
                KafkaStream::Ssl(ref mut s) => s.write(buf),
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.flush(),
                KafkaStream::Ssl(ref mut s) => s.flush(),
            }
        }
    }
}

/// A TCP stream to a remote Kafka broker.
pub struct KafkaConnection {
    // a surrogate identifier to distinguish between
    // connections to the same host in debug messages
    id: u32,
    // "host:port"
    host: String,
    // the (wrapped) tcp stream
    stream: KafkaStream,
    broker_api_versions: Option<BrokerApiVersions>,
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KafkaConnection {{ id: {}, secured: {}, host: \"{}\" }}",
            self.id,
            self.stream.is_secured(),
            self.host
        )
    }
}

impl KafkaConnection {
    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        let r = self.stream.write(msg).map_err(From::from);
        trace!("Sent {} bytes to: {:?} => {:?}", msg.len(), self, r);
        r
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let r = (self.stream).read_exact(buf).map_err(From::from);
        trace!("Read {} bytes from: {:?} => {:?}", buf.len(), self, r);
        r
    }

    pub fn read_exact_alloc(&mut self, size: u64) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size as usize];
        self.read_exact(buffer.as_mut_slice())?;
        Ok(buffer)
    }

    pub fn broker_api_versions(&self) -> Option<&BrokerApiVersions> {
        self.broker_api_versions.as_ref()
    }

    fn shutdown(&mut self) -> Result<()> {
        let r = self.stream.shutdown(Shutdown::Both);
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn negotiate_api_versions(&mut self, correlation_id: i32, client_id: &str) -> Result<()> {
        let req = ApiVersionsRequest::new(correlation_id, client_id);
        __send_request(self, req)?;
        let resp = __get_response::<ApiVersionsResponse>(self)?;
        self.broker_api_versions = Some(resp.into_broker_api_versions()?);
        Ok(())
    }

    fn from_stream(
        stream: KafkaStream,
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
    ) -> Result<KafkaConnection> {
        stream.set_read_timeout(rw_timeout)?;
        stream.set_write_timeout(rw_timeout)?;
        Ok(KafkaConnection {
            id,
            host: host.to_owned(),
            stream,
            broker_api_versions: None,
        })
    }

    #[cfg(not(feature = "security"))]
    fn new(id: u32, host: &str, rw_timeout: Option<Duration>) -> Result<KafkaConnection> {
        KafkaConnection::from_stream(TcpStream::connect(host)?, id, host, rw_timeout)
    }

    #[cfg(feature = "security")]
    fn new(
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
        security: Option<(SslConnector, bool)>,
    ) -> Result<KafkaConnection> {
        use crate::Error;

        let stream = TcpStream::connect(host)?;
        let stream = match security {
            Some((connector, verify_hostname)) => {
                if !verify_hostname {
                    connector
                        .configure()
                        .map_err(SslError::from)?
                        .set_verify_hostname(false);
                }
                let domain = match host.rfind(':') {
                    None => host,
                    Some(i) => &host[..i],
                };
                let connection = connector.connect(domain, stream).map_err(|err| match err {
                    HandshakeError::SetupFailure(err) => Error::from(SslError::from(err)),
                    HandshakeError::Failure(err) | HandshakeError::WouldBlock(err) => {
                        Error::from(err.into_error())
                    }
                })?;
                KafkaStream::Ssl(connection)
            }
            None => KafkaStream::Plain(stream),
        };
        KafkaConnection::from_stream(stream, id, host, rw_timeout)
    }
}

fn __send_request<T: ToByte>(conn: &mut KafkaConnection, request: T) -> Result<usize> {
    let mut buffer = Vec::with_capacity(4);
    buffer.extend_from_slice(&[0, 0, 0, 0]);
    request.encode(&mut buffer)?;
    let size = buffer.len() as i32 - 4;
    size.encode(&mut &mut buffer[..])?;
    conn.send(&buffer)
}

fn __get_response<T: FromByte>(conn: &mut KafkaConnection) -> Result<T::R> {
    let size = __get_response_size(conn)?;
    let resp = conn.read_exact_alloc(size as u64)?;
    T::decode_new(&mut Cursor::new(resp))
}

fn __get_response_size(conn: &mut KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    i32::decode_new(&mut Cursor::new(&buf))
}
