//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `kafkang::client`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::io::{Cursor, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};

use super::sasl::SaslConfig;
#[cfg(feature = "security")]
use super::tls;
#[cfg(feature = "security")]
use super::tls::SecurityConfig;
#[cfg(feature = "security")]
use crate::error::TlsError;

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, KafkaCode, Result};
use crate::protocol::KafkaRequest;
use crate::protocol::{
    ApiVersionsRequest, ApiVersionsResponse, BrokerApiVersions, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};

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
    sasl: Option<SaslConfig>,
    #[cfg(feature = "security")]
    security: Option<SecurityConfig>,
}

impl Config {
    #[cfg(not(feature = "security"))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        let mut conn = KafkaConnection::new(id, host, self.rw_timeout)?;
        if let Some(sasl) = self.sasl.as_ref() {
            conn.authenticate_sasl(id as i32, &self.client_id, sasl)?;
            conn.negotiate_api_versions((id as i32).wrapping_add(2), &self.client_id)?;
        } else {
            conn.negotiate_api_versions(id as i32, &self.client_id)?;
        }
        debug!("Established: {:?}", conn);
        Ok(conn)
    }

    #[cfg(feature = "security")]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        let mut conn = KafkaConnection::new(id, host, self.rw_timeout, self.security.as_ref())?;
        if let Some(sasl) = self.sasl.as_ref() {
            conn.authenticate_sasl(id as i32, &self.client_id, sasl)?;
            conn.negotiate_api_versions((id as i32).wrapping_add(2), &self.client_id)?;
        } else {
            conn.negotiate_api_versions(id as i32, &self.client_id)?;
        }
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
    pub fn new(
        client_id: String,
        rw_timeout: Option<Duration>,
        idle_timeout: Duration,
    ) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                client_id,
                rw_timeout,
                idle_timeout,
                sasl: None,
            },
        }
    }

    #[cfg(feature = "security")]
    pub fn new(
        client_id: String,
        rw_timeout: Option<Duration>,
        idle_timeout: Duration,
    ) -> Connections {
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
                sasl: None,
                security,
            },
        }
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.config.client_id = client_id;
    }

    pub fn set_idle_timeout(&mut self, idle_timeout: Duration) {
        self.config.idle_timeout = idle_timeout;
    }

    pub fn set_sasl_config(&mut self, sasl: Option<SaslConfig>) {
        for conn in self.conns.values_mut() {
            let _ = conn.item.shutdown();
        }
        self.conns.clear();
        self.config.sasl = sasl;
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
                Ok(&mut entry
                    .insert(Pooled::new(now, self.config.new_conn(cid, host)?))
                    .item)
            }
        }
    }

    pub fn with_conn<T, F>(&mut self, host: &str, now: Instant, operation: F) -> Result<T>
    where
        F: FnOnce(&mut KafkaConnection) -> Result<T>,
    {
        let result = operation(self.get_conn(host, now)?);
        if result.is_err() {
            self.invalidate(host);
        }
        result
    }

    pub fn with_conn_any<T, F>(&mut self, now: Instant, operation: F) -> Result<T>
    where
        F: FnOnce(&mut KafkaConnection) -> Result<T>,
    {
        let hosts: Vec<String> = self.conns.keys().cloned().collect();
        let mut last_error = None;
        for host in hosts {
            let result = match self.get_conn(&host, now) {
                Ok(conn) => operation(conn),
                Err(e) => {
                    warn!("Failed to establish connection to {}: {:?}", host, e);
                    last_error = Some(e);
                    continue;
                }
            };
            if result.is_err() {
                self.invalidate(&host);
            }
            return result;
        }
        Err(last_error.unwrap_or(Error::NoHostReachable))
    }

    fn invalidate(&mut self, host: &str) {
        if let Some(mut conn) = self.conns.remove(host) {
            debug!("Evicting failed connection: {:?}", conn.item);
            let _ = conn.item.shutdown();
        }
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
use self::tlsed::KafkaStream;

#[cfg(feature = "security")]
mod tlsed {
    use std::io::{self, Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::time::Duration;

    use super::IsSecured;
    use crate::client::tls::TlsStream;

    pub enum KafkaStream {
        Plain(TcpStream),
        Tls(Box<TlsStream>),
    }

    impl IsSecured for KafkaStream {
        fn is_secured(&self) -> bool {
            matches!(self, KafkaStream::Tls(_))
        }
    }

    impl KafkaStream {
        fn get_ref(&self) -> &TcpStream {
            match *self {
                KafkaStream::Plain(ref s) => s,
                KafkaStream::Tls(ref s) => s.get_ref(),
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
                KafkaStream::Tls(ref mut s) => s.read(buf),
            }
        }
    }

    impl Write for KafkaStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.write(buf),
                KafkaStream::Tls(ref mut s) => s.write(buf),
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.flush(),
                KafkaStream::Tls(ref mut s) => s.flush(),
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
            "KafkaConnection {{ id: {}, secured: {}, host: \"{}\", api_versions: {} }}",
            self.id,
            self.stream.is_secured(),
            self.host,
            self.broker_api_versions.is_some()
        )
    }
}

impl KafkaConnection {
    #[cfg(feature = "security")]
    fn map_tls_io_error(err: std::io::Error) -> crate::Error {
        if err.kind() == std::io::ErrorKind::InvalidData {
            let kind = err.kind();
            if let Some(inner) = err.into_inner() {
                match inner.downcast::<rustls::Error>() {
                    Ok(rustls_err) => {
                        return crate::Error::Tls(TlsError::HandshakeFailed(rustls_err));
                    }
                    Err(inner) => return crate::Error::Io(std::io::Error::new(kind, inner)),
                }
            }
            return crate::Error::Io(std::io::Error::new(kind, "TLS error"));
        }
        crate::Error::Io(err)
    }

    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        #[cfg(feature = "security")]
        let r = self
            .stream
            .write(msg)
            .map_err(KafkaConnection::map_tls_io_error);
        #[cfg(not(feature = "security"))]
        let r = self.stream.write(msg).map_err(From::from);
        trace!("Sent {} bytes to: {:?} => {:?}", msg.len(), self, r);
        r
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        #[cfg(feature = "security")]
        let r = (self.stream)
            .read_exact(buf)
            .map_err(KafkaConnection::map_tls_io_error);
        #[cfg(not(feature = "security"))]
        let r = (self.stream).read_exact(buf).map_err(From::from);
        trace!("Read {} bytes from: {:?} => {:?}", buf.len(), self, r);
        r
    }

    pub fn read_exact_alloc(&mut self, size: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size];
        self.read_exact(buffer.as_mut_slice())?;
        Ok(buffer)
    }

    fn shutdown(&mut self) -> Result<()> {
        let r = self.stream.shutdown(Shutdown::Both);
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn negotiate_api_versions(&mut self, correlation_id: i32, client_id: &str) -> Result<()> {
        let req = ApiVersionsRequest::new(correlation_id, client_id);
        let correlation_id = __send_request(self, req)?;
        let resp = __get_response::<ApiVersionsResponse>(self, correlation_id)?;
        let versions = resp.into_broker_api_versions()?;
        let _ = versions.select_highest_common_version(0, &[8])?; // Produce v8
        let _ = versions.select_highest_common_version(1, &[11])?; // Fetch v11
        let _ = versions.select_highest_common_version(2, &[5])?; // ListOffsets v5
        let _ = versions.select_highest_common_version(3, &[8])?; // Metadata v8
        let _ = versions.select_highest_common_version(8, &[7])?; // OffsetCommit v7
        let _ = versions.select_highest_common_version(9, &[5])?; // OffsetFetch v5
        let _ = versions.select_highest_common_version(10, &[2])?; // FindCoordinator v2
        let _ = versions.select_highest_common_version(18, &[2])?; // ApiVersions v2
        self.broker_api_versions = Some(versions);
        Ok(())
    }

    fn authenticate_sasl(
        &mut self,
        correlation_seed: i32,
        client_id: &str,
        sasl: &SaslConfig,
    ) -> Result<()> {
        if let Some(versions) = self.broker_api_versions.as_ref() {
            let _ = versions.select_highest_common_version(17, &[1])?; // SaslHandshake v1
            let _ = versions.select_highest_common_version(36, &[1])?; // SaslAuthenticate v1
        }

        match sasl {
            SaslConfig::Plain(cfg) => {
                let mechanism = "PLAIN";
                let req = SaslHandshakeRequest::new(correlation_seed, client_id, mechanism);
                let correlation_id = __send_request(self, req)?;

                let resp = __get_response::<SaslHandshakeResponse>(self, correlation_id)?;
                resp.check_error()?;
                if !resp.enabled_mechanisms.iter().any(|m| m == mechanism) {
                    return Err(Error::Kafka(KafkaCode::UnsupportedSaslMechanism));
                }

                let token = cfg.initial_response();
                let req = SaslAuthenticateRequest::new(
                    correlation_seed.wrapping_add(1),
                    client_id,
                    token.as_slice(),
                );
                let correlation_id = __send_request(self, req)?;

                let resp = __get_response::<SaslAuthenticateResponse>(self, correlation_id)?;
                if let Err(err) = resp.check_error() {
                    if !resp.error_message.is_empty() {
                        warn!("SASL authentication failed: {}", resp.error_message);
                    }
                    return Err(err);
                }
                Ok(())
            }
        }
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
        security: Option<&SecurityConfig>,
    ) -> Result<KafkaConnection> {
        let stream = TcpStream::connect(host)?;
        let stream = match security {
            Some(security) => {
                KafkaStream::Tls(Box::new(tls::connect(host, stream, rw_timeout, security)?))
            }
            None => KafkaStream::Plain(stream),
        };
        KafkaConnection::from_stream(stream, id, host, rw_timeout)
    }
}

fn __send_request<T: KafkaRequest>(conn: &mut KafkaConnection, request: T) -> Result<i32> {
    let correlation_id = request.correlation_id();
    let mut buffer = Vec::with_capacity(4);
    buffer.extend_from_slice(&[0, 0, 0, 0]);
    request.encode(&mut buffer)?;
    let size = buffer.len() as i32 - 4;
    size.encode(&mut &mut buffer[..])?;
    conn.send(&buffer)?;
    Ok(correlation_id)
}

fn __get_response<T: FromByte>(conn: &mut KafkaConnection, correlation_id: i32) -> Result<T::R> {
    let size = crate::protocol::validate_response_frame_size(__get_response_size(conn)?)?;
    let resp = conn.read_exact_alloc(size)?;
    crate::protocol::validate_response_correlation(&resp, correlation_id)?;
    T::decode_new(&mut Cursor::new(resp))
}

fn __get_response_size(conn: &mut KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    i32::decode_new(&mut Cursor::new(&buf))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[cfg(feature = "security")]
    use super::KafkaStream;
    use super::{__get_response, Connections, KafkaConnection, Pooled};
    use crate::error::Error;
    use crate::protocol::HeaderResponse;

    const HOST: &str = "mock-broker";

    fn connected_streams() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    fn connections_with_stream(stream: TcpStream) -> Connections {
        let mut connections = Connections::new(
            "test-client".to_owned(),
            None,
            std::time::Duration::from_secs(60),
        );
        #[cfg(feature = "security")]
        let stream = KafkaStream::Plain(stream);
        let connection = KafkaConnection::from_stream(stream, 0, HOST, None).unwrap();
        connections.conns.insert(
            HOST.to_owned(),
            Pooled::new(std::time::Instant::now(), connection),
        );
        connections
    }

    #[test]
    fn matching_correlation_keeps_connection_pooled() {
        let (client, mut server) = connected_streams();
        let writer = thread::spawn(move || {
            server.write_all(&4i32.to_be_bytes()).unwrap();
            server.write_all(&42i32.to_be_bytes()).unwrap();
        });
        let mut connections = connections_with_stream(client);

        let response = connections
            .with_conn(HOST, std::time::Instant::now(), |conn| {
                __get_response::<HeaderResponse>(conn, 42)
            })
            .unwrap();

        writer.join().unwrap();
        assert_eq!(response.correlation, 42);
        assert!(connections.conns.contains_key(HOST));
    }

    #[test]
    fn correlation_mismatch_evicts_connection() {
        let (client, mut server) = connected_streams();
        let writer = thread::spawn(move || {
            server.write_all(&4i32.to_be_bytes()).unwrap();
            server.write_all(&7i32.to_be_bytes()).unwrap();
        });
        let mut connections = connections_with_stream(client);

        let error = connections
            .with_conn(HOST, std::time::Instant::now(), |conn| {
                __get_response::<HeaderResponse>(conn, 42)
            })
            .unwrap_err();

        writer.join().unwrap();
        assert!(matches!(
            error,
            Error::CorrelationIdMismatch {
                expected: 42,
                actual: 7
            }
        ));
        assert!(!connections.conns.contains_key(HOST));
    }

    #[test]
    fn incomplete_response_evicts_connection() {
        let (client, mut server) = connected_streams();
        let writer = thread::spawn(move || {
            server.write_all(&8i32.to_be_bytes()).unwrap();
            server.write_all(&[0, 0]).unwrap();
        });
        let mut connections = connections_with_stream(client);

        let error = connections
            .with_conn(HOST, std::time::Instant::now(), |conn| {
                __get_response::<HeaderResponse>(conn, 42)
            })
            .unwrap_err();

        writer.join().unwrap();
        assert!(matches!(error, Error::Io(_)));
        assert!(!connections.conns.contains_key(HOST));
    }
}
