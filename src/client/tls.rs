//! TLS (Transport Layer Security) configuration and stream setup for the synchronous client.
//!
//! This module is compiled only when the `security` feature is enabled.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, RootCertStore};

use crate::error::{Result, TlsError};

pub(crate) type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

#[derive(Debug)]
struct NoHostnameVerification {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for NoHostnameVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(v) => Ok(v),
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// A TLS connector configuration.
///
/// This type is intentionally backend-agnostic: callers configure the expected trust roots,
/// optional client authentication (mTLS), and other knobs without interacting with rustls types.
#[derive(Clone)]
pub struct TlsConnector {
    config_verify_hostname: Arc<ClientConfig>,
    config_no_hostname_verification: Arc<ClientConfig>,
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TlsConnector {{ .. }}")
    }
}

impl Default for TlsConnector {
    fn default() -> Self {
        // Default configuration: trust native roots when available, otherwise fall back to the
        // Mozilla root set from `webpki-roots`.
        //
        // Any failure is treated as a hard error in the builder; here we choose a configuration
        // that should always be buildable.
        TlsConnectorBuilder::new()
            .build()
            .expect("default TLS connector must be buildable")
    }
}

impl TlsConnector {
    #[must_use]
    pub fn builder() -> TlsConnectorBuilder {
        TlsConnectorBuilder::new()
    }

    pub(crate) fn client_config(&self, verify_hostname: bool) -> Arc<ClientConfig> {
        if verify_hostname {
            self.config_verify_hostname.clone()
        } else {
            self.config_no_hostname_verification.clone()
        }
    }
}

/// Builder for [`TlsConnector`].
#[derive(Debug)]
pub struct TlsConnectorBuilder {
    include_native_roots: bool,
    include_webpki_roots_if_no_native_roots: bool,
    extra_root_certs: Vec<CertificateDer<'static>>,
    client_cert_chain: Option<Vec<CertificateDer<'static>>>,
    client_key: Option<PrivateKeyDer<'static>>,
    alpn_protocols: Vec<Vec<u8>>,
}

impl Default for TlsConnectorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TlsConnectorBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            include_native_roots: true,
            include_webpki_roots_if_no_native_roots: true,
            extra_root_certs: Vec::new(),
            client_cert_chain: None,
            client_key: None,
            alpn_protocols: Vec::new(),
        }
    }

    /// Whether to include system (platform) root CAs. Defaults to `true`.
    #[must_use]
    pub fn with_native_roots(mut self, include: bool) -> Self {
        self.include_native_roots = include;
        self
    }

    /// Whether to fall back to the Mozilla root set (`webpki-roots`) when system roots cannot be
    /// loaded (or are empty). Defaults to `true`.
    #[must_use]
    pub fn with_webpki_roots_fallback(mut self, include: bool) -> Self {
        self.include_webpki_roots_if_no_native_roots = include;
        self
    }

    /// Append one or more PEM-encoded CA certificates to the trust store.
    pub fn add_ca_certs_pem(mut self, pem: &[u8]) -> Result<Self> {
        let certs = load_certs_from_pem(pem)
            .map_err(|e| crate::Error::Tls(TlsError::CaCertParseFailed(e)))?;
        if certs.is_empty() {
            return Err(crate::Error::Tls(TlsError::NoCertificatesFound));
        }
        self.extra_root_certs.extend(certs);
        Ok(self)
    }

    /// Append PEM-encoded CA certificates from a file path to the trust store.
    pub fn add_ca_certs_pem_file(mut self, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let certs = load_certs_from_pem_file(path)
            .map_err(|e| crate::Error::Tls(TlsError::CaCertReadFailed(path.to_owned(), e)))?;
        if certs.is_empty() {
            return Err(crate::Error::Tls(TlsError::NoCertificatesFound));
        }
        self.extra_root_certs.extend(certs);
        Ok(self)
    }

    /// Configure a client certificate chain and private key for mutual TLS (mTLS).
    pub fn with_client_auth_pem(mut self, cert_pem: &[u8], key_pem: &[u8]) -> Result<Self> {
        let certs = load_certs_from_pem(cert_pem)
            .map_err(|e| crate::Error::Tls(TlsError::ClientCertParseFailed(e)))?;
        if certs.is_empty() {
            return Err(crate::Error::Tls(TlsError::NoCertificatesFound));
        }

        let key = load_private_key_from_pem(key_pem)
            .map_err(|e| crate::Error::Tls(TlsError::ClientKeyParseFailed(e)))?
            .ok_or_else(|| crate::Error::Tls(TlsError::NoPrivateKeyFound))?;

        self.client_cert_chain = Some(certs);
        self.client_key = Some(key);
        Ok(self)
    }

    /// Configure a client certificate chain and private key from file paths for mutual TLS (mTLS).
    pub fn with_client_auth_pem_files(
        mut self,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let cert_path = cert_path.as_ref();
        let key_path = key_path.as_ref();

        let certs = load_certs_from_pem_file(cert_path).map_err(|e| {
            crate::Error::Tls(TlsError::ClientCertReadFailed(cert_path.to_owned(), e))
        })?;
        if certs.is_empty() {
            return Err(crate::Error::Tls(TlsError::NoCertificatesFound));
        }

        let key = load_private_key_from_pem_file(key_path)
            .map_err(|e| crate::Error::Tls(TlsError::ClientKeyReadFailed(key_path.to_owned(), e)))?
            .ok_or_else(|| crate::Error::Tls(TlsError::NoPrivateKeyFound))?;

        self.client_cert_chain = Some(certs);
        self.client_key = Some(key);
        Ok(self)
    }

    /// Configure ALPN protocols to advertise.
    ///
    /// Kafka does not require ALPN today, but this can be useful if brokers/proxies introduce it.
    #[must_use]
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    pub fn build(self) -> Result<TlsConnector> {
        let roots = build_root_store(
            self.include_native_roots,
            self.include_webpki_roots_if_no_native_roots,
            &self.extra_root_certs,
        )?;

        let roots = Arc::new(roots);
        let webpki_verifier = WebPkiServerVerifier::builder(roots.clone())
            .build()
            .map_err(|e| crate::Error::Tls(TlsError::VerifierBuildFailed(Box::new(e))))?;

        let client_key_for_verify = self.client_key.as_ref().map(PrivateKeyDer::clone_key);

        let mut config_verify_hostname = finish_client_config(
            ClientConfig::builder().with_webpki_verifier(webpki_verifier.clone()),
            self.client_cert_chain.clone(),
            client_key_for_verify,
        )?;

        let mut config_no_hostname_verification = finish_client_config(
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoHostnameVerification {
                    inner: webpki_verifier,
                })),
            self.client_cert_chain,
            self.client_key,
        )?;

        config_verify_hostname.alpn_protocols = self.alpn_protocols.clone();
        config_no_hostname_verification.alpn_protocols = self.alpn_protocols;

        Ok(TlsConnector {
            config_verify_hostname: Arc::new(config_verify_hostname),
            config_no_hostname_verification: Arc::new(config_no_hostname_verification),
        })
    }
}

fn build_root_store(
    include_native_roots: bool,
    include_webpki_roots_if_no_native_roots: bool,
    extra_root_certs: &[CertificateDer<'static>],
) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();

    // Precedence:
    // 1) User-provided roots (append).
    // 2) Native roots (default).
    // 3) webpki-roots fallback if native roots are unavailable.
    if !extra_root_certs.is_empty() {
        roots.add_parsable_certificates(extra_root_certs.iter().cloned());
    }

    if include_native_roots {
        match rustls_native_certs::load_native_certs() {
            Ok(certs) => {
                let (added, invalid) = roots.add_parsable_certificates(certs);
                debug!(
                    "Loaded native root certificates: added={}, ignored_invalid={}",
                    added, invalid
                );
            }
            Err(e) => {
                debug!("Failed to load native root certificates: {e}");
            }
        }
    }

    if roots.is_empty() && include_webpki_roots_if_no_native_roots {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    if roots.is_empty() {
        return Err(crate::Error::Tls(TlsError::NoRootCertificates));
    }

    Ok(roots)
}

fn finish_client_config(
    builder: rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert>,
    client_cert_chain: Option<Vec<CertificateDer<'static>>>,
    client_key: Option<PrivateKeyDer<'static>>,
) -> Result<ClientConfig> {
    let mut config = match (client_cert_chain, client_key) {
        (Some(chain), Some(key)) => builder
            .with_client_auth_cert(chain, key)
            .map_err(|e| crate::Error::Tls(TlsError::ClientAuthConfigFailed(Box::new(e))))?,
        (None, None) => builder.with_no_client_auth(),
        _ => return Err(crate::Error::Tls(TlsError::IncompleteClientAuthConfig)),
    };
    config.enable_sni = true;
    Ok(config)
}

fn load_certs_from_pem_file(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader).collect()
}

fn load_private_key_from_pem_file(path: &Path) -> io::Result<Option<PrivateKeyDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
}

fn load_certs_from_pem(pem: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(pem);
    rustls_pemfile::certs(&mut reader).collect()
}

fn load_private_key_from_pem(pem: &[u8]) -> io::Result<Option<PrivateKeyDer<'static>>> {
    let mut reader = BufReader::new(pem);
    rustls_pemfile::private_key(&mut reader)
}

/// Security relevant configuration options for `KafkaClient`.
#[derive(Clone)]
pub struct SecurityConfig {
    connector: TlsConnector,
    verify_hostname: bool,
    server_name_override: Option<String>,
}

impl SecurityConfig {
    #[must_use]
    pub fn new(connector: TlsConnector) -> Self {
        SecurityConfig {
            connector,
            verify_hostname: true,
            server_name_override: None,
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

    /// Overrides the server name used for SNI and hostname verification.
    ///
    /// This is useful when connecting to an IP address but verifying a DNS name.
    #[must_use]
    pub fn with_server_name(self, server_name: impl Into<String>) -> SecurityConfig {
        SecurityConfig {
            server_name_override: Some(server_name.into()),
            ..self
        }
    }

    pub(crate) fn connector(&self) -> &TlsConnector {
        &self.connector
    }

    pub(crate) fn verify_hostname(&self) -> bool {
        self.verify_hostname
    }

    pub(crate) fn server_name_override(&self) -> Option<&str> {
        self.server_name_override.as_deref()
    }
}

impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SecurityConfig {{ verify_hostname: {}, server_name_override: {} }}",
            self.verify_hostname,
            self.server_name_override
                .as_deref()
                .unwrap_or("<none>")
        )
    }
}

pub(crate) fn connect(
    host: &str,
    tcp: TcpStream,
    rw_timeout: Option<std::time::Duration>,
    security: &SecurityConfig,
) -> Result<TlsStream> {
    if let Some(timeout) = rw_timeout {
        // Ensure the handshake inherits the same timeouts as the rest of the connection.
        tcp.set_read_timeout(Some(timeout))?;
        tcp.set_write_timeout(Some(timeout))?;
    }

    let server_name = security
        .server_name_override()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| extract_host(host).to_owned());

    let server_name = ServerName::try_from(server_name)
        .map_err(|_| crate::Error::Tls(TlsError::InvalidServerName))?;

    let config = security
        .connector()
        .client_config(security.verify_hostname());

    let conn = rustls::ClientConnection::new(config, server_name)
        .map_err(|e| crate::Error::Tls(TlsError::ClientConfigFailed(Box::new(e))))?;

    let mut tls_stream = rustls::StreamOwned::new(conn, tcp);

    // Force the handshake now so errors are surfaced during connection establishment.
    while tls_stream.conn.is_handshaking() {
        match tls_stream.conn.complete_io(&mut tls_stream.sock) {
            Ok(_) => {}
            Err(io_err) if io_err.kind() == io::ErrorKind::InvalidData => {
                let kind = io_err.kind();
                if let Some(inner) = io_err.into_inner() {
                    match inner.downcast::<rustls::Error>() {
                        Ok(rustls_err) => {
                            return Err(crate::Error::Tls(TlsError::HandshakeFailed(rustls_err)));
                        }
                        Err(inner) => return Err(crate::Error::Io(io::Error::new(kind, inner))),
                    }
                }
                return Err(crate::Error::Io(io::Error::new(
                    kind,
                    "TLS handshake failed",
                )));
            }
            Err(io_err) => return Err(crate::Error::Io(io_err)),
        }
    }

    Ok(tls_stream)
}

fn extract_host(host: &str) -> &str {
    // Inputs are expected to be "host:port" (or "[ipv6]:port").
    // The returned host is used for SNI and hostname verification.
    if let Some(rest) = host.strip_prefix('[') {
        if let Some((h, _)) = rest.split_once(']') {
            return h;
        }
    }

    match host.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CA_CERT_PEM: &[u8] = include_bytes!("../../tests/fixtures/tls/ca.crt.pem");
    const CLIENT_CERT_PEM: &[u8] = include_bytes!("../../tests/fixtures/tls/client.crt.pem");
    const CLIENT_KEY_PEM: &[u8] = include_bytes!("../../tests/fixtures/tls/client.key.pem");

    #[test]
    fn extract_host_handles_ipv4_and_ipv6() {
        assert_eq!(extract_host("localhost:9094"), "localhost");
        assert_eq!(extract_host("127.0.0.1:9094"), "127.0.0.1");
        assert_eq!(extract_host("[::1]:9094"), "::1");
    }

    #[test]
    fn tls_connector_builder_parses_ca_pem() {
        let _connector = TlsConnector::builder()
            .with_native_roots(false)
            .with_webpki_roots_fallback(false)
            .add_ca_certs_pem(CA_CERT_PEM)
            .unwrap()
            .build()
            .unwrap();
    }

    #[test]
    fn tls_connector_builder_parses_client_auth_pem() {
        let _connector = TlsConnector::builder()
            .with_native_roots(false)
            .with_webpki_roots_fallback(false)
            .add_ca_certs_pem(CA_CERT_PEM)
            .unwrap()
            .with_client_auth_pem(CLIENT_CERT_PEM, CLIENT_KEY_PEM)
            .unwrap()
            .build()
            .unwrap();
    }
}
