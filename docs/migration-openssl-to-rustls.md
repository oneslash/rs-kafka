# Migrating TLS from OpenSSL to rustls

This crate’s TLS support (`feature = "security"`) used to be implemented with OpenSSL (`openssl` / `openssl-sys`). It is now implemented with `rustls`, and the OpenSSL dependency has been removed from this repository.

This guide shows how to migrate existing OpenSSL-based client setup code to the new rustls-based API while keeping the rest of your Kafka usage the same.


## What changed (high level)

- The TLS backend is now `rustls` (synchronous, `std::net::TcpStream`-based).
- `SecurityConfig::new` no longer accepts `openssl::ssl::SslConnector`; it now accepts a `kafkang::client::TlsConnector`.
- The public error variant for TLS failures is now `kafkang::Error::Tls(..)` (replacing `kafkang::Error::Ssl(..)`).
- Hostname verification is still supported via `SecurityConfig::with_hostname_verification(bool)`:
  - Default is `true` (recommended).
  - Setting it to `false` disables only the hostname check; certificate chain validation still runs.


## Cargo.toml changes

If you only depended on OpenSSL for this crate’s TLS support, you can remove it from your application:

    # Remove these if they were only used for kafkang TLS:
    # openssl = ...

You generally do not need to change crate features, because `security` remains enabled by default:

    [dependencies]
    kafkang = "0.1.0"

If you previously disabled default features, ensure `security` is enabled if you want TLS:

    [dependencies]
    kafkang = { version = "0.1.0", default-features = false, features = ["security"] }


## Code migration

### 1) Replace OpenSSL connector construction with `TlsConnector`

Before (OpenSSL-based; pre-migration):

    use kafkang::client::{KafkaClient, SecurityConfig};
    use openssl::ssl::{SslConnector, SslMethod};

    let openssl = SslConnector::builder(SslMethod::tls()).unwrap().build();
    let security = SecurityConfig::new(openssl);
    let mut client = KafkaClient::new_secure(vec!["localhost:9094".to_owned()], security);

After (rustls-based):

    use kafkang::client::{KafkaClient, SecurityConfig, TlsConnector};

    let connector = TlsConnector::default();
    let security = SecurityConfig::new(connector);
    let mut client = KafkaClient::new_secure(vec!["localhost:9094".to_owned()], security);

`TlsConnector::default()` trusts native OS root CAs (when available) and falls back to the bundled Mozilla root set (`webpki-roots`) if native roots are unavailable.


### 2) Trust a private CA (self-signed / internal PKI)

If your broker is signed by a private CA, append that CA certificate (or bundle) as PEM:

    use kafkang::client::{KafkaClient, SecurityConfig, TlsConnector};

    let connector = TlsConnector::builder()
        .add_ca_certs_pem_file("ca.crt.pem")
        .unwrap()
        .build()
        .unwrap();

    let mut client = KafkaClient::new_secure(
        vec!["localhost:9094".to_owned()],
        SecurityConfig::new(connector),
    );


### 3) Configure mutual TLS (mTLS / client certificates)

If the broker requires a client certificate, configure it on the connector builder:

    use kafkang::client::{KafkaClient, SecurityConfig, TlsConnector};

    let connector = TlsConnector::builder()
        .add_ca_certs_pem_file("ca.crt.pem")
        .unwrap()
        .with_client_auth_pem_files("client.crt.pem", "client.key.pem")
        .unwrap()
        .build()
        .unwrap();

    let mut client = KafkaClient::new_secure(
        vec!["localhost:9094".to_owned()],
        SecurityConfig::new(connector),
    );

Notes:

- The client cert file may contain a certificate chain (multiple PEM blocks).
- PEM private keys are supported; PKCS#8 is a good default if you have a choice.


### 4) Keep hostname verification enabled (recommended), or disable it (insecure)

Hostname verification is enabled by default:

    let security = SecurityConfig::new(TlsConnector::default());

To disable hostname verification (insecure; not recommended):

    let security = SecurityConfig::new(TlsConnector::default())
        .with_hostname_verification(false);


### 5) Connect to an IP address but verify a DNS name (SNI override)

If you connect to an IP (e.g. `127.0.0.1:9094`) but the broker certificate is issued for `localhost`, set an explicit server name for SNI/verification:

    use kafkang::client::{KafkaClient, SecurityConfig, TlsConnector};

    let connector = TlsConnector::builder()
        .add_ca_certs_pem_file("ca.crt.pem")
        .unwrap()
        .build()
        .unwrap();

    let mut client = KafkaClient::new_secure(
        vec!["127.0.0.1:9094".to_owned()],
        SecurityConfig::new(connector).with_server_name("localhost"),
    );


## Error handling changes

If you previously matched on `kafkang::Error::Ssl(..)`, update your code to handle `kafkang::Error::Tls(..)`:

    match client.load_metadata_all() {
        Ok(_) => {}
        Err(kafkang::Error::Tls(e)) => {
            eprintln!("TLS error: {e}");
        }
        Err(e) => {
            eprintln!("Kafka error: {e}");
        }
    }


## Common migration gotchas

- **Broker certificate must have SANs**: rustls/webpki requires the broker certificate to contain a Subject Alternative Name for the hostname you verify (for example `DNS:localhost`). Certificates that rely only on the legacy Common Name field will fail hostname verification.
- **Wrong-host / wrong-CA errors are now surfaced**: the client now returns the last connection/TLS error when metadata fetch fails across all hosts, rather than only returning a generic “no host reachable”.
