use super::*;

use kafka::client::{KafkaClient, SecurityConfig, TlsConnector};

#[test]
fn test_tls_wrong_ca_fails() {
    if secure_mode() != "tls" {
        return;
    }

    let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST_TLS.to_owned()];
    let connector = TlsConnector::default();

    let mut client = KafkaClient::new_secure(hosts, SecurityConfig::new(connector));
    let err = client.load_metadata_all().unwrap_err();
    assert!(
        matches!(err, kafka::Error::Tls(_)),
        "expected TLS error, got {err:?}"
    );
}

#[test]
fn test_tls_wrong_hostname_fails() {
    if secure_mode() != "tls" {
        return;
    }

    let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST_TLS_IP.to_owned()];
    let connector = TlsConnector::builder()
        .add_ca_certs_pem_file(ca_cert_path())
        .unwrap()
        .build()
        .unwrap();

    let mut client = KafkaClient::new_secure(hosts, SecurityConfig::new(connector));
    let err = client.load_metadata_all().unwrap_err();
    assert!(
        matches!(err, kafka::Error::Tls(_)),
        "expected TLS error, got {err:?}"
    );
}

#[test]
fn test_tls_server_name_override_allows_ip_connect() {
    if secure_mode() != "tls" {
        return;
    }

    let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST_TLS_IP.to_owned()];
    let connector = TlsConnector::builder()
        .add_ca_certs_pem_file(ca_cert_path())
        .unwrap()
        .build()
        .unwrap();

    let mut client = KafkaClient::new_secure(
        hosts,
        SecurityConfig::new(connector).with_server_name("localhost"),
    );
    client.load_metadata_all().unwrap();
}

#[test]
fn test_mtls_missing_client_cert_fails() {
    if secure_mode() != "mtls" {
        return;
    }

    let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST_TLS.to_owned()];
    let connector = TlsConnector::builder()
        .add_ca_certs_pem_file(ca_cert_path())
        .unwrap()
        .build()
        .unwrap();

    let mut client = KafkaClient::new_secure(hosts, SecurityConfig::new(connector));
    let err = client.load_metadata_all().unwrap_err();
    assert!(
        matches!(err, kafka::Error::Tls(_)),
        "expected TLS error, got {err:?}"
    );
}
