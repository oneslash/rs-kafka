#[cfg(feature = "integration_tests")]
extern crate kafkang;

#[cfg(feature = "integration_tests")]
extern crate rand;

#[cfg(feature = "integration_tests")]
#[macro_use]
extern crate lazy_static;

#[cfg(feature = "integration_tests")]
mod integration {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use tracing::debug;

    use kafkang::client::{
        Compression, GroupOffsetStorage, KafkaClient, SaslConfig, SecurityConfig, TlsConnector,
    };

    mod client;
    mod consumer_producer;
    mod sasl;
    mod tls;

    pub const LOCAL_KAFKA_BOOTSTRAP_HOST_PLAINTEXT: &str = "localhost:9092";
    pub const LOCAL_KAFKA_BOOTSTRAP_HOST_TLS: &str = "localhost:9094";
    pub const LOCAL_KAFKA_BOOTSTRAP_HOST_TLS_IP: &str = "127.0.0.1:9094";
    pub const LOCAL_KAFKA_BOOTSTRAP_HOST_SASL_PLAINTEXT: &str = "localhost:9096";

    pub const TEST_TOPIC_NAME: &str = "kafka-rust-test";
    pub const TEST_TOPIC_NAME_2: &str = "kafka-rust-test2";
    pub const TEST_GROUP_NAME: &str = "kafka-rust-tester";
    pub const TEST_TOPIC_PARTITIONS: [i32; 2] = [0, 1];
    pub const KAFKA_CONSUMER_OFFSETS_TOPIC_NAME: &str = "__consumer_offsets";

    // env vars
    const KAFKA_CLIENT_SECURE: &str = "KAFKA_CLIENT_SECURE";
    const KAFKA_CLIENT_COMPRESSION: &str = "KAFKA_CLIENT_COMPRESSION";

    lazy_static! {
        static ref COMPRESSIONS: HashMap<&'static str, Compression> = {
            let mut m = HashMap::new();

            m.insert("", Compression::NONE);
            m.insert("none", Compression::NONE);
            m.insert("NONE", Compression::NONE);

            m.insert("snappy", Compression::SNAPPY);
            m.insert("SNAPPY", Compression::SNAPPY);

            m.insert("gzip", Compression::GZIP);
            m.insert("GZIP", Compression::GZIP);

            m
        };
    }

    pub(crate) fn secure_mode() -> String {
        std::env::var(KAFKA_CLIENT_SECURE).unwrap_or_default()
    }

    pub(crate) fn bootstrap_host() -> &'static str {
        match secure_mode().as_str() {
            "" => LOCAL_KAFKA_BOOTSTRAP_HOST_PLAINTEXT,
            "tls" | "mtls" => LOCAL_KAFKA_BOOTSTRAP_HOST_TLS,
            "sasl_plaintext" => LOCAL_KAFKA_BOOTSTRAP_HOST_SASL_PLAINTEXT,
            other => panic!("Unsupported KAFKA_CLIENT_SECURE={other:?}"),
        }
    }

    pub(crate) fn tls_fixture_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
    }

    pub(crate) fn ca_cert_path() -> PathBuf {
        tls_fixture_dir().join("ca.crt.pem")
    }

    pub(crate) fn client_cert_path() -> PathBuf {
        tls_fixture_dir().join("client.crt.pem")
    }

    pub(crate) fn client_key_path() -> PathBuf {
        tls_fixture_dir().join("client.key.pem")
    }

    /// Constructs a Kafka client for the integration tests, and loads its metadata so it is ready
    /// to use.
    pub(crate) fn new_ready_kafka_client() -> KafkaClient {
        let mut client = new_kafka_client();
        client.load_metadata_all().unwrap();
        client
    }

    /// Constructs a Kafka client for the integration tests.
    pub(crate) fn new_kafka_client() -> KafkaClient {
        let hosts = vec![bootstrap_host().to_owned()];

        let mut client = if let Some(security_config) = new_security_config() {
            KafkaClient::new_secure(hosts, security_config)
        } else {
            KafkaClient::new(hosts)
        };

        if secure_mode() == "sasl_plaintext" {
            client.set_sasl_config(Some(SaslConfig::plain("kafkang", "kafkang-secret")));
        }

        client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));

        let compression = std::env::var(KAFKA_CLIENT_COMPRESSION).unwrap_or_default();
        let compression = COMPRESSIONS.get(&*compression).unwrap();

        client.set_compression(*compression);
        debug!("Constructing client: {:?}", client);

        client
    }

    pub(crate) fn new_security_config() -> Option<SecurityConfig> {
        match secure_mode().as_str() {
            "" => None,
            "tls" => Some(tls_security_config()),
            "mtls" => Some(mtls_security_config()),
            "sasl_plaintext" => None,
            other => panic!("Unsupported KAFKA_CLIENT_SECURE={other:?}"),
        }
    }

    fn tls_security_config() -> SecurityConfig {
        let connector = TlsConnector::builder()
            .add_ca_certs_pem_file(ca_cert_path())
            .unwrap()
            .build()
            .unwrap();

        SecurityConfig::new(connector)
    }

    fn mtls_security_config() -> SecurityConfig {
        let connector = TlsConnector::builder()
            .add_ca_certs_pem_file(ca_cert_path())
            .unwrap()
            .with_client_auth_pem_files(client_cert_path(), client_key_path())
            .unwrap()
            .build()
            .unwrap();

        SecurityConfig::new(connector)
    }
}
