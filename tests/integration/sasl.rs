use super::*;

use kafkang::client::{KafkaClient, SaslConfig};

#[test]
fn test_sasl_plaintext_load_metadata_succeeds() {
    if secure_mode() != "sasl_plaintext" {
        return;
    }

    let _ = tracing_subscriber::fmt::try_init();
    let mut client = new_kafka_client();
    client.load_metadata_all().unwrap();
}

#[test]
fn test_sasl_plaintext_wrong_password_fails() {
    if secure_mode() != "sasl_plaintext" {
        return;
    }

    let _ = tracing_subscriber::fmt::try_init();

    let hosts = vec![bootstrap_host().to_owned()];
    let mut client = KafkaClient::new(hosts);
    client.set_sasl_config(Some(SaslConfig::plain("kafkang", "wrong-password")));

    let err = client.load_metadata_all().unwrap_err();
    assert!(
        matches!(
            err,
            kafkang::Error::Kafka(kafkang::error::KafkaCode::SaslAuthenticationFailed)
        ),
        "expected SASL auth failure, got {err:?}"
    );
}
