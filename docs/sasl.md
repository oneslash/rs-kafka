# SASL authentication

`kafkang` supports SASL authentication during broker connection establishment.

## Security note

SASL provides **authentication**, not encryption. In production, combine SASL/PLAIN with TLS
(`KafkaClient::new_secure`) to avoid sending credentials in cleartext.

## Supported mechanisms

- SASL/PLAIN

## Usage

```rust
use kafkang::client::{KafkaClient, SaslConfig};

let mut client = KafkaClient::new(vec!["localhost:9096".to_owned()]);
client.set_sasl_config(Some(SaslConfig::plain("kafkang", "kafkang-secret")));
client.load_metadata_all().unwrap();
```

## Example

Run the standalone example:

```
cargo run --example example-sasl-plain -- --brokers localhost:9096 --username kafkang --password kafkang-secret
```

## Integration tests (Docker)

From `tests/`:

```
./run-all-tests 4.1.0
```

To run only the SASL mode:

```
SECURES=":sasl_plaintext" ./run-all-tests 4.1.0
```

