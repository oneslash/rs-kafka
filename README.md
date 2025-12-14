# Kafkang – Kafka Rust Client

## Project Status

This project is a fork of the original kafka-rust project; because it was not well maintained and still in alpha, I forked it and created **kafkang**, now hosted at https://github.com/oneslash/rs-kafka.

This project has new features such as:

- Rustls instead of OpenSSL
- Supports only maintained versions of Kafka
- Rust 2024

## Documentation

- This library is primarily documented through examples in its [API documentation](https://docs.rs/kafkang/).
- Documentation about Kafka itself can be found at [its project home page](http://kafka.apache.org/).

## Installation

This crate works with Cargo and is on
[crates.io](https://crates.io/crates/kafkang). The API is currently
under active development although we do follow semantic versioning (but
expect the version number to grow quickly).

```toml
[dependencies]
kafkang = "0.1.0"
```

To build **kafkang** the usual `cargo build` should suffice. The crate
supports various features which can be turned off at compile time.
See kafkang's `Cargo.toml` and [cargo's documentation](http://doc.crates.io/manifest.html#the-features-section).

## Rust version

This crate targets the Rust 2024 edition and requires Rust **1.85.0 or newer** (our MSRV). CI is pinned to that toolchain to keep builds reproducible.

## TLS (SSL)

TLS support is enabled by default via the `security` feature and is implemented using `rustls` (no OpenSSL dependency in default builds).

At a high level:

- Use `KafkaClient::new_secure(..)` and pass a `SecurityConfig`.
- Construct a `TlsConnector` using `TlsConnector::default()` (native roots, with a bundled-root fallback), or use `TlsConnector::builder()` to append a custom CA bundle and/or configure client authentication (mTLS).
- Migration guide for older OpenSSL-based code: `docs/migration-openssl-to-rustls.md`.

## SASL authentication

SASL authentication is configured per broker connection via `KafkaClient::set_sasl_config(..)`.

```rust
use kafkang::client::{KafkaClient, SaslConfig};

let mut client = KafkaClient::new(vec!["localhost:9096".to_owned()]);
client.set_sasl_config(Some(SaslConfig::plain("kafkang", "kafkang-secret")));
client.load_metadata_all().unwrap();
```

Security note: SASL/PLAIN does not encrypt credentials; combine it with TLS (`KafkaClient::new_secure`)
in production. See `docs/sasl.md`.

## Supported Kafka versions

`kafkang` is tested in CI against the following Kafka versions:

- 3.8.1
- 3.9.1
- 4.0.1
- 4.1.0

## Examples

As mentioned, the [cargo generated documentation](https://docs.rs/kafkang/) contains some examples.
Further, standalone, compilable example programs are provided in the
[examples directory of the repository](https://github.com/oneslash/rs-kafka/tree/master/examples).

## Consumer

This is a higher-level consumer API for Kafka and is provided by the
module `kafkang::consumer`. It provides convenient offset management
support on behalf of a specified group. This is the API a client
application of this library wants to use for receiving messages from
Kafka.

## Producer

This is a higher-level producer API for Kafka and is provided by the
module `kafkang::producer`. It provides convenient automatic partition
assignment capabilities through partitioners. This is the API a
client application of this library wants to use for sending messages
to Kafka.

## KafkaClient

`KafkaClient` in the `kafkang::client` module is the central point of
this API. However, this is a mid-level abstraction for Kafka rather
suitable for building higher-level APIs. Applications typically want
to use the already mentioned `Consumer` and `Producer`.
Nevertheless, the main features or `KafkaClient` are:

- Loading metadata
- Fetching topic offsets
- Sending messages
- Fetching messages
- Committing a consumer group's offsets
- Fetching a consumer group's offsets

## Bugs / Features / Contributing

There's still a lot of room for improvement on `kafkang`.
Not everything works right at the moment, and testing coverage could be better.
**Use it in production at your own risk.** Have a look at the
[issue tracker](https://github.com/oneslash/rs-kafka/issues) and feel free
to contribute by reporting new problems or contributing to existing
ones. Any constructive feedback is warmly welcome!

As usually with open source, don't hesitate to fork the repo and
submit a pull request if you see something to be changed. We'll be
happy to see `kafkang` improving over time.

### Integration tests

When working locally, the integration tests require that you must have
Docker (1.10.0+) and docker-compose (1.6.0+) installed and run the tests via the
included `run-all-tests` script in the `tests` directory. See the `run-all-tests`
script itself for details on its usage.

## Creating a topic

Note unless otherwise explicitly stated in the documentation, this
library will ignore requests to topics which it doesn't know about.
In particular it will not try to retrieve messages from
non-existing/unknown topics. (This behavior is very likely to change
in future version of this library.)

Given a local kafka server installation you can create topics with the
following command (where `kafka-topics.sh` is part of the Kafka
distribution):

```
kafka-topics.sh --topic my-topic --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1
```

Zookeeper will be removed in the next major kafka release. Using `--bootstrap-server` to be more ready.

```
kafka-topics.sh --topic my-topic --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

See also [Kafka's quickstart guide](https://kafka.apache.org/documentation.html#quickstart)
for more information.

## Alternative/Related projects

- [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) is an emerging alternative Kafka client library for Rust based on
  `librdkafka`. rust-rdkafka provides a safe Rust interface to librdkafka.
- [kafkang](https://github.com/kafka-rust/kafka-rust) - This fork’s repository (original code based on kafka-rust).
