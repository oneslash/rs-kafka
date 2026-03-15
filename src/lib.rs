//! Clients for comunicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `kafkang::producer::Producer` - for sending message to Kafka
//! - `kafkang::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `kafkang::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more responsibility
//!
//! See module level documentation corresponding to each client individually.
#![recursion_limit = "128"]
#![cfg_attr(all(feature = "nightly", kafka_rust_nightly), feature(test))]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::too_many_lines
)]

#[macro_use]
extern crate tracing;

#[cfg(feature = "snappy")]
extern crate snap;

#[cfg(all(test, feature = "nightly", kafka_rust_nightly))]
extern crate test;

pub mod client;
mod client_internals;
mod codecs;
mod compression;
pub mod consumer;
pub mod error;
pub mod producer;
mod protocol;
mod utils;

pub use self::error::{Error, Result};
