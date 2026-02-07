use std::io::{Read, Write};

use crate::codecs::{AsStrings, FromByte, ToByte};
use crate::error::Result;

use super::{API_KEY_METADATA, HeaderRequest, HeaderResponse};

pub const METADATA_API_VERSION: i16 = 8;

#[derive(Debug)]
pub struct MetadataRequest<'a, T> {
    pub header: HeaderRequest<'a>,
    pub topics: &'a [T],
    pub allow_auto_topic_creation: i8,
    pub include_cluster_authorized_operations: i8,
    pub include_topic_authorized_operations: i8,
}

impl<'a, T: AsRef<str>> MetadataRequest<'a, T> {
    pub fn new(correlation_id: i32, client_id: &'a str, topics: &'a [T]) -> MetadataRequest<'a, T> {
        MetadataRequest {
            header: HeaderRequest::new(
                API_KEY_METADATA,
                METADATA_API_VERSION,
                correlation_id,
                client_id,
            ),
            topics,
            allow_auto_topic_creation: 0,
            include_cluster_authorized_operations: 0,
            include_topic_authorized_operations: 0,
        }
    }
}

impl<'a, T: AsRef<str> + 'a> ToByte for MetadataRequest<'a, T> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.header.encode(buffer)?;
        // For metadata requests, a null topic array means "all topics".
        // With modern broker versions, an empty (non-null) array can yield
        // an empty response set instead.
        if self.topics.is_empty() {
            (-1i32).encode(buffer)?;
        } else {
            AsStrings(self.topics).encode(buffer)?;
        }
        if self.header.api_version >= 4 {
            self.allow_auto_topic_creation.encode(buffer)?;
        }
        if self.header.api_version >= 8 {
            self.include_cluster_authorized_operations.encode(buffer)?;
            self.include_topic_authorized_operations.encode(buffer)?;
        }
        Ok(())
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct MetadataResponse {
    pub header: HeaderResponse,
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: String,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
    pub cluster_authorized_operations: i32,
}

#[derive(Default, Debug)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: String,
}

#[derive(Default, Debug)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub is_internal: i8,
    pub partitions: Vec<PartitionMetadata>,
    pub topic_authorized_operations: i32,
}

#[derive(Default, Debug)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl FromByte for MetadataResponse {
    type R = MetadataResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.throttle_time_ms.decode(buffer),
            self.brokers.decode(buffer),
            self.cluster_id.decode(buffer),
            self.controller_id.decode(buffer),
            self.topics.decode(buffer),
            self.cluster_authorized_operations.decode(buffer)
        )
    }
}

impl FromByte for BrokerMetadata {
    type R = BrokerMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.node_id.decode(buffer),
            self.host.decode(buffer),
            self.port.decode(buffer),
            self.rack.decode(buffer)
        )
    }
}

impl FromByte for TopicMetadata {
    type R = TopicMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.error.decode(buffer),
            self.topic.decode(buffer),
            self.is_internal.decode(buffer),
            self.partitions.decode(buffer),
            self.topic_authorized_operations.decode(buffer)
        )
    }
}

impl FromByte for PartitionMetadata {
    type R = PartitionMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.error.decode(buffer),
            self.id.decode(buffer),
            self.leader.decode(buffer),
            self.leader_epoch.decode(buffer),
            self.replicas.decode(buffer),
            self.isr.decode(buffer),
            self.offline_replicas.decode(buffer)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{MetadataRequest, MetadataResponse};

    #[test]
    fn test_metadata_request_uses_v8_header() {
        let topics = vec!["topic-a".to_string()];
        let req = MetadataRequest::new(77, "client-a", &topics);

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // api_key=3, api_version=8
        assert_eq!(&buf[0..4], &[0, 3, 0, 8]);
    }

    #[test]
    fn test_metadata_request_empty_topics_encodes_null_array() {
        let topics: Vec<String> = vec![];
        let req = MetadataRequest::new(77, "client-a", &topics);

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // Header (i16 key + i16 version + i32 correlation + string client_id)
        let header_len = 2 + 2 + 4 + 2 + "client-a".len();
        // topics array length should be null (-1)
        assert_eq!(&buf[header_len..header_len + 4], &[0xff, 0xff, 0xff, 0xff]);
    }

    #[test]
    fn test_decode_metadata_response_v8_shape() {
        let mut buf = Vec::new();
        (77i32).encode(&mut buf).unwrap(); // correlation
        (10i32).encode(&mut buf).unwrap(); // throttle_time_ms

        // brokers
        (1i32).encode(&mut buf).unwrap();
        (1i32).encode(&mut buf).unwrap(); // node_id
        "broker".encode(&mut buf).unwrap();
        (9092i32).encode(&mut buf).unwrap();
        "rack-a".encode(&mut buf).unwrap();

        "cluster-a".encode(&mut buf).unwrap(); // cluster_id
        (1i32).encode(&mut buf).unwrap(); // controller_id

        // topics
        (1i32).encode(&mut buf).unwrap();
        (0i16).encode(&mut buf).unwrap(); // error_code
        "topic-a".encode(&mut buf).unwrap();
        (0i8).encode(&mut buf).unwrap(); // is_internal
        (1i32).encode(&mut buf).unwrap(); // partitions
        (0i16).encode(&mut buf).unwrap(); // p.error_code
        (0i32).encode(&mut buf).unwrap(); // partition_index
        (1i32).encode(&mut buf).unwrap(); // leader_id
        (5i32).encode(&mut buf).unwrap(); // leader_epoch
        (1i32).encode(&mut buf).unwrap(); // replica_nodes len
        (1i32).encode(&mut buf).unwrap(); // replica node id
        (1i32).encode(&mut buf).unwrap(); // isr_nodes len
        (1i32).encode(&mut buf).unwrap(); // isr node id
        (0i32).encode(&mut buf).unwrap(); // offline_replicas len
        (0i32).encode(&mut buf).unwrap(); // topic_authorized_operations

        (0i32).encode(&mut buf).unwrap(); // cluster_authorized_operations

        let resp = MetadataResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, 77);
        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.cluster_id, "cluster-a");
        assert_eq!(resp.brokers[0].rack, "rack-a");
        assert_eq!(resp.topics[0].is_internal, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
    }
}
