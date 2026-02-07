use std::io::{Read, Write};

use crate::codecs::{self, FromByte, ToByte};
use crate::error::{self, Error, KafkaCode, Result};
use crate::utils::PartitionOffset;

use super::{API_KEY_GROUP_COORDINATOR, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_FETCH};
use super::{HeaderRequest, HeaderResponse};

const FIND_COORDINATOR_REQUEST_VERSION: i16 = 2;

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct GroupCoordinatorRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub key: &'b str,
    pub key_type: i8,
}

impl<'a, 'b> GroupCoordinatorRequest<'a, 'b> {
    pub fn new(
        group: &'b str,
        correlation_id: i32,
        client_id: &'a str,
    ) -> GroupCoordinatorRequest<'a, 'b> {
        GroupCoordinatorRequest {
            header: HeaderRequest::new(
                API_KEY_GROUP_COORDINATOR,
                FIND_COORDINATOR_REQUEST_VERSION,
                correlation_id,
                client_id,
            ),
            key: group,
            key_type: 0,
        }
    }
}

impl ToByte for GroupCoordinatorRequest<'_, '_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.key.encode(buffer),
            self.key_type.encode(buffer)
        )
    }
}

#[derive(Debug, Default)]
pub struct GroupCoordinatorResponse {
    pub header: HeaderResponse,
    pub throttle_time_ms: i32,
    pub error: i16,
    pub error_message: String,
    pub broker_id: i32,
    pub port: i32,
    pub host: String,
}

impl GroupCoordinatorResponse {
    pub fn into_result(self) -> Result<Self> {
        match Error::from_protocol(self.error) {
            Some(e) => Err(e),
            None => Ok(self),
        }
    }
}

impl FromByte for GroupCoordinatorResponse {
    type R = GroupCoordinatorResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.throttle_time_ms.decode(buffer),
            self.error.decode(buffer),
            self.error_message.decode(buffer),
            self.broker_id.decode(buffer),
            self.host.decode(buffer),
            self.port.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OffsetFetchVersion {
    /// causes the retrieval of the offsets from zookeeper
    V0 = 0,
    /// supported on modern Kafka brokers.
    V5 = 5,
}

#[derive(Debug)]
pub struct OffsetFetchRequest<'a, 'b, 'c> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchRequest<'c>>,
}

#[derive(Debug)]
pub struct TopicPartitionOffsetFetchRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionOffsetFetchRequest>,
}

#[derive(Debug)]
pub struct PartitionOffsetFetchRequest {
    pub partition: i32,
}

impl<'a, 'b, 'c> OffsetFetchRequest<'a, 'b, 'c> {
    pub fn new(
        group: &'b str,
        version: OffsetFetchVersion,
        correlation_id: i32,
        client_id: &'a str,
    ) -> OffsetFetchRequest<'a, 'b, 'c> {
        OffsetFetchRequest {
            header: HeaderRequest::new(
                API_KEY_OFFSET_FETCH,
                version as i16,
                correlation_id,
                client_id,
            ),
            group,
            topic_partitions: vec![],
        }
    }

    pub fn add(&mut self, topic: &'c str, partition: i32) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetFetchRequest::new(topic);
        tp.add(partition);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionOffsetFetchRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionOffsetFetchRequest<'a> {
        TopicPartitionOffsetFetchRequest {
            topic,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: i32) {
        self.partitions
            .push(PartitionOffsetFetchRequest::new(partition));
    }
}

impl PartitionOffsetFetchRequest {
    pub fn new(partition: i32) -> PartitionOffsetFetchRequest {
        PartitionOffsetFetchRequest { partition }
    }
}

impl ToByte for OffsetFetchRequest<'_, '_, '_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.group.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionOffsetFetchRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.topic.encode(buffer), self.partitions.encode(buffer))
    }
}

impl ToByte for PartitionOffsetFetchRequest {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.partition.encode(buffer)
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct OffsetFetchResponse {
    pub header: HeaderResponse,
    pub throttle_time_ms: i32,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchResponse>,
    pub error_code: i16,
}

impl OffsetFetchResponse {
    pub fn group_error(&self) -> Option<Error> {
        Error::from_protocol(self.error_code)
    }
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: String,
    pub error: i16,
}

impl PartitionOffsetFetchResponse {
    pub fn get_offsets(&self) -> Result<PartitionOffset> {
        match Error::from_protocol(self.error) {
            Some(Error::Kafka(KafkaCode::UnknownTopicOrPartition)) => {
                // ~ occurs only on protocol v0 when no offset available
                // for the group in question; we'll align the behavior
                // with protocol v1.
                Ok(PartitionOffset {
                    partition: self.partition,
                    offset: -1,
                })
            }
            Some(e) => Err(e),
            None => Ok(PartitionOffset {
                partition: self.partition,
                offset: self.offset,
            }),
        }
    }
}

impl FromByte for OffsetFetchResponse {
    type R = OffsetFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.throttle_time_ms.decode(buffer),
            self.topic_partitions.decode(buffer),
            self.error_code.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionOffsetFetchResponse {
    type R = TopicPartitionOffsetFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

impl FromByte for PartitionOffsetFetchResponse {
    type R = PartitionOffsetFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.offset.decode(buffer),
            self.committed_leader_epoch.decode(buffer),
            self.metadata.decode(buffer),
            self.error.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OffsetCommitVersion {
    /// causes offset to be stored in zookeeper
    V0 = 0,
    /// supported as of kafka 0.8.2, causes offsets to be stored
    /// directly in kafka
    V1 = 1,
    /// supported as of kafka 0.9.0, causes offsets to be stored
    /// directly in kafka
    V2 = 2,
    V3 = 3,
    V4 = 4,
    V5 = 5,
    V6 = 6,
    V7 = 7,
}

impl OffsetCommitVersion {
    fn from_protocol(n: i16) -> Self {
        match n {
            0 => Self::V0,
            1 => Self::V1,
            2 => Self::V2,
            3 => Self::V3,
            4 => Self::V4,
            5 => Self::V5,
            6 => Self::V6,
            7 => Self::V7,
            _ => panic!("Unknown offset commit version code: {n}"),
        }
    }
}

#[derive(Debug)]
pub struct OffsetCommitRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
    pub generation_id_or_member_epoch: i32,
    pub member_id: &'a str,
    pub group_instance_id: Option<&'a str>,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitRequest<'b>>,
}

#[derive(Debug)]
pub struct TopicPartitionOffsetCommitRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionOffsetCommitRequest<'a>>,
}

#[derive(Debug)]
pub struct PartitionOffsetCommitRequest<'a> {
    pub partition: i32,
    pub offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: &'a str,
}

impl<'a, 'b> OffsetCommitRequest<'a, 'b> {
    pub fn new(
        group: &'b str,
        version: OffsetCommitVersion,
        correlation_id: i32,
        client_id: &'a str,
    ) -> OffsetCommitRequest<'a, 'b> {
        OffsetCommitRequest {
            header: HeaderRequest::new(
                API_KEY_OFFSET_COMMIT,
                version as i16,
                correlation_id,
                client_id,
            ),
            group,
            generation_id_or_member_epoch: -1,
            member_id: "",
            group_instance_id: None,
            topic_partitions: vec![],
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, offset: i64, metadata: &'b str) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, offset, metadata);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetCommitRequest::new(topic);
        tp.add(partition, offset, metadata);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionOffsetCommitRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionOffsetCommitRequest<'a> {
        TopicPartitionOffsetCommitRequest {
            topic,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64, metadata: &'a str) {
        self.partitions.push(PartitionOffsetCommitRequest::new(
            partition, offset, metadata,
        ));
    }
}

impl<'a> PartitionOffsetCommitRequest<'a> {
    pub fn new(partition: i32, offset: i64, metadata: &'a str) -> PartitionOffsetCommitRequest<'a> {
        PartitionOffsetCommitRequest {
            partition,
            offset,
            committed_leader_epoch: -1,
            metadata,
        }
    }
}

impl ToByte for OffsetCommitRequest<'_, '_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        let v = OffsetCommitVersion::from_protocol(self.header.api_version);
        self.header.encode(buffer)?;
        self.group.encode(buffer)?;
        match v {
            OffsetCommitVersion::V1 => {
                (-1i32).encode(buffer)?;
                "".encode(buffer)?;
            }
            OffsetCommitVersion::V2 => {
                (-1i32).encode(buffer)?;
                "".encode(buffer)?;
                (-1i64).encode(buffer)?;
            }
            OffsetCommitVersion::V3 | OffsetCommitVersion::V4 => {
                self.generation_id_or_member_epoch.encode(buffer)?;
                self.member_id.encode(buffer)?;
                (-1i64).encode(buffer)?;
            }
            OffsetCommitVersion::V5 => {
                self.generation_id_or_member_epoch.encode(buffer)?;
                self.member_id.encode(buffer)?;
            }
            OffsetCommitVersion::V6 => {
                self.generation_id_or_member_epoch.encode(buffer)?;
                self.member_id.encode(buffer)?;
            }
            OffsetCommitVersion::V7 => {
                self.generation_id_or_member_epoch.encode(buffer)?;
                self.member_id.encode(buffer)?;
                self.group_instance_id.encode(buffer)?;
            }
            OffsetCommitVersion::V0 => {
                // nothing to do
            }
        }
        codecs::encode_as_array(buffer, &self.topic_partitions, |buffer, tp| {
            try_multi!(
                tp.topic.encode(buffer),
                codecs::encode_as_array(buffer, &tp.partitions, |buffer, p| {
                    p.partition.encode(buffer)?;
                    p.offset.encode(buffer)?;
                    if v == OffsetCommitVersion::V1 {
                        (-1i64).encode(buffer)?;
                    }
                    if v == OffsetCommitVersion::V6 || v == OffsetCommitVersion::V7 {
                        p.committed_leader_epoch.encode(buffer)?;
                    }
                    p.metadata.encode(buffer)
                })
            )
        })
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub throttle_time_ms: i32,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitResponse>,
}

impl FromByte for OffsetCommitResponse {
    type R = OffsetCommitResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.throttle_time_ms.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetCommitResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitResponse>,
}

impl FromByte for TopicPartitionOffsetCommitResponse {
    type R = TopicPartitionOffsetCommitResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

#[derive(Default, Debug)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error: i16,
}

impl PartitionOffsetCommitResponse {
    pub fn to_error(&self) -> Option<error::KafkaCode> {
        error::KafkaCode::from_protocol(self.error)
    }
}

impl FromByte for PartitionOffsetCommitResponse {
    type R = PartitionOffsetCommitResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.partition.decode(buffer), self.error.decode(buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{
        GroupCoordinatorRequest, OffsetCommitRequest, OffsetCommitVersion, OffsetFetchResponse,
    };

    #[test]
    fn test_group_coordinator_request_v2_encoding() {
        let req = GroupCoordinatorRequest::new("group-a", 42, "client-a");
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // api_key=10, api_version=2
        assert_eq!(&buf[0..4], &[0, 10, 0, 2]);
    }

    #[test]
    fn test_offset_commit_request_v7_encoding() {
        let mut req = OffsetCommitRequest::new("group-a", OffsetCommitVersion::V7, 42, "client-a");
        req.add("topic-a", 1, 100, "m");

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // api_key=8, api_version=7
        assert_eq!(&buf[0..4], &[0, 8, 0, 7]);
        // Nullable group_instance_id is encoded as null by default (-1 i16)
        assert!(buf.windows(2).any(|w| w == [0xff, 0xff]));
    }

    #[test]
    fn test_offset_fetch_response_v5_decoding_and_group_error() {
        let mut buf = Vec::new();
        (42i32).encode(&mut buf).unwrap(); // correlation
        (10i32).encode(&mut buf).unwrap(); // throttle_time_ms
        (1i32).encode(&mut buf).unwrap(); // topics len
        "topic-a".encode(&mut buf).unwrap();
        (1i32).encode(&mut buf).unwrap(); // partitions len
        (1i32).encode(&mut buf).unwrap(); // partition
        (100i64).encode(&mut buf).unwrap(); // committed_offset
        (3i32).encode(&mut buf).unwrap(); // committed_leader_epoch
        "meta".encode(&mut buf).unwrap();
        (0i16).encode(&mut buf).unwrap(); // partition error
        (0i16).encode(&mut buf).unwrap(); // group error code

        let resp = OffsetFetchResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, 42);
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.group_error().is_none());
        let p = &resp.topic_partitions[0].partitions[0];
        assert_eq!(p.partition, 1);
        assert_eq!(p.offset, 100);
        assert_eq!(p.committed_leader_epoch, 3);
    }
}
