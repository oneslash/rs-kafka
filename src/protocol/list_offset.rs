use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{KafkaCode, Result};
use crate::utils::TimestampedPartitionOffset;

use super::{API_KEY_OFFSET, HeaderRequest, HeaderResponse};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ListOffsetVersion {
    // Highest non-flexible version supported by this crate.
    V5 = 5,
}

/// https://kafka.apache.org/protocol.html#The_Messages_ListOffsets
#[derive(Debug)]
pub struct ListOffsetsRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub replica: i32,
    pub isolation_level: i8,
    pub topics: Vec<TopicListOffsetsRequest<'a>>,
}

#[derive(Debug)]
pub struct TopicListOffsetsRequest<'a> {
    pub topic: &'a str,
    pub api_version: i16,
    pub partitions: Vec<PartitionListOffsetsRequest>,
}

#[derive(Default, Debug)]
pub struct PartitionListOffsetsRequest {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub time: i64,
}

impl<'a> ListOffsetsRequest<'a> {
    pub fn new(
        correlation_id: i32,
        version: ListOffsetVersion,
        client_id: &'a str,
    ) -> ListOffsetsRequest<'a> {
        ListOffsetsRequest {
            header: HeaderRequest::new(API_KEY_OFFSET, version as i16, correlation_id, client_id),
            replica: -1,
            isolation_level: 0,
            topics: vec![],
        }
    }

    pub fn add(&mut self, topic: &'a str, partition: i32, time: i64) {
        for tp in &mut self.topics {
            if tp.topic == topic {
                tp.add(partition, time);
                return;
            }
        }
        let mut tp = TopicListOffsetsRequest::new(topic, self.header.api_version);
        tp.add(partition, time);
        self.topics.push(tp);
    }
}

impl<'a> TopicListOffsetsRequest<'a> {
    fn new(topic: &'a str, api_version: i16) -> TopicListOffsetsRequest<'a> {
        TopicListOffsetsRequest {
            topic,
            api_version,
            partitions: vec![],
        }
    }
    fn add(&mut self, partition: i32, time: i64) {
        self.partitions
            .push(PartitionListOffsetsRequest::new(partition, time));
    }
}

impl PartitionListOffsetsRequest {
    fn new(partition: i32, time: i64) -> PartitionListOffsetsRequest {
        PartitionListOffsetsRequest {
            partition,
            current_leader_epoch: -1,
            time,
        }
    }
}

impl ToByte for ListOffsetsRequest<'_> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.replica.encode(buffer),
            if self.header.api_version >= 2 {
                self.isolation_level.encode(buffer)
            } else {
                Ok(())
            },
            self.topics.encode(buffer)
        )
    }
}

impl ToByte for TopicListOffsetsRequest<'_> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        self.topic.encode(buffer)?;
        (self.partitions.len() as i32).encode(buffer)?;
        for p in &self.partitions {
            p.encode_with_version(self.api_version, buffer)?;
        }
        Ok(())
    }
}

impl ToByte for PartitionListOffsetsRequest {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        self.encode_with_version(1, buffer)
    }
}

impl PartitionListOffsetsRequest {
    fn encode_with_version<T: Write>(&self, api_version: i16, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.encode(buffer),
            if api_version >= 4 {
                self.current_leader_epoch.encode(buffer)
            } else {
                Ok(())
            },
            self.time.encode(buffer)
        )
    }
}

// -------------------------------------

#[derive(Default, Debug)]
pub struct ListOffsetsResponse {
    pub header: HeaderResponse,
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicListOffsetsResponse>,
}

#[derive(Default, Debug)]
pub struct TopicListOffsetsResponse {
    pub topic: String,
    pub partitions: Vec<TimestampedPartitionOffsetListOffsetsResponse>,
}

#[derive(Default, Debug)]
pub struct TimestampedPartitionOffsetListOffsetsResponse {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
}

impl TimestampedPartitionOffsetListOffsetsResponse {
    pub fn to_offset(&self) -> std::result::Result<TimestampedPartitionOffset, KafkaCode> {
        match KafkaCode::from_protocol(self.error_code) {
            Some(code) => Err(code),
            None => Ok(TimestampedPartitionOffset {
                partition: self.partition,
                offset: self.offset,
                time: self.timestamp,
            }),
        }
    }
}

impl FromByte for ListOffsetsResponse {
    type R = ListOffsetsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.throttle_time_ms.decode(buffer),
            self.topics.decode(buffer)
        )
    }
}

impl FromByte for TopicListOffsetsResponse {
    type R = TopicListOffsetsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

impl FromByte for TimestampedPartitionOffsetListOffsetsResponse {
    type R = TimestampedPartitionOffsetListOffsetsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error_code.decode(buffer),
            self.timestamp.decode(buffer),
            self.offset.decode(buffer),
            self.leader_epoch.decode(buffer)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{ListOffsetVersion, ListOffsetsRequest, ListOffsetsResponse};

    #[test]
    fn test_list_offsets_request_v5_encoding_includes_isolation_and_leader_epoch() {
        let mut req = ListOffsetsRequest::new(11, ListOffsetVersion::V5, "client-a");
        req.add("topic-a", 2, -1);

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // api_key=2, api_version=5
        assert_eq!(&buf[0..4], &[0, 2, 0, 5]);
        // The isolation level is present in v2+ requests (defaults to 0)
        assert!(buf.windows(1).any(|w| w == [0]));
    }

    #[test]
    fn test_list_offsets_response_v5_decoding() {
        let mut buf = Vec::new();
        (11i32).encode(&mut buf).unwrap(); // correlation
        (25i32).encode(&mut buf).unwrap(); // throttle_time_ms
        (1i32).encode(&mut buf).unwrap(); // topics len
        "topic-a".encode(&mut buf).unwrap();
        (1i32).encode(&mut buf).unwrap(); // partitions len
        (2i32).encode(&mut buf).unwrap(); // partition
        (0i16).encode(&mut buf).unwrap(); // error_code
        (123i64).encode(&mut buf).unwrap(); // timestamp
        (456i64).encode(&mut buf).unwrap(); // offset
        (9i32).encode(&mut buf).unwrap(); // leader_epoch

        let resp = ListOffsetsResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, 11);
        assert_eq!(resp.throttle_time_ms, 25);
        let p = &resp.topics[0].partitions[0];
        assert_eq!(p.partition, 2);
        assert_eq!(p.timestamp, 123);
        assert_eq!(p.offset, 456);
        assert_eq!(p.leader_epoch, 9);
    }
}
