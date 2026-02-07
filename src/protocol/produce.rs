use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::compression::Compression;

use crate::error::{KafkaCode, Result};

use super::API_KEY_PRODUCE;
use super::records::encode_record_batch;
use super::{HeaderRequest, HeaderResponse};
use crate::producer::{ProduceConfirm, ProducePartitionConfirm};

const PRODUCE_API_VERSION: i16 = 8;

impl ToByte for Option<&str> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        match *self {
            Some(s) => s.encode(buffer),
            None => (-1i16).encode(buffer),
        }
    }
}

#[derive(Debug)]
pub struct ProduceRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub transactional_id: Option<&'a str>,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic_partitions: Vec<TopicPartitionProduceRequest<'b>>,
    pub compression: Compression,
    pub timestamp: Option<ProducerTimestamp>,
}

#[derive(Debug)]
pub struct TopicPartitionProduceRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionProduceRequest<'a>>,
    pub compression: Compression,
    #[allow(unused)]
    pub timestamp: Option<ProducerTimestamp>,
}

#[derive(Debug)]
pub struct PartitionProduceRequest<'a> {
    pub partition: i32,
    pub messages: Vec<MessageProduceRequest<'a>>,
}

#[derive(Debug)]
pub struct MessageProduceRequest<'a> {
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
}

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum ProducerTimestamp {
    CreateTime = 0,
    LogAppendTime = 8, // attributes bit 3 should be set to 1 in case of the LogAppend param. See https://kafka.apache.org/39/documentation/#messageset
}

impl<'a, 'b> ProduceRequest<'a, 'b> {
    pub fn new(
        required_acks: i16,
        timeout: i32,
        correlation_id: i32,
        client_id: &'a str,
        compression: Compression,
        #[cfg(feature = "producer_timestamp")] timestamp: Option<ProducerTimestamp>,
    ) -> ProduceRequest<'a, 'b> {
        ProduceRequest {
            header: HeaderRequest::new(
                API_KEY_PRODUCE,
                PRODUCE_API_VERSION,
                correlation_id,
                client_id,
            ),
            transactional_id: None,
            required_acks,
            timeout,
            topic_partitions: vec![],
            compression,
            #[cfg(feature = "producer_timestamp")]
            timestamp,
            #[cfg(not(feature = "producer_timestamp"))]
            timestamp: None,
        }
    }

    pub fn add(
        &mut self,
        topic: &'b str,
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, key, value);
                return;
            }
        }
        let mut tp = TopicPartitionProduceRequest::new(topic, self.compression, self.timestamp);
        tp.add(partition, key, value);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionProduceRequest<'a> {
    pub fn new(
        topic: &'a str,
        compression: Compression,
        timestamp: Option<ProducerTimestamp>,
    ) -> TopicPartitionProduceRequest<'a> {
        TopicPartitionProduceRequest {
            topic,
            partitions: vec![],
            compression,
            timestamp,
        }
    }

    pub fn add(&mut self, partition: i32, key: Option<&'a [u8]>, value: Option<&'a [u8]>) {
        if let Some(pp) = self
            .partitions
            .iter_mut()
            .find(|pp| pp.partition == partition)
        {
            pp.add(key, value);
            return;
        }

        self.partitions
            .push(PartitionProduceRequest::new(partition, key, value));
    }
}

impl<'a> PartitionProduceRequest<'a> {
    pub fn new<'b>(
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) -> PartitionProduceRequest<'b> {
        let mut r = PartitionProduceRequest {
            partition,
            messages: Vec::new(),
        };
        r.add(key, value);
        r
    }

    pub fn add(&mut self, key: Option<&'a [u8]>, value: Option<&'a [u8]>) {
        self.messages.push(MessageProduceRequest::new(key, value));
    }
}

impl ToByte for ProduceRequest<'_, '_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.transactional_id.encode(buffer),
            self.required_acks.encode(buffer),
            self.timeout.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionProduceRequest<'_> {
    // render: TopicName [Partition MessageSetSize MessageSet]
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.topic.encode(buffer)?;
        (self.partitions.len() as i32).encode(buffer)?;
        for e in &self.partitions {
            #[cfg(not(feature = "producer_timestamp"))]
            e.encode_partition(buffer, self.compression)?;

            #[cfg(feature = "producer_timestamp")]
            {
                match self.timestamp {
                    Some(timestamp) => {
                        e.encode_partition_with_timestamp(buffer, self.compression, timestamp)?;
                    }
                    None => e.encode_partition(buffer, self.compression)?,
                }
            }
        }
        Ok(())
    }
}

impl PartitionProduceRequest<'_> {
    // render: Partition MessageSetSize MessageSet
    //
    // MessetSet => [Offset MessageSize Message]
    // MessageSets are not preceded by an int32 like other array elements in the protocol.
    fn encode_partition<W: Write>(&self, out: &mut W, compression: Compression) -> Result<()> {
        self.partition.encode(out)?;

        let mut msgs = Vec::with_capacity(self.messages.len());
        for msg in &self.messages {
            msgs.push((msg.key, msg.value));
        }
        let batch = encode_record_batch(&msgs, compression)?;
        batch.encode(out)
    }

    #[cfg(feature = "producer_timestamp")]
    fn encode_partition_with_timestamp<W: Write>(
        &self,
        out: &mut W,
        compression: Compression,
        _timestamp: ProducerTimestamp,
    ) -> Result<()> {
        self.encode_partition(out, compression)
    }
}

impl MessageProduceRequest<'_> {
    fn new<'b>(key: Option<&'b [u8]>, value: Option<&'b [u8]>) -> MessageProduceRequest<'b> {
        MessageProduceRequest { key, value }
    }
}

impl ToByte for Option<&[u8]> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        match *self {
            Some(xs) => xs.encode(buffer),
            None => (-1i32).encode(buffer),
        }
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
pub struct ProduceResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionProduceResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionProduceResponse {
    pub topic: String,
    pub partitions: Vec<PartitionProduceResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: i64,
    pub log_append_time: i64,
    pub log_start_offset: i64,
    pub record_errors: Vec<RecordError>,
    pub error_message: String,
}

#[derive(Default, Debug, Clone)]
pub struct RecordError {
    pub batch_index: i32,
    pub batch_index_error_message: String,
}

impl ProduceResponse {
    pub fn get_response(self) -> Vec<ProduceConfirm> {
        self.topic_partitions
            .into_iter()
            .map(TopicPartitionProduceResponse::get_response)
            .collect()
    }
}

impl TopicPartitionProduceResponse {
    pub fn get_response(self) -> ProduceConfirm {
        let Self { topic, partitions } = self;
        let partition_confirms = partitions
            .iter()
            .map(PartitionProduceResponse::get_response)
            .collect();

        ProduceConfirm {
            topic,
            partition_confirms,
        }
    }
}

impl PartitionProduceResponse {
    pub fn get_response(&self) -> ProducePartitionConfirm {
        ProducePartitionConfirm {
            partition: self.partition,
            offset: match KafkaCode::from_protocol(self.error) {
                None => Ok(self.offset),
                Some(code) => Err(code),
            },
        }
    }
}

impl FromByte for ProduceResponse {
    type R = ProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer),
            self.throttle_time_ms.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionProduceResponse {
    type R = TopicPartitionProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

impl FromByte for PartitionProduceResponse {
    type R = PartitionProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error.decode(buffer),
            self.offset.decode(buffer),
            self.log_append_time.decode(buffer),
            self.log_start_offset.decode(buffer),
            self.record_errors.decode(buffer),
            self.error_message.decode(buffer)
        )
    }
}

impl FromByte for RecordError {
    type R = RecordError;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.batch_index.decode(buffer),
            self.batch_index_error_message.decode(buffer)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};
    use crate::compression::Compression;

    use super::{ProduceRequest, ProduceResponse};

    #[test]
    fn test_produce_request_uses_v8_header() {
        let mut req = ProduceRequest::new(
            1,
            1_000,
            123,
            "client-a",
            Compression::NONE,
            #[cfg(feature = "producer_timestamp")]
            None,
        );
        req.add("topic-a", 0, None, Some(b"v"));

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // api_key=0, api_version=8
        assert_eq!(&buf[0..4], &[0, 0, 0, 8]);
    }

    #[test]
    fn test_decode_v8_produce_response_partition_fields() {
        let mut buf = Vec::new();

        // response header
        (7i32).encode(&mut buf).unwrap();
        // topics
        (1i32).encode(&mut buf).unwrap();
        "topic-a".encode(&mut buf).unwrap();
        // partitions
        (1i32).encode(&mut buf).unwrap();
        (0i32).encode(&mut buf).unwrap(); // partition
        (0i16).encode(&mut buf).unwrap(); // error_code
        (42i64).encode(&mut buf).unwrap(); // base_offset
        (-1i64).encode(&mut buf).unwrap(); // log_append_time_ms
        (7i64).encode(&mut buf).unwrap(); // log_start_offset
        (1i32).encode(&mut buf).unwrap(); // record_errors len
        (3i32).encode(&mut buf).unwrap(); // batch_index
        "bad record".encode(&mut buf).unwrap(); // batch_index_error_message
        "global error".encode(&mut buf).unwrap(); // error_message
        (0i32).encode(&mut buf).unwrap(); // throttle_time_ms

        let resp = ProduceResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, 7);
        assert_eq!(resp.topic_partitions.len(), 1);
        let p = &resp.topic_partitions[0].partitions[0];
        assert_eq!(p.partition, 0);
        assert_eq!(p.offset, 42);
        assert_eq!(p.log_start_offset, 7);
        assert_eq!(p.record_errors.len(), 1);
        assert_eq!(p.record_errors[0].batch_index, 3);
        assert_eq!(p.record_errors[0].batch_index_error_message, "bad record");
        assert_eq!(p.error_message, "global error");
    }
}
