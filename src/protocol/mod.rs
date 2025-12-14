use std::io::{Read, Write};
use std::time::Duration;

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, KafkaCode, Result};
use crc::Crc;

/// Macro to return Result<()> from multiple statements
macro_rules! try_multi {
    ($($input_expr:expr),*) => ({
        $($input_expr?;)*
        Ok(())
    })
}

pub mod consumer;
pub mod metadata;
pub mod produce;
pub mod list_offset;

pub mod fetch;
pub mod api_versions;
pub mod records;
mod zreader;

// ~ convenient re-exports for request/response types defined in the
// submodules
pub use self::consumer::{
    GroupCoordinatorRequest, GroupCoordinatorResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetCommitVersion, OffsetFetchRequest, OffsetFetchResponse, OffsetFetchVersion,
};
pub use self::fetch::FetchRequest;
#[allow(unused_imports)]
pub use self::api_versions::{ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse, BrokerApiVersions};
pub use self::metadata::{MetadataRequest, MetadataResponse};
pub use self::produce::{ProduceRequest, ProduceResponse};
pub use self::list_offset::{ListOffsetsRequest, ListOffsetsResponse};

// --------------------------------------------------------------------

const API_KEY_PRODUCE: i16 = 0;
const API_KEY_FETCH: i16 = 1;
const API_KEY_OFFSET: i16 = 2;
const API_KEY_METADATA: i16 = 3;
// 4-7 reserved for non-public kafka api services
const API_KEY_OFFSET_COMMIT: i16 = 8;
const API_KEY_OFFSET_FETCH: i16 = 9;
const API_KEY_GROUP_COORDINATOR: i16 = 10;
const API_KEY_API_VERSIONS: i16 = 18;

// the default version of Kafka API we are requesting
const API_VERSION: i16 = 0;

// --------------------------------------------------------------------

/// Provides a way to parse the full raw response data into a
/// particular response structure.
pub trait ResponseParser {
    type T;
    fn parse(&self, response: Vec<u8>) -> Result<Self::T>;
}

// --------------------------------------------------------------------

impl KafkaCode {
    fn from_protocol(n: i16) -> Option<KafkaCode> {
        match n {
            0 => None,
            -1 => Some(KafkaCode::Unknown),
            1 => Some(KafkaCode::OffsetOutOfRange),
            2 => Some(KafkaCode::CorruptMessage),
            3 => Some(KafkaCode::UnknownTopicOrPartition),
            4 => Some(KafkaCode::InvalidMessageSize),
            5 => Some(KafkaCode::LeaderNotAvailable),
            6 => Some(KafkaCode::NotLeaderForPartition),
            7 => Some(KafkaCode::RequestTimedOut),
            8 => Some(KafkaCode::BrokerNotAvailable),
            9 => Some(KafkaCode::ReplicaNotAvailable),
            10 => Some(KafkaCode::MessageSizeTooLarge),
            11 => Some(KafkaCode::StaleControllerEpoch),
            12 => Some(KafkaCode::OffsetMetadataTooLarge),
            13 => Some(KafkaCode::NetworkException),
            14 => Some(KafkaCode::GroupLoadInProgress),
            15 => Some(KafkaCode::GroupCoordinatorNotAvailable),
            16 => Some(KafkaCode::NotCoordinatorForGroup),
            17 => Some(KafkaCode::InvalidTopic),
            18 => Some(KafkaCode::RecordListTooLarge),
            19 => Some(KafkaCode::NotEnoughReplicas),
            20 => Some(KafkaCode::NotEnoughReplicasAfterAppend),
            21 => Some(KafkaCode::InvalidRequiredAcks),
            22 => Some(KafkaCode::IllegalGeneration),
            23 => Some(KafkaCode::InconsistentGroupProtocol),
            24 => Some(KafkaCode::InvalidGroupId),
            25 => Some(KafkaCode::UnknownMemberId),
            26 => Some(KafkaCode::InvalidSessionTimeout),
            27 => Some(KafkaCode::RebalanceInProgress),
            28 => Some(KafkaCode::InvalidCommitOffsetSize),
            29 => Some(KafkaCode::TopicAuthorizationFailed),
            30 => Some(KafkaCode::GroupAuthorizationFailed),
            31 => Some(KafkaCode::ClusterAuthorizationFailed),
            32 => Some(KafkaCode::InvalidTimestamp),
            33 => Some(KafkaCode::UnsupportedSaslMechanism),
            34 => Some(KafkaCode::IllegalSaslState),
            35 => Some(KafkaCode::UnsupportedVersion),
            _ => Some(KafkaCode::Unknown),
        }
    }
}

#[test]
fn test_kafka_code_from_protocol() {
    macro_rules! assert_kafka_code {
        ($kcode:path, $n:expr) => {
            assert_eq!(KafkaCode::from_protocol($n), Some($kcode));
        };
    }

    assert!(KafkaCode::from_protocol(0).is_none());
    assert_kafka_code!(
        KafkaCode::OffsetOutOfRange,
        KafkaCode::OffsetOutOfRange as i16
    );
    assert_kafka_code!(
        KafkaCode::IllegalGeneration,
        KafkaCode::IllegalGeneration as i16
    );
    assert_kafka_code!(
        KafkaCode::UnsupportedVersion,
        KafkaCode::UnsupportedVersion as i16
    );
    assert_kafka_code!(KafkaCode::Unknown, KafkaCode::Unknown as i16);
    // ~ test some un mapped non-zero codes; should all map to "unknown"
    assert_kafka_code!(KafkaCode::Unknown, i16::MAX);
    assert_kafka_code!(KafkaCode::Unknown, i16::MIN);
    assert_kafka_code!(KafkaCode::Unknown, -100);
    assert_kafka_code!(KafkaCode::Unknown, 100);
}

// a (sub-) module private method for error
impl Error {
    fn from_protocol(n: i16) -> Option<Error> {
        KafkaCode::from_protocol(n).map(Error::Kafka)
    }
}

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct HeaderRequest<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}

impl<'a> HeaderRequest<'a> {
    fn new(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: &'a str,
    ) -> HeaderRequest<'a> {
        HeaderRequest {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
}

impl ToByte for HeaderRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.api_key.encode(buffer),
            self.api_version.encode(buffer),
            self.correlation_id.encode(buffer),
            self.client_id.encode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32,
}

impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer)
    }
}

// --------------------------------------------------------------------

pub fn to_crc(data: &[u8]) -> u32 {
    Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(data)
}

// --------------------------------------------------------------------

/// Safely converts a Duration into the number of milliseconds as a
/// i32 as often required in the kafka protocol.
pub fn to_millis_i32(d: Duration) -> Result<i32> {
    let m = d
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(d.subsec_millis()));
    let max_millis = u64::try_from(i32::MAX).expect("i32::MAX is positive");
    if m > max_millis {
        Err(Error::InvalidDuration)
    } else {
        Ok(i32::try_from(m).expect("checked to fit in i32"))
    }
}

#[test]
fn test_to_millis_i32() {
    fn assert_invalid(d: Duration) {
        match to_millis_i32(d) {
            Err(Error::InvalidDuration) => {}
            other => panic!("Expected Err(InvalidDuration) but got {other:?}"),
        }
    }
    fn assert_valid(d: Duration, expected_millis: i32) {
        let r = to_millis_i32(d);
        match r {
            Ok(m) => assert_eq!(expected_millis, m),
            Err(e) => panic!("Expected Ok({expected_millis}) but got Err({e:?})"),
        }
    }
    assert_valid(Duration::from_millis(1_234), 1_234);
    assert_valid(Duration::new(540, 123_456_789), 540_123);
    assert_invalid(Duration::from_millis(u64::MAX));
    assert_invalid(Duration::from_millis(u64::from(u32::MAX)));
    assert_invalid(Duration::from_millis(i32::MAX as u64 + 1));
    assert_valid(Duration::from_millis(i32::MAX as u64 - 1), i32::MAX - 1);
}
