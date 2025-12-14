use std::collections::HashMap;
use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, KafkaCode, Result};

use super::{API_KEY_API_VERSIONS, HeaderRequest, HeaderResponse};

const API_VERSIONS_REQUEST_VERSION: i16 = 2;

#[derive(Debug)]
pub struct ApiVersionsRequest<'a> {
    pub header: HeaderRequest<'a>,
}

impl<'a> ApiVersionsRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str) -> Self {
        ApiVersionsRequest {
            header: HeaderRequest::new(
                API_KEY_API_VERSIONS,
                API_VERSIONS_REQUEST_VERSION,
                correlation_id,
                client_id,
            ),
        }
    }
}

impl ToByte for ApiVersionsRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.header.encode(buffer)
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionRange {
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn contains(&self, version: i16) -> bool {
        version >= self.min_version && version <= self.max_version
    }
}

#[derive(Debug, Default, Clone)]
pub struct BrokerApiVersions {
    ranges: HashMap<i16, ApiVersionRange>,
}

impl BrokerApiVersions {
    #[must_use]
    pub fn range(&self, api_key: i16) -> Option<ApiVersionRange> {
        self.ranges.get(&api_key).copied()
    }

    pub fn select_highest_common_version(
        &self,
        api_key: i16,
        implemented_versions: &[i16],
    ) -> Result<i16> {
        let range = self
            .range(api_key)
            .ok_or(Error::Kafka(KafkaCode::UnsupportedVersion))?;

        implemented_versions
            .iter()
            .copied()
            .filter(|v| range.contains(*v))
            .max()
            .ok_or(Error::Kafka(KafkaCode::UnsupportedVersion))
    }

    fn insert(&mut self, api_key: i16, min_version: i16, max_version: i16) {
        self.ranges.insert(
            api_key,
            ApiVersionRange {
                min_version,
                max_version,
            },
        );
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct ApiVersionsResponse {
    pub header: HeaderResponse,
    pub error_code: i16,
    pub api_keys: Vec<ApiKeyVersionRange>,
    pub throttle_time_ms: i32,
}

#[derive(Default, Debug)]
pub struct ApiKeyVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionsResponse {
    pub fn into_broker_api_versions(self) -> Result<BrokerApiVersions> {
        if let Some(err) = Error::from_protocol(self.error_code) {
            return Err(err);
        }

        let mut versions = BrokerApiVersions::default();
        for api in self.api_keys {
            versions.insert(api.api_key, api.min_version, api.max_version);
        }
        Ok(versions)
    }
}

impl FromByte for ApiVersionsResponse {
    type R = ApiVersionsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.error_code.decode(buffer),
            self.api_keys.decode(buffer),
            self.throttle_time_ms.decode(buffer)
        )
    }
}

impl FromByte for ApiKeyVersionRange {
    type R = ApiKeyVersionRange;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.api_key.decode(buffer),
            self.min_version.decode(buffer),
            self.max_version.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{ApiVersionsResponse, BrokerApiVersions};

    #[test]
    fn test_api_versions_response_v2_parsing() {
        let correlation_id: i32 = 123;
        let error_code: i16 = 0;

        let mut buf = Vec::new();
        correlation_id.encode(&mut buf).unwrap();
        error_code.encode(&mut buf).unwrap();

        // api_keys array
        (2i32).encode(&mut buf).unwrap();
        // (api_key=3, min=0, max=12)
        (3i16).encode(&mut buf).unwrap();
        (0i16).encode(&mut buf).unwrap();
        (12i16).encode(&mut buf).unwrap();
        // (api_key=18, min=0, max=3)
        (18i16).encode(&mut buf).unwrap();
        (0i16).encode(&mut buf).unwrap();
        (3i16).encode(&mut buf).unwrap();

        // throttle_time_ms
        (50i32).encode(&mut buf).unwrap();

        let resp = ApiVersionsResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, correlation_id);
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.api_keys.len(), 2);
        assert_eq!(resp.throttle_time_ms, 50);

        let versions = resp.into_broker_api_versions().unwrap();
        assert_eq!(
            versions.range(3),
            Some(super::ApiVersionRange {
                min_version: 0,
                max_version: 12
            })
        );
    }

    #[test]
    fn test_select_highest_common_version() {
        let mut versions = BrokerApiVersions::default();
        versions.insert(3, 1, 5);

        assert_eq!(
            versions
                .select_highest_common_version(3, &[0, 1, 2, 3, 4])
                .unwrap(),
            4
        );
        assert!(versions.select_highest_common_version(3, &[0]).is_err());
        assert!(
            versions
                .select_highest_common_version(999, &[0, 1])
                .is_err()
        );
    }
}
