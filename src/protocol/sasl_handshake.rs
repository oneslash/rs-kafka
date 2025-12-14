use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, Result};

use super::{API_KEY_SASL_HANDSHAKE, HeaderRequest, HeaderResponse};

// v1 indicates that the SASL tokens will be wrapped in `SaslAuthenticate` requests (KIP-86).
const SASL_HANDSHAKE_REQUEST_VERSION: i16 = 1;

#[derive(Debug)]
pub struct SaslHandshakeRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub mechanism: &'a str,
}

impl<'a> SaslHandshakeRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, mechanism: &'a str) -> Self {
        SaslHandshakeRequest {
            header: HeaderRequest::new(
                API_KEY_SASL_HANDSHAKE,
                SASL_HANDSHAKE_REQUEST_VERSION,
                correlation_id,
                client_id,
            ),
            mechanism,
        }
    }
}

impl ToByte for SaslHandshakeRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.header.encode(buffer), self.mechanism.encode(buffer))
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct SaslHandshakeResponse {
    pub header: HeaderResponse,
    pub error_code: i16,
    pub enabled_mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    pub fn check_error(&self) -> Result<()> {
        match Error::from_protocol(self.error_code) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl FromByte for SaslHandshakeResponse {
    type R = SaslHandshakeResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.error_code.decode(buffer),
            self.enabled_mechanisms.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{SaslHandshakeRequest, SaslHandshakeResponse};

    #[test]
    fn test_sasl_handshake_request_v1_encoding() {
        let correlation_id: i32 = 123;
        let client_id = "test-client";
        let mechanism = "PLAIN";

        let req = SaslHandshakeRequest::new(correlation_id, client_id, mechanism);

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        let mut expected = Vec::new();
        (17i16).encode(&mut expected).unwrap(); // api_key
        (1i16).encode(&mut expected).unwrap(); // api_version
        correlation_id.encode(&mut expected).unwrap();
        client_id.encode(&mut expected).unwrap();
        mechanism.encode(&mut expected).unwrap();

        assert_eq!(buf, expected);
    }

    #[test]
    fn test_sasl_handshake_response_v0_decoding() {
        let correlation_id: i32 = 456;
        let error_code: i16 = 0;
        let mechanisms = ["PLAIN", "SCRAM-SHA-256"];

        let mut buf = Vec::new();
        correlation_id.encode(&mut buf).unwrap();
        error_code.encode(&mut buf).unwrap();
        (mechanisms.len() as i32).encode(&mut buf).unwrap();
        for m in mechanisms {
            m.encode(&mut buf).unwrap();
        }

        let resp = SaslHandshakeResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, correlation_id);
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.enabled_mechanisms, ["PLAIN", "SCRAM-SHA-256"]);
    }
}
