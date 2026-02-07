use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, Result};

use super::{API_KEY_SASL_AUTHENTICATE, HeaderRequest, HeaderResponse};

const SASL_AUTHENTICATE_REQUEST_VERSION: i16 = 1;

#[derive(Debug)]
pub struct SaslAuthenticateRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub auth_bytes: &'a [u8],
}

impl<'a> SaslAuthenticateRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, auth_bytes: &'a [u8]) -> Self {
        SaslAuthenticateRequest {
            header: HeaderRequest::new(
                API_KEY_SASL_AUTHENTICATE,
                SASL_AUTHENTICATE_REQUEST_VERSION,
                correlation_id,
                client_id,
            ),
            auth_bytes,
        }
    }
}

impl ToByte for SaslAuthenticateRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.header.encode(buffer), self.auth_bytes.encode(buffer))
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct SaslAuthenticateResponse {
    pub header: HeaderResponse,
    pub error_code: i16,
    pub error_message: String,
    pub auth_bytes: Vec<u8>,
    pub session_lifetime_ms: i64,
}

impl SaslAuthenticateResponse {
    pub fn check_error(&self) -> Result<()> {
        match Error::from_protocol(self.error_code) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl FromByte for SaslAuthenticateResponse {
    type R = SaslAuthenticateResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.error_code.decode(buffer),
            self.error_message.decode(buffer),
            self.auth_bytes.decode(buffer),
            self.session_lifetime_ms.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{SaslAuthenticateRequest, SaslAuthenticateResponse};

    #[test]
    fn test_sasl_authenticate_request_v1_encoding() {
        let correlation_id: i32 = 123;
        let client_id = "test-client";
        let auth_bytes = b"token";

        let req = SaslAuthenticateRequest::new(correlation_id, client_id, auth_bytes);

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        let mut expected = Vec::new();
        (36i16).encode(&mut expected).unwrap(); // api_key
        (1i16).encode(&mut expected).unwrap(); // api_version
        correlation_id.encode(&mut expected).unwrap();
        client_id.encode(&mut expected).unwrap();
        auth_bytes.as_slice().encode(&mut expected).unwrap();

        assert_eq!(buf, expected);
    }

    #[test]
    fn test_sasl_authenticate_response_v1_decoding_null_error_message() {
        let correlation_id: i32 = 456;
        let error_code: i16 = 0;

        let mut buf = Vec::new();
        correlation_id.encode(&mut buf).unwrap();
        error_code.encode(&mut buf).unwrap();
        (-1i16).encode(&mut buf).unwrap(); // error_message: null
        (0i32).encode(&mut buf).unwrap(); // auth_bytes: empty
        (0i64).encode(&mut buf).unwrap(); // session_lifetime_ms

        let resp = SaslAuthenticateResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, correlation_id);
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.error_message, "");
        assert!(resp.auth_bytes.is_empty());
        assert_eq!(resp.session_lifetime_ms, 0);
    }

    #[test]
    fn test_sasl_authenticate_response_v1_decoding_error_message_present() {
        let correlation_id: i32 = 789;
        let error_code: i16 = 34;
        let error_message = "Illegal state";

        let mut buf = Vec::new();
        correlation_id.encode(&mut buf).unwrap();
        error_code.encode(&mut buf).unwrap();
        error_message.encode(&mut buf).unwrap();
        (0i32).encode(&mut buf).unwrap(); // auth_bytes: empty
        (3600000i64).encode(&mut buf).unwrap(); // session_lifetime_ms

        let resp = SaslAuthenticateResponse::decode_new(&mut Cursor::new(buf)).unwrap();
        assert_eq!(resp.header.correlation, correlation_id);
        assert_eq!(resp.error_code, error_code);
        assert_eq!(resp.error_message, "Illegal state");
        assert!(resp.auth_bytes.is_empty());
        assert_eq!(resp.session_lifetime_ms, 3_600_000);
    }
}
