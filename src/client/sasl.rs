use std::fmt;

/// SASL authentication configuration for Kafka connections.
///
/// Notes:
/// - SASL provides **authentication**, not encryption.
/// - For `PLAIN`, use TLS in production to protect credentials in transit.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SaslConfig {
    Plain(SaslPlainConfig),
}

impl SaslConfig {
    #[must_use]
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        SaslConfig::Plain(SaslPlainConfig {
            username: username.into(),
            password: password.into(),
        })
    }
}

/// Configuration for SASL/PLAIN.
#[derive(Clone, PartialEq, Eq)]
pub struct SaslPlainConfig {
    username: String,
    password: String,
}

impl fmt::Debug for SaslPlainConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslPlainConfig")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl SaslPlainConfig {
    pub(crate) fn initial_response(&self) -> Vec<u8> {
        let mut token = Vec::with_capacity(1 + self.username.len() + 1 + self.password.len());
        token.push(0);
        token.extend_from_slice(self.username.as_bytes());
        token.push(0);
        token.extend_from_slice(self.password.as_bytes());
        token
    }
}

#[cfg(test)]
mod tests {
    use super::SaslConfig;

    #[test]
    fn test_sasl_plain_initial_response_format() {
        let SaslConfig::Plain(cfg) = SaslConfig::plain("user", "pass");

        let token = cfg.initial_response();
        assert_eq!(token, b"\0user\0pass");
    }

    #[test]
    fn test_sasl_plain_initial_response_utf8_preserved() {
        let username = "us\u{00E9}r";
        let password = "p\u{00E4}ss";
        let SaslConfig::Plain(cfg) = SaslConfig::plain(username, password);

        let token = cfg.initial_response();
        assert!(token.starts_with(&[0]));
        assert!(token.contains(&0));
        assert!(token.windows(2).any(|w| w == [0xC3, 0xA9])); // é
        assert!(token.windows(2).any(|w| w == [0xC3, 0xA4])); // ä
    }

    #[test]
    fn test_sasl_plain_debug_redacts_password() {
        let cfg = SaslConfig::plain("user", "super-secret");
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("super-secret"));
        assert!(dbg.contains("user"));
        assert!(dbg.contains("<redacted>"));
    }
}
