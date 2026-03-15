use std::io::{Read, Write};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::error::{Error, Result};

pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut e = GzEncoder::new(Vec::new(), Compression::best());

    e.write_all(src)?;
    let compressed_bytes = e.finish()?;
    Ok(compressed_bytes)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>> {
    uncompress_limited(src, usize::MAX)
}

pub(crate) fn uncompress_limited<T: Read>(src: T, max_output_size: usize) -> Result<Vec<u8>> {
    let mut d = GzDecoder::new(src);

    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8 * 1024];
    loop {
        match d.read(&mut chunk) {
            Ok(0) => return Ok(buffer),
            Ok(n) => {
                let new_len =
                    buffer
                        .len()
                        .checked_add(n)
                        .ok_or(Error::DecompressionLimitExceeded {
                            limit: max_output_size,
                        })?;
                if new_len > max_output_size {
                    return Err(Error::DecompressionLimitExceeded {
                        limit: max_output_size,
                    });
                }
                buffer.extend_from_slice(&chunk[..n]);
            }
            Err(err) => return Err(From::from(err)),
        }
    }
}

#[test]
fn test_uncompress() {
    use std::io::Cursor;
    // The vector should uncompress to "test"
    let msg: Vec<u8> = vec![
        31, 139, 8, 0, 192, 248, 79, 85, 2, 255, 43, 73, 45, 46, 1, 0, 12, 126, 127, 216, 4, 0, 0,
        0,
    ];
    let uncomp_msg = String::from_utf8(uncompress(Cursor::new(msg)).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "test");
}

#[test]
fn test_uncompress_invalid_data() {
    use std::io::Cursor;
    let msg: Vec<u8> = vec![
        12, 42, 84, 104, 105, 115, 32, 105, 115, 32, 116, 101, 115, 116,
    ];
    assert!(uncompress(Cursor::new(msg)).is_err());
}

#[test]
fn test_uncompress_limited_rejects_oversized_output() {
    use std::io::Cursor;

    let payload = vec![b'a'; 8 * 1024];
    let compressed = compress(&payload).unwrap();
    assert!(matches!(
        uncompress_limited(Cursor::new(compressed), 1024),
        Err(Error::DecompressionLimitExceeded { limit: 1024 })
    ));
}
