use std::io::{Cursor, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ReadBytesExt};
use crc::Crc;

use crate::codecs::ToByte;
use crate::compression::Compression;
#[cfg(feature = "gzip")]
use crate::compression::gzip;
#[cfg(feature = "snappy")]
use crate::compression::snappy;
#[cfg(feature = "snappy")]
use crate::compression::snappy::SnappyReader;
use crate::error::{Error, KafkaCode, Result};

const RECORD_BATCH_MAGIC: i8 = 2;

#[derive(Debug)]
pub struct RecordMessage<'a> {
    pub offset: i64,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[inline]
fn crc32c(data: &[u8]) -> u32 {
    Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(data)
}

fn now_millis() -> Result<i64> {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidDuration)?;
    let ms = dur
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(dur.subsec_millis()));
    Ok(ms.min(i64::MAX as u64) as i64)
}

#[inline]
fn zigzag_encode_i32(v: i32) -> u32 {
    ((v << 1) ^ (v >> 31)) as u32
}

#[inline]
fn zigzag_decode_i32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

#[inline]
fn zigzag_encode_i64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
fn zigzag_decode_i64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

fn write_uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn write_varint_i32(v: i32, out: &mut Vec<u8>) {
    write_uvarint(u64::from(zigzag_encode_i32(v)), out);
}

fn write_varlong_i64(v: i64, out: &mut Vec<u8>) {
    write_uvarint(zigzag_encode_i64(v), out);
}

fn read_uvarint<R: Read>(r: &mut R) -> Result<u64> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        let b = r.read_u8().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;
        x |= u64::from(b & 0x7f) << shift;
        if (b & 0x80) == 0 {
            return Ok(x);
        }
        shift += 7;
    }
    Err(Error::CodecError)
}

fn read_varint_i32<R: Read>(r: &mut R) -> Result<i32> {
    let u = read_uvarint(r)?;
    if u > u64::from(u32::MAX) {
        return Err(Error::CodecError);
    }
    Ok(zigzag_decode_i32(u as u32))
}

fn read_varlong_i64<R: Read>(r: &mut R) -> Result<i64> {
    let u = read_uvarint(r)?;
    Ok(zigzag_decode_i64(u))
}

fn encode_record(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    offset_delta: i32,
    timestamp_delta: i64,
    out: &mut Vec<u8>,
) {
    let mut body = Vec::new();

    // attributes
    body.push(0);
    write_varlong_i64(timestamp_delta, &mut body);
    write_varint_i32(offset_delta, &mut body);

    match key {
        None => write_varint_i32(-1, &mut body),
        Some(k) => {
            write_varint_i32(k.len() as i32, &mut body);
            body.extend_from_slice(k);
        }
    }
    match value {
        None => write_varint_i32(-1, &mut body),
        Some(v) => {
            write_varint_i32(v.len() as i32, &mut body);
            body.extend_from_slice(v);
        }
    }

    // headers (we emit none)
    write_varint_i32(0, &mut body);

    write_varint_i32(body.len() as i32, out);
    out.extend_from_slice(&body);
}

type RecordKeyValue<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

/// Encodes a RecordBatch (magic=2) containing one record per provided key/value
/// pair.
///
/// The returned bytes are the raw RecordBatch bytes (i.e., without the
/// surrounding Kafka `BYTES` length prefix used in requests).
pub fn encode_record_batch(
    messages: &[RecordKeyValue<'_>],
    compression: Compression,
) -> Result<Vec<u8>> {
    let ts = now_millis()?;
    let mut records = Vec::new();
    for (idx, (k, v)) in messages.iter().enumerate() {
        encode_record(*k, *v, idx as i32, 0, &mut records);
    }

    let (attributes, records) = match compression {
        Compression::NONE => (0i16, records),
        #[cfg(feature = "gzip")]
        Compression::GZIP => (Compression::GZIP as i16, gzip::compress(&records)?),
        #[cfg(feature = "snappy")]
        Compression::SNAPPY => (
            Compression::SNAPPY as i16,
            snappy::compress_xerial(&records)?,
        ),
    };

    let mut batch = Vec::new();

    // BaseOffset
    (0i64).encode(&mut batch)?;

    // BatchLength (placeholder)
    let batch_len_pos = batch.len();
    (0i32).encode(&mut batch)?;

    // PartitionLeaderEpoch
    (-1i32).encode(&mut batch)?;
    // Magic
    RECORD_BATCH_MAGIC.encode(&mut batch)?;

    // CRC (placeholder)
    let crc_pos = batch.len();
    (0i32).encode(&mut batch)?;

    // Attributes (compression + CreateTime)
    attributes.encode(&mut batch)?;

    // LastOffsetDelta
    let last_offset_delta = i32::try_from(messages.len().saturating_sub(1)).unwrap_or(0);
    last_offset_delta.encode(&mut batch)?;

    // BaseTimestamp / MaxTimestamp
    ts.encode(&mut batch)?;
    ts.encode(&mut batch)?;

    // ProducerId / ProducerEpoch / BaseSequence (non-idempotent)
    (-1i64).encode(&mut batch)?;
    (-1i16).encode(&mut batch)?;
    (-1i32).encode(&mut batch)?;

    // RecordsCount
    let records_count = i32::try_from(messages.len()).map_err(|_| Error::CodecError)?;
    records_count.encode(&mut batch)?;

    // Records
    batch.extend_from_slice(&records);

    // Fill BatchLength: bytes following the BatchLength field
    let batch_length =
        i32::try_from(batch.len().saturating_sub(12)).map_err(|_| Error::CodecError)?;
    batch_length.encode(&mut &mut batch[batch_len_pos..batch_len_pos + 4])?;

    // Fill CRC32C over bytes from Attributes to end
    let crc_start = crc_pos + 4;
    let crc = crc32c(&batch[crc_start..]);
    (crc as i32).encode(&mut &mut batch[crc_pos..crc_pos + 4])?;

    Ok(batch)
}

pub(crate) fn record_set_has_compressed_batches(record_set: &[u8]) -> Result<bool> {
    let mut r = Cursor::new(record_set);

    while (r.position() as usize) < record_set.len() {
        let _base_offset = r.read_i64::<BigEndian>().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;
        let batch_length = r.read_i32::<BigEndian>().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;

        if batch_length < 0 {
            return Err(Error::CodecError);
        }

        let batch_start = r.position() as usize;
        let batch_end = batch_start
            .checked_add(batch_length as usize)
            .ok_or(Error::CodecError)?;
        if batch_end > record_set.len() {
            return Err(Error::UnexpectedEOF);
        }
        let batch_bytes = &record_set[batch_start..batch_end];
        r.set_position(batch_end as u64);

        let mut br = Cursor::new(batch_bytes);
        let _ = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let magic = br.read_i8().map_err(|_| Error::UnexpectedEOF)?;
        if magic != RECORD_BATCH_MAGIC {
            return Err(Error::UnsupportedProtocol);
        }
        let _ = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let attributes = br
            .read_i16::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        if (attributes & 0x07) != 0 {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(crate) fn decompress_record_set(record_set: &[u8], validate_crc: bool) -> Result<Vec<u8>> {
    let mut r = Cursor::new(record_set);
    let mut out = Vec::with_capacity(record_set.len());

    while (r.position() as usize) < record_set.len() {
        let base_offset = r.read_i64::<BigEndian>().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;
        let batch_length = r.read_i32::<BigEndian>().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;

        if batch_length < 0 {
            return Err(Error::CodecError);
        }

        let batch_start = r.position() as usize;
        let batch_end = batch_start
            .checked_add(batch_length as usize)
            .ok_or(Error::CodecError)?;
        if batch_end > record_set.len() {
            return Err(Error::UnexpectedEOF);
        }
        let batch_bytes = &record_set[batch_start..batch_end];
        r.set_position(batch_end as u64);

        let mut br = Cursor::new(batch_bytes);
        let _partition_leader_epoch = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let magic = br.read_i8().map_err(|_| Error::UnexpectedEOF)?;
        if magic != RECORD_BATCH_MAGIC {
            return Err(Error::UnsupportedProtocol);
        }

        let crc_wire = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let attrs_pos = br.position() as usize;
        let attributes = br
            .read_i16::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let compression = attributes & 0x07;

        let _ = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // last_offset_delta
        let _ = br
            .read_i64::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // base_timestamp
        let _ = br
            .read_i64::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // max_timestamp
        let _ = br
            .read_i64::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // producer_id
        let _ = br
            .read_i16::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // producer_epoch
        let _ = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // base_sequence
        let _ = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?; // records_count

        let records_start = br.position() as usize;
        let records_bytes = &batch_bytes[records_start..];

        if validate_crc {
            let crc_calc = crc32c(&batch_bytes[attrs_pos..]);
            if crc_calc as i32 != crc_wire {
                return Err(Error::Kafka(KafkaCode::CorruptMessage));
            }
        }

        if compression == 0 {
            base_offset.encode(&mut out)?;
            batch_length.encode(&mut out)?;
            out.extend_from_slice(batch_bytes);
            continue;
        }

        let records = match compression {
            #[cfg(feature = "gzip")]
            1 => gzip::uncompress(Cursor::new(records_bytes))?,
            #[cfg(feature = "snappy")]
            2 => {
                let mut v = Vec::new();
                SnappyReader::new(records_bytes)?.read_to_end(&mut v)?;
                v
            }
            _ => return Err(Error::UnsupportedCompression),
        };

        let mut new_batch = Vec::with_capacity(records_start + records.len());
        new_batch.extend_from_slice(&batch_bytes[..records_start]);
        let new_attributes = attributes & !0x07;
        new_batch[attrs_pos..attrs_pos + 2].copy_from_slice(&new_attributes.to_be_bytes());
        new_batch.extend_from_slice(&records);

        let crc_calc = crc32c(&new_batch[attrs_pos..]) as i32;
        new_batch[5..9].copy_from_slice(&crc_calc.to_be_bytes());

        base_offset.encode(&mut out)?;
        let new_batch_length = i32::try_from(new_batch.len()).map_err(|_| Error::CodecError)?;
        new_batch_length.encode(&mut out)?;
        out.extend_from_slice(&new_batch);
    }

    Ok(out)
}

/// Decodes an uncompressed record set (Kafka `RECORDS`) into individual
/// messages. Only RecordBatch magic=2 is supported.
pub fn decode_uncompressed_record_set(
    record_set: &[u8],
    req_offset: i64,
    validate_crc: bool,
) -> Result<Vec<RecordMessage<'_>>> {
    let mut r = Cursor::new(record_set);
    let mut out = Vec::new();

    while (r.position() as usize) < record_set.len() {
        let base_offset = r.read_i64::<BigEndian>().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;
        let batch_length = r.read_i32::<BigEndian>().map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::UnexpectedEOF
            } else {
                Error::Io(e)
            }
        })?;

        if batch_length < 0 {
            return Err(Error::CodecError);
        }

        let batch_start = r.position() as usize;
        let batch_end = batch_start
            .checked_add(batch_length as usize)
            .ok_or(Error::CodecError)?;
        if batch_end > record_set.len() {
            return Err(Error::UnexpectedEOF);
        }
        let batch_bytes = &record_set[batch_start..batch_end];
        r.set_position(batch_end as u64);

        let mut br = Cursor::new(batch_bytes);
        let _partition_leader_epoch = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let magic = br.read_i8().map_err(|_| Error::UnexpectedEOF)?;
        if magic != RECORD_BATCH_MAGIC {
            return Err(Error::UnsupportedProtocol);
        }

        let crc_wire = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let attrs_pos = br.position() as usize;
        let attributes = br
            .read_i16::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let compression = attributes & 0x07;
        if compression != 0 {
            return Err(Error::UnsupportedCompression);
        }

        let _last_offset_delta = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let _base_timestamp = br
            .read_i64::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let _max_timestamp = br
            .read_i64::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let _producer_id = br
            .read_i64::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let _producer_epoch = br
            .read_i16::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let _base_sequence = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        let records_count = br
            .read_i32::<BigEndian>()
            .map_err(|_| Error::UnexpectedEOF)?;
        if records_count < 0 {
            return Err(Error::CodecError);
        }

        if validate_crc {
            let crc_calc = crc32c(&batch_bytes[attrs_pos..]);
            if crc_calc as i32 != crc_wire {
                return Err(Error::Kafka(KafkaCode::CorruptMessage));
            }
        }

        let records_start = br.position() as usize;
        let records_bytes = &batch_bytes[records_start..];
        let mut rr = Cursor::new(records_bytes);

        for _ in 0..records_count {
            let len = read_varint_i32(&mut rr)?;
            if len < 0 {
                return Err(Error::CodecError);
            }
            let rec_start = rr.position() as usize;
            let rec_end = rec_start
                .checked_add(len as usize)
                .ok_or(Error::CodecError)?;
            if rec_end > records_bytes.len() {
                return Err(Error::UnexpectedEOF);
            }
            let rec = &records_bytes[rec_start..rec_end];
            rr.set_position(rec_end as u64);

            let mut rec_r = Cursor::new(rec);
            let _record_attributes = rec_r.read_u8().map_err(|_| Error::UnexpectedEOF)?;
            let _timestamp_delta = read_varlong_i64(&mut rec_r)?;
            let offset_delta = read_varint_i32(&mut rec_r)?;

            let key_len = read_varint_i32(&mut rec_r)?;
            let key = if key_len < 0 {
                &[][..]
            } else {
                let key_len = key_len as usize;
                let key_start = rec_r.position() as usize;
                let key_end = key_start.checked_add(key_len).ok_or(Error::CodecError)?;
                if key_end > rec.len() {
                    return Err(Error::UnexpectedEOF);
                }
                rec_r.set_position(key_end as u64);
                &rec[key_start..key_end]
            };

            let value_len = read_varint_i32(&mut rec_r)?;
            let value = if value_len < 0 {
                &[][..]
            } else {
                let value_len = value_len as usize;
                let value_start = rec_r.position() as usize;
                let value_end = value_start
                    .checked_add(value_len)
                    .ok_or(Error::CodecError)?;
                if value_end > rec.len() {
                    return Err(Error::UnexpectedEOF);
                }
                rec_r.set_position(value_end as u64);
                &rec[value_start..value_end]
            };

            let headers_count = read_varint_i32(&mut rec_r)?;
            if headers_count != 0 {
                // We don't support headers yet; skip by decoding them.
                for _ in 0..headers_count {
                    let header_key_len = read_varint_i32(&mut rec_r)?;
                    if header_key_len < 0 {
                        return Err(Error::CodecError);
                    }
                    let key_start = rec_r.position() as usize;
                    let key_end = key_start
                        .checked_add(header_key_len as usize)
                        .ok_or(Error::CodecError)?;
                    if key_end > rec.len() {
                        return Err(Error::UnexpectedEOF);
                    }
                    rec_r.set_position(key_end as u64);

                    let header_val_len = read_varint_i32(&mut rec_r)?;
                    if header_val_len >= 0 {
                        let val_start = rec_r.position() as usize;
                        let val_end = val_start
                            .checked_add(header_val_len as usize)
                            .ok_or(Error::CodecError)?;
                        if val_end > rec.len() {
                            return Err(Error::UnexpectedEOF);
                        }
                        rec_r.set_position(val_end as u64);
                    }
                }
            }

            let abs_offset = base_offset + i64::from(offset_delta);
            if abs_offset >= req_offset {
                out.push(RecordMessage {
                    offset: abs_offset,
                    key,
                    value,
                });
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_uncompressed_record_set, decompress_record_set, encode_record_batch,
        record_set_has_compressed_batches,
    };
    use crate::compression::Compression;

    #[test]
    fn test_record_batch_roundtrip_single() {
        let batch =
            encode_record_batch(&[(None, Some(b"hello".as_slice()))], Compression::NONE).unwrap();
        let msgs = decode_uncompressed_record_set(&batch, 0, true).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].offset, 0);
        assert_eq!(msgs[0].value, b"hello");
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_record_batch_roundtrip_gzip() {
        let batch =
            encode_record_batch(&[(None, Some(b"hello".as_slice()))], Compression::GZIP).unwrap();
        assert!(record_set_has_compressed_batches(&batch).unwrap());
        let decompressed = decompress_record_set(&batch, true).unwrap();
        assert!(!record_set_has_compressed_batches(&decompressed).unwrap());
        let msgs = decode_uncompressed_record_set(&decompressed, 0, true).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"hello");
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn test_record_batch_roundtrip_snappy() {
        let batch =
            encode_record_batch(&[(None, Some(b"hello".as_slice()))], Compression::SNAPPY).unwrap();
        assert!(record_set_has_compressed_batches(&batch).unwrap());
        let decompressed = decompress_record_set(&batch, true).unwrap();
        assert!(!record_set_has_compressed_batches(&decompressed).unwrap());
        let msgs = decode_uncompressed_record_set(&decompressed, 0, true).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"hello");
    }
}
