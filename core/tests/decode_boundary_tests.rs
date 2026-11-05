//! Decoding must not depend on where the input is split. These tests build COPY
//! streams by hand, split them at every possible point, and expect the same rows.

use arrow::compute::concat_batches;
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use bytes::{BufMut, BytesMut};
use pgpq::error::ErrorKind;
use pgpq::pg_schema::{Column, PostgresSchema, PostgresType};
use pgpq::{PostgresBinaryToArrowDecoder, PostgresBinaryToArrowReader};
use std::io::{BufReader, Cursor};

const HEADER_MAGIC_BYTES: &[u8] = b"PGCOPY\n\xff\r\n\0";

fn schema() -> PostgresSchema {
    schema_with_payload_type(PostgresType::Text)
}

fn schema_with_payload_type(payload_type: PostgresType) -> PostgresSchema {
    PostgresSchema {
        columns: vec![
            (
                "id".to_string(),
                Column {
                    data_type: PostgresType::Int4,
                    nullable: false,
                },
            ),
            (
                "payload".to_string(),
                Column {
                    data_type: payload_type,
                    nullable: false,
                },
            ),
        ],
    }
}

/// Rows of (id, payload). Row 2's payload is larger than the small read buffers
/// used below, so a single row spans several reads.
fn rows() -> Vec<(i32, String)> {
    vec![
        (1, "a".repeat(10)),
        (2, "b".repeat(200)),
        (3, "c".repeat(10)),
        (4, "d".repeat(3)),
        (5, "e".repeat(40)),
    ]
}

/// Encodes rows as a binary COPY stream: header, one tuple per row, trailer.
fn copy_stream(rows: &[(i32, String)]) -> Vec<u8> {
    copy_stream_with_payload_prefix(rows, &[])
}

/// Encodes rows with prefix written before each payload, as jsonb writes its
/// version byte.
fn copy_stream_with_payload_prefix(rows: &[(i32, String)], prefix: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put(header().as_slice());
    for (id, payload) in rows {
        buf.put(row(*id, prefix, payload).as_slice());
    }
    buf.put_i16(-1); // trailer
    buf.to_vec()
}

fn header() -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put(HEADER_MAGIC_BYTES);
    buf.put_i32(0); // flags
    buf.put_i32(0); // header extension length
    buf.to_vec()
}

fn row(id: i32, prefix: &[u8], payload: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_i16(2); // field count
    buf.put_i32(4);
    buf.put_i32(id);
    buf.put_i32((prefix.len() + payload.len()) as i32);
    buf.put(prefix);
    buf.put(payload.as_bytes());
    buf.to_vec()
}

fn decode_with_read_size(stream: &[u8], read_size: usize) -> Result<RecordBatch, String> {
    let reader = BufReader::with_capacity(read_size, Cursor::new(stream.to_vec()));
    let mut decoder = PostgresBinaryToArrowReader::new(schema(), reader, read_size)
        .map_err(|e| format!("{e:?}"))?;
    decoder.read_header().map_err(|e| format!("{e:?}"))?;
    let batches = decoder.decode_batches().map_err(|e| format!("{e:?}"))?;
    let batch_schema = batches
        .first()
        .ok_or_else(|| "no batches".to_string())?
        .schema();
    concat_batches(&batch_schema, &batches).map_err(|e| format!("{e:?}"))
}

fn as_rows(batch: &RecordBatch) -> Vec<(i32, String)> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let payloads = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| (ids.value(i), payloads.value(i).to_string()))
        .collect()
}

#[test]
fn test_decode_is_independent_of_read_size() {
    let expected = rows();
    let stream = copy_stream(&expected);

    let mut failures = vec![];
    for read_size in 1..=stream.len() {
        match decode_with_read_size(&stream, read_size) {
            Ok(batch) => {
                let actual = as_rows(&batch);
                if actual != expected {
                    let ids: Vec<i32> = actual.iter().map(|(id, _)| *id).collect();
                    let lens: Vec<usize> = actual.iter().map(|(_, p)| p.len()).collect();
                    failures.push(format!(
                        "read_size={read_size}: wrong rows, ids={ids:?} payload lengths={lens:?}"
                    ));
                }
            }
            Err(e) => failures.push(format!("read_size={read_size}: error {e}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} read sizes failed:\n{}",
        failures.len(),
        stream.len(),
        failures.join("\n")
    );
}

fn new_decoder(batch_size: usize) -> PostgresBinaryToArrowDecoder {
    PostgresBinaryToArrowDecoder::new(schema(), batch_size).unwrap()
}

#[test]
fn test_decoder_one_byte_at_a_time() {
    let expected = rows();
    let stream = copy_stream(&expected);
    let mut decoder = new_decoder(2);

    let mut batches = vec![];
    for byte in &stream {
        batches.extend(decoder.write(&[*byte]).unwrap());
    }
    decoder.finish().unwrap();

    assert_eq!(
        batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    let all = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(as_rows(&all), expected);
}

#[test]
fn test_decoder_emits_a_full_batch_before_the_stream_ends() {
    let mut decoder = new_decoder(2);
    assert!(decoder.write(&header()).unwrap().is_empty());
    assert!(decoder.write(&row(1, &[], "a")).unwrap().is_empty());

    let batches = decoder.write(&row(2, &[], "b")).unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(
        as_rows(&batches[0]),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
}

#[test]
fn test_decoder_batch_size_equal_to_row_count_gives_one_batch() {
    let expected = rows();
    let mut decoder = new_decoder(expected.len());

    let batches = decoder.write(&copy_stream(&expected)).unwrap();
    decoder.finish().unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(as_rows(&batches[0]), expected);
}

#[test]
fn test_decoder_stream_with_no_rows_gives_no_batches() {
    let mut decoder = new_decoder(2);

    let batches = decoder.write(&copy_stream(&[])).unwrap();

    decoder.finish().unwrap();
    assert!(batches.is_empty());
}

#[test]
fn test_decoder_zero_batch_size_is_rejected() {
    let result = PostgresBinaryToArrowDecoder::new(schema(), 0);

    assert!(matches!(result, Err(ErrorKind::InvalidBatchSize)));
}

#[test]
fn test_finish_fails_on_a_stream_cut_off_inside_a_row() {
    let stream = copy_stream(&rows());
    let mut decoder = new_decoder(100);
    // Stop five bytes into row 2's payload.
    let cut = header().len() + row(1, &[], &rows()[0].1).len() + 4 + 4 + 4 + 5;
    decoder.write(&stream[..cut]).unwrap();

    let result = decoder.finish();

    assert!(matches!(result, Err(ErrorKind::IncompleteDecode { .. })));
}

#[test]
fn test_finish_fails_on_a_stream_cut_off_between_rows() {
    let mut decoder = new_decoder(100);
    decoder.write(&header()).unwrap();
    decoder.write(&row(1, &[], "a")).unwrap();

    let result = decoder.finish();

    assert!(matches!(result, Err(ErrorKind::MissingTrailer)));
}

#[test]
fn test_data_after_the_trailer_is_rejected() {
    let mut stream = copy_stream(&rows());
    stream.push(0);
    let mut decoder = new_decoder(100);

    let result = decoder.write(&stream);

    assert!(matches!(result, Err(ErrorKind::DataAfterTrailer)));
}

#[test]
fn test_a_write_after_the_trailer_is_rejected() {
    let mut decoder = new_decoder(100);
    decoder.write(&copy_stream(&rows())).unwrap();

    let result = decoder.write(&[0]);

    assert!(matches!(result, Err(ErrorKind::DataAfterTrailer)));
}

#[test]
fn test_a_row_with_the_wrong_field_count_is_rejected() {
    let mut decoder = new_decoder(100);
    let mut bad_row = BytesMut::new();
    bad_row.put_i16(3);

    decoder.write(&header()).unwrap();
    let result = decoder.write(&bad_row);

    assert!(matches!(
        result,
        Err(ErrorKind::UnexpectedFieldCount {
            expected: 2,
            actual: 3
        })
    ));
}

#[test]
fn test_a_field_that_does_not_fit_its_column_type_is_rejected() {
    let mut decoder = new_decoder(100);
    let mut bad_row = BytesMut::new();
    bad_row.put_i16(2);
    bad_row.put_i32(2); // an int4 field must be 4 bytes
    bad_row.put_i16(7);
    bad_row.put_i32(0);

    decoder.write(&header()).unwrap();
    let result = decoder.write(&bad_row);

    assert!(matches!(result, Err(ErrorKind::MalformedRow { .. })));
}

#[test]
fn test_a_failed_decoder_rejects_further_calls() {
    let mut decoder = new_decoder(100);
    let _ = decoder.write(b"not a copy stream!!");

    assert!(matches!(
        decoder.write(&header()),
        Err(ErrorKind::DecoderFailed)
    ));
    assert!(matches!(decoder.finish(), Err(ErrorKind::DecoderFailed)));
}

#[test]
fn test_reader_yields_batches_as_an_iterator() {
    let expected = rows();
    let stream = copy_stream(&expected);
    let reader = BufReader::with_capacity(7, Cursor::new(stream));
    let decoder = PostgresBinaryToArrowReader::new(schema(), reader, 2).unwrap();

    let batches = decoder.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(
        batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    let all = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(as_rows(&all), expected);
}

#[test]
fn test_reader_reports_a_truncated_stream() {
    let stream = copy_stream(&rows());
    let truncated = stream[..stream.len() - 2].to_vec();
    let reader = BufReader::new(Cursor::new(truncated));
    let decoder = PostgresBinaryToArrowReader::new(schema(), reader, 100).unwrap();

    let results = decoder.collect::<Vec<_>>();

    assert!(matches!(
        results.last(),
        Some(Err(ErrorKind::MissingTrailer))
    ));
}

#[test]
fn test_jsonb_skips_its_version_byte_at_every_split() {
    let expected = vec![
        (1, r#"{"a": 1}"#.to_string()),
        (2, r#"{"b": [1, 2, 3]}"#.to_string()),
        (3, "{}".to_string()),
    ];
    let jsonb_version = [1u8];
    let stream = copy_stream_with_payload_prefix(&expected, &jsonb_version);

    for read_size in 1..=stream.len() {
        let mut decoder =
            PostgresBinaryToArrowDecoder::new(schema_with_payload_type(PostgresType::Jsonb), 100)
                .unwrap();
        let mut batches = vec![];
        for piece in stream.chunks(read_size) {
            batches.extend(decoder.write(piece).unwrap());
        }
        decoder.finish().unwrap();

        let batch = &batches[0];
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let docs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let actual: Vec<(i32, String)> = (0..batch.num_rows())
            .map(|i| (ids.value(i), docs.value(i).to_string()))
            .collect();
        assert_eq!(actual, expected, "read_size={read_size}");
    }
}

#[test]
fn test_a_batch_after_an_all_null_batch_keeps_its_values() {
    let mut schema = schema();
    schema.columns[1].1.nullable = true;
    let mut decoder = PostgresBinaryToArrowDecoder::new(schema, 1).unwrap();
    let mut null_row = BytesMut::new();
    null_row.put_i16(2);
    null_row.put_i32(4);
    null_row.put_i32(1);
    null_row.put_i32(-1); // null payload

    let mut stream = header();
    stream.extend(null_row.to_vec());
    stream.extend(row(2, &[], "b"));
    stream.extend([0xff, 0xff]); // trailer
    let batches = decoder.write(&stream).unwrap();

    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].column(1).data_type(), &DataType::Null);
    assert_eq!(batches[0].column(1).len(), 1);
    assert_eq!(as_rows(&batches[1]), vec![(2, "b".to_string())]);
}
