use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_schema::{Field, Schema};
use pgpq::error::ErrorKind;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use pgpq::{pg_schema::PostgresSchema, PostgresBinaryToArrowDecoder};

const READ_CHUNK_SIZE: usize = 1024 * 1024 * 8;

fn read_schema_file(path: PathBuf, timezone: String) -> PostgresSchema {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    PostgresSchema::from_reader(reader, ',', timezone).unwrap()
}

fn read_arrow_file(path: PathBuf) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

fn run_test_case(case: &str, timezone: String) -> Result<(), ErrorKind> {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/snapshots/{case}.bin"));
    let schema_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/decoding/{case}.schema"));
    let arrow_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/testdata/{case}.arrow"));

    let file = File::open(path).unwrap();
    let reader = BufReader::with_capacity(READ_CHUNK_SIZE, file);
    let schema = read_schema_file(schema_path, timezone);

    let mut decoder = PostgresBinaryToArrowDecoder::new(schema, reader, READ_CHUNK_SIZE).unwrap();
    decoder.read_header()?;
    let batches = decoder.decode_batches()?;

    let mut expected_batches = read_arrow_file(arrow_path);

    // all testdata currently has nullable set where it should not.
    // This is a workaround to make the test pass.
    if !case.contains("nullable") {
        expected_batches = expected_batches
            .into_iter()
            .map(|batch| {
                let new_fields: Vec<Arc<Field>> = (*(*batch.schema()).clone().fields)
                    .to_vec()
                    .clone()
                    .into_iter()
                    .map(|f| Arc::new((*f).clone().with_nullable(false)))
                    .collect();
                let new_schema = Schema::new(new_fields);
                println!("{:?}", new_schema);
                RecordBatch::try_new(Arc::new(new_schema), batch.columns().to_vec()).unwrap()
            })
            .collect::<Vec<_>>();
    }

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), expected_batches.iter().map(|b| b.num_rows()).sum());
    let batch_schemas = batches.iter().map(|b| b.schema()).collect::<Vec<_>>();
    let expected_batch_schemas = expected_batches.iter().map(|b| b.schema()).collect::<Vec<_>>();
    assert_eq!(batch_schemas, expected_batch_schemas);
    assert_eq!(batches, expected_batches);

    Ok(())
}

#[test]
fn test_bool() -> Result<(), ErrorKind> {
    run_test_case("bool", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_int16() -> Result<(), ErrorKind> {
    run_test_case("int16", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_int32() -> Result<(), ErrorKind> {
    run_test_case("int32", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_int64() -> Result<(), ErrorKind> {
    run_test_case("int64", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_float32() -> Result<(), ErrorKind> {
    run_test_case("float32", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_float64() -> Result<(), ErrorKind> {
    run_test_case("float64", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_numeric() -> Result<(), ErrorKind> {
    run_test_case("numeric", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_timestamp_us_notz() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_notz", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_timestamp_us_tz() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_tz", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_time_us() -> Result<(), ErrorKind> {
    run_test_case("time_us", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_date32() -> Result<(), ErrorKind> {
    run_test_case("date32", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_duration_us() -> Result<(), ErrorKind> {
    run_test_case("duration_us", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_binary() -> Result<(), ErrorKind> {
    run_test_case("binary", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_string() -> Result<(), ErrorKind> {
    run_test_case("string", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_bool_nullable() -> Result<(), ErrorKind> {
    run_test_case("bool_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_int16_nullable() -> Result<(), ErrorKind> {
    run_test_case("int16_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_int32_nullable() -> Result<(), ErrorKind> {
    run_test_case("int32_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_int64_nullable() -> Result<(), ErrorKind> {
    run_test_case("int64_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_float32_nullable() -> Result<(), ErrorKind> {
    run_test_case("float32_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_float64_nullable() -> Result<(), ErrorKind> {
    run_test_case("float64_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_timestamp_us_notz_nullable() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_notz_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_timestamp_us_tz_nullable() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_tz_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_time_us_nullable() -> Result<(), ErrorKind> {
    run_test_case("time_us_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_date32_nullable() -> Result<(), ErrorKind> {
    run_test_case("date32_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_duration_us_nullable() -> Result<(), ErrorKind> {
    run_test_case("duration_us_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_binary_nullable() -> Result<(), ErrorKind> {
    run_test_case("binary_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_string_nullable() -> Result<(), ErrorKind> {
    run_test_case("string_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_numeric_nullable() -> Result<(), ErrorKind> {
    run_test_case("numeric_nullable", "America/New_York".to_string())?;
    Ok(())
}
