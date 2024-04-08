use std::fs::File;
use std::path::PathBuf;
use std::io::BufReader;
use pgpq::error::ErrorKind;

use pgpq::{PostgresBinaryToArrowDecoder, pg_schema::PostgresSchema};


const KB: usize = 1024* 512;

fn read_schema_file(path: PathBuf) -> PostgresSchema {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    PostgresSchema::from_reader(reader, ',').unwrap()
}

fn run_test_case(case: &str) -> Result<(), ErrorKind> {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/snapshots/{case}.bin"));
    let schema_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/decoding/{case}.schema"));
    let file = File::open(path).unwrap();
    let reader = BufReader::with_capacity(KB, file);
    let schema = read_schema_file(schema_path);
    println!("{:?}", schema);
    let mut decoder = PostgresBinaryToArrowDecoder::new(schema, reader, KB).unwrap();
    decoder.read_header()?;
    let batches =  decoder.decode_batches()?;
    println!("{:?}", batches);
    Ok(())
}


#[test]
fn test_bool() -> Result<(), ErrorKind> {
    run_test_case("bool")?;
	Ok(())
}

#[test]
fn test_int16() -> Result<(), ErrorKind> {
    run_test_case("int16")?;
	Ok(())
}

#[test]
fn test_int32() -> Result<(), ErrorKind> {
    run_test_case("int32")?;
	Ok(())
}

#[test]
fn test_int64() -> Result<(), ErrorKind> {
    run_test_case("int64")?;
	Ok(())
}

#[test]
fn test_float32() -> Result<(), ErrorKind> {
    run_test_case("float32")?;
	Ok(())
}

#[test]
fn test_float64() -> Result<(), ErrorKind> {
    run_test_case("float64")?;
	Ok(())
}

#[test]
fn test_timestamp_us_notz() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_notz")?;
	Ok(())
}

#[test]
fn test_timestamp_us_tz() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_tz")?;
	Ok(())
}

#[test]
fn test_time_us() -> Result<(), ErrorKind> {
    run_test_case("time_us")?;
	Ok(())
}

#[test]
fn test_date32() -> Result<(), ErrorKind> {
    run_test_case("date32")?;
	Ok(())
}

#[test]
fn test_duration_us() -> Result<(), ErrorKind> {
    run_test_case("duration_us")?;
	Ok(())
}


#[test]
fn test_binary() -> Result<(), ErrorKind> {
    run_test_case("binary")?;
	Ok(())
}

#[test]
fn test_string() -> Result<(), ErrorKind> {
    run_test_case("string")?;
	Ok(())
}

#[test]
fn test_bool_nullable() -> Result<(), ErrorKind> {
    run_test_case("bool_nullable")?;
	Ok(())
}

#[test]
fn test_int16_nullable() -> Result<(), ErrorKind> {
    run_test_case("int16_nullable")?;
	Ok(())
}

#[test]
fn test_int32_nullable() -> Result<(), ErrorKind> {
    run_test_case("int32_nullable")?;
	Ok(())
}

#[test]
fn test_int64_nullable() -> Result<(), ErrorKind> {
    run_test_case("int64_nullable")?;
	Ok(())
}

#[test]
fn test_float32_nullable() -> Result<(), ErrorKind> {
    run_test_case("float32_nullable")?;
	Ok(())
}

#[test]
fn test_float64_nullable() -> Result<(), ErrorKind> {
    run_test_case("float64_nullable")?;
	Ok(())
}

#[test]
fn test_timestamp_us_notz_nullable() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_notz_nullable")?;
	Ok(())
}

#[test]
fn test_timestamp_us_tz_nullable() -> Result<(), ErrorKind> {
    run_test_case("timestamp_us_tz_nullable")?;
	Ok(())
}

#[test]
fn test_time_us_nullable() -> Result<(), ErrorKind> {
    run_test_case("time_us_nullable")?;
	Ok(())
}

#[test]
fn test_date32_nullable() -> Result<(), ErrorKind> {
    run_test_case("date32_nullable")?;
	Ok(())
}

#[test]
fn test_duration_us_nullable() -> Result<(), ErrorKind> {
    run_test_case("duration_us_nullable")?;
	Ok(())
}

#[test]
fn test_binary_nullable() -> Result<(), ErrorKind> {
    run_test_case("binary_nullable")?;
	Ok(())
}

#[test]
fn test_string_nullable() -> Result<(), ErrorKind> {
    run_test_case("string_nullable")?;
	Ok(())
}

// #[test]
// fn test_profile() -> Result<(), ErrorKind> {
//     run_test_case("profile")?;
//     Ok(())
// }
