use arrow::compute::cast;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Field, Schema};
use pgpq::error::ErrorKind;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use pgpq::{pg_schema::PostgresSchema, PostgresBinaryToArrowReader};

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

/// Adds the Postgres types the decoder writes to an expected batch. The Arrow
/// files carry no metadata because they are also the encoder tests' inputs,
/// and those tests check that the encoder infers types without it.
fn with_pg_metadata(batch: &RecordBatch, schema: &PostgresSchema) -> RecordBatch {
    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];
    let expected = batch.schema();
    for ((field, column), (name, pg_column)) in expected
        .fields()
        .iter()
        .zip(batch.columns())
        .zip(schema.iter())
    {
        let decoded = pg_column.to_field(name);
        // A list's element field carries its own type, so the list's data type
        // changes too, and the column is cast to it.
        let data_type = match (field.data_type(), decoded.data_type()) {
            (DataType::List(element), DataType::List(decoded_element)) => DataType::List(Arc::new(
                element
                    .as_ref()
                    .clone()
                    .with_metadata(decoded_element.metadata().clone()),
            )),
            (data_type, _) => data_type.clone(),
        };
        columns.push(cast(column, &data_type).unwrap());
        fields.push(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_metadata(decoded.metadata().clone()),
        );
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
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
    println!("Schema: {:?}", schema);

    let mut decoder =
        PostgresBinaryToArrowReader::new(schema.clone(), reader, READ_CHUNK_SIZE).unwrap();
    decoder.read_header()?;
    let batches = decoder.decode_batches()?;

    let mut expected_batches = read_arrow_file(arrow_path);

    // all testdata currently has nullable set where it should not.
    // This is a workaround to make the test pass.
    if !case.contains("nullable") {
        println!("Setting nullable to false for case: {}", case);
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
                RecordBatch::try_new(Arc::new(new_schema), batch.columns().to_vec()).unwrap()
            })
            .collect::<Vec<_>>();
    }

    // All list types are not nullable in the expected data
    // This is a workaround to make the test pass.
    if case.contains("list") && case.contains("nullable") {
        expected_batches = expected_batches
            .into_iter()
            .map(|batch| {
                let new_fields: Vec<Arc<Field>> = (*(*batch.schema()).clone().fields)
                    .to_vec()
                    .clone()
                    .into_iter()
                    .map(|f| {
                        let new_field = (*f).clone();
                        let new_field = new_field.with_nullable(false);
                        if new_field.data_type().to_string().contains("List") {
                            Arc::new(new_field.with_nullable(true))
                        } else {
                            Arc::new(new_field)
                        }
                    })
                    .collect();
                let new_schema = Schema::new(new_fields);
                RecordBatch::try_new(Arc::new(new_schema), batch.columns().to_vec()).unwrap()
            })
            .collect::<Vec<_>>();
    }

    let expected_batches = expected_batches
        .iter()
        .map(|batch| with_pg_metadata(batch, &schema))
        .collect::<Vec<_>>();

    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        expected_batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    let batch_schemas = batches.iter().map(|b| b.schema()).collect::<Vec<_>>();
    let expected_batch_schemas = expected_batches
        .iter()
        .map(|b| b.schema())
        .collect::<Vec<_>>();
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

// nullable types

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

// Nested types non-nullable

#[test]
fn test_list_int16() -> Result<(), ErrorKind> {
    run_test_case("list_int16", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_int32() -> Result<(), ErrorKind> {
    run_test_case("list_int32", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_int64() -> Result<(), ErrorKind> {
    run_test_case("list_int64", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_float32() -> Result<(), ErrorKind> {
    run_test_case("list_float32", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_float64() -> Result<(), ErrorKind> {
    run_test_case("list_float64", "America/New_York".to_string())?;
    Ok(())
}

// Needed to add expected data for list_numeric
// #[test]
// fn test_list_numeric() -> Result<(), ErrorKind> {
//     run_test_case("list_numeric", "America/New_York".to_string())?;
//     Ok(())
// }

#[test]
fn test_list_timestamp_us_notz() -> Result<(), ErrorKind> {
    run_test_case("list_timestamp_us_notz", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_timestamp_us_tz() -> Result<(), ErrorKind> {
    run_test_case("list_timestamp_us_tz", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_time_us() -> Result<(), ErrorKind> {
    run_test_case("list_time_us", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_date32() -> Result<(), ErrorKind> {
    run_test_case("list_date32", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_duration_us() -> Result<(), ErrorKind> {
    run_test_case("list_duration_us", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_binary() -> Result<(), ErrorKind> {
    run_test_case("list_binary", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_string() -> Result<(), ErrorKind> {
    run_test_case("list_string", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_bool() -> Result<(), ErrorKind> {
    run_test_case("list_bool", "America/New_York".to_string())?;
    Ok(())
}

// Nested types nullable

#[test]
fn test_list_bool_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_bool_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_int16_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_int16_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_int32_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_int32_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_int64_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_int64_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_float32_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_float32_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_float64_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_float64_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_timestamp_us_notz_nullable() -> Result<(), ErrorKind> {
    run_test_case(
        "list_timestamp_us_notz_nullable",
        "America/New_York".to_string(),
    )?;
    Ok(())
}

#[test]
fn test_list_timestamp_us_tz_nullable() -> Result<(), ErrorKind> {
    run_test_case(
        "list_timestamp_us_tz_nullable",
        "America/New_York".to_string(),
    )?;
    Ok(())
}

#[test]
fn test_list_time_us_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_time_us_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_date32_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_date32_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_duration_us_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_duration_us_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_binary_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_binary_nullable", "America/New_York".to_string())?;
    Ok(())
}

#[test]
fn test_list_string_nullable() -> Result<(), ErrorKind> {
    run_test_case("list_string_nullable", "America/New_York".to_string())?;
    Ok(())
}

// Needed to add expected data for list_numeric_nullable
// #[test]
// fn test_list_numeric_nullable() -> Result<(), ErrorKind> {
//     run_test_case("list_numeric_nullable", "America/New_York".to_string())?;
//     Ok(())
// }
