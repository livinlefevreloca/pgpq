//! Postgres types carried in Arrow field metadata. The decoder writes them and
//! the encoder reads them, so data loads back into the types it came from.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use pgpq::encoders::{BuildEncoder, EncoderBuilder};
use pgpq::error::ErrorKind;
use pgpq::pg_schema::{Column, PostgresSchema, PostgresType, PG_TYPE_METADATA_KEY};

fn column(data_type: PostgresType) -> Column {
    Column {
        data_type,
        nullable: true,
    }
}

fn list_of(name: &str, element: PostgresType) -> PostgresType {
    PostgresType::List((name.to_string(), Box::new(column(element))))
}

fn field_with_pg_type(name: &str, data_type: DataType, pg_type: &str) -> Arc<Field> {
    Arc::new(
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            PG_TYPE_METADATA_KEY.to_string(),
            pg_type.to_string(),
        )])),
    )
}

fn encoded_type(field: Arc<Field>) -> Result<PostgresType, ErrorKind> {
    Ok(EncoderBuilder::try_new(field)?.schema().data_type)
}

#[test]
fn test_arrow_schema_carries_postgres_types() {
    let schema = PostgresSchema {
        columns: vec![
            ("doc".to_string(), column(PostgresType::Jsonb)),
            ("raw".to_string(), column(PostgresType::Json)),
            ("code".to_string(), column(PostgresType::Char)),
            (
                "at".to_string(),
                column(PostgresType::TimestampTz("America/New_York".to_string())),
            ),
            (
                "docs".to_string(),
                column(list_of("docs", PostgresType::Jsonb)),
            ),
        ],
    };

    let arrow: SchemaRef = schema.clone().into();
    let pg_types: Vec<&str> = arrow
        .fields()
        .iter()
        .map(|f| f.metadata()[PG_TYPE_METADATA_KEY].as_str())
        .collect();
    assert_eq!(
        pg_types,
        ["jsonb", "json", "bpchar", "timestamptz", "_jsonb"]
    );
    let DataType::List(element) = arrow.field(4).data_type() else {
        panic!("docs should be a list");
    };
    assert_eq!(element.metadata()[PG_TYPE_METADATA_KEY], "jsonb");

    let round_tripped = PostgresSchema::try_from(arrow.as_ref().clone()).unwrap();
    assert_eq!(round_tripped.columns, schema.columns);
}

#[test]
fn test_schema_without_metadata_is_inferred() {
    let arrow = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]);

    let schema = PostgresSchema::try_from(arrow).unwrap();

    assert_eq!(schema.columns[0].1.data_type, PostgresType::Text);
    assert_eq!(schema.columns[1].1.data_type, PostgresType::Timestamp);
}

#[test]
fn test_encoder_writes_the_type_in_the_metadata() {
    let jsonb = field_with_pg_type("doc", DataType::Utf8, "jsonb");
    assert_eq!(encoded_type(jsonb).unwrap(), PostgresType::Jsonb);

    let json = field_with_pg_type("doc", DataType::LargeUtf8, "json");
    assert_eq!(encoded_type(json).unwrap(), PostgresType::Json);

    let element = field_with_pg_type("docs", DataType::Utf8, "jsonb");
    let docs = field_with_pg_type("docs", DataType::List(element), "_jsonb");
    assert_eq!(
        encoded_type(docs).unwrap().typname().as_deref(),
        Some("_jsonb")
    );
}

#[test]
fn test_encoder_writes_each_string_type_as_itself() {
    let cases = [
        ("text", PostgresType::Text),
        ("varchar", PostgresType::Varchar),
        ("bpchar", PostgresType::Char),
        ("numeric", PostgresType::Numeric),
    ];
    for (pg_type, expected) in cases {
        let field = field_with_pg_type("value", DataType::Utf8, pg_type);
        assert_eq!(encoded_type(field).unwrap(), expected, "{pg_type}");
    }
}

#[test]
fn test_encoder_without_metadata_infers_the_type() {
    let field = Arc::new(Field::new("doc", DataType::Utf8, true));
    assert_eq!(encoded_type(field).unwrap(), PostgresType::Text);
}

#[test]
fn test_encoder_rejects_metadata_its_arrow_type_cannot_encode() {
    let cases = [
        field_with_pg_type("id", DataType::Int32, "int8"),
        field_with_pg_type("amount", DataType::Int32, "numeric"),
        field_with_pg_type("count", DataType::Utf8, "int4"),
        field_with_pg_type(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamptz",
        ),
        field_with_pg_type(
            "ids",
            DataType::List(Arc::new(Field::new("ids", DataType::Int32, true))),
            "_int8",
        ),
    ];
    for field in cases {
        let name = field.name().clone();
        let result = EncoderBuilder::try_new(field);
        assert!(
            matches!(result, Err(ErrorKind::MetadataTypeMismatch { .. })),
            "{name}: {result:?}"
        );
    }
}
