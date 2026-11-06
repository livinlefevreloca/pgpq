//! Schemas built from catalog rows and from JSON.

use pgpq::error::ErrorKind;
use pgpq::pg_schema::{CatalogColumn, Column, PostgresSchema, PostgresType};

fn catalog_column(name: &str, type_schema: &str, typname: &str, nullable: bool) -> CatalogColumn {
    CatalogColumn {
        name: name.to_string(),
        type_schema: type_schema.to_string(),
        typname: typname.to_string(),
        nullable,
    }
}

#[test]
fn test_schema_from_catalog_rows() {
    let rows = [
        catalog_column("id", "pg_catalog", "int8", false),
        catalog_column("code", "pg_catalog", "varchar", true),
        catalog_column("doc", "pg_catalog", "jsonb", true),
        catalog_column("at", "pg_catalog", "timestamptz", true),
        catalog_column("tags", "pg_catalog", "_text", true),
    ];

    let schema = PostgresSchema::from_catalog(&rows, "America/New_York").unwrap();

    let types: Vec<(&str, PostgresType, bool)> = schema
        .iter()
        .map(|(name, column)| (name.as_str(), column.data_type.clone(), column.nullable))
        .collect();
    let text_element = Column {
        data_type: PostgresType::Text,
        nullable: true,
    };
    assert_eq!(
        types,
        vec![
            ("id", PostgresType::Int8, false),
            ("code", PostgresType::Varchar, true),
            ("doc", PostgresType::Jsonb, true),
            (
                "at",
                PostgresType::TimestampTz("America/New_York".to_string()),
                true
            ),
            (
                "tags",
                PostgresType::List(("item".to_string(), Box::new(text_element))),
                true
            ),
        ]
    );
}

#[test]
fn test_catalog_types_without_a_decoder_name_the_column() {
    let cases = [
        catalog_column("embedding", "public", "vector", true),
        catalog_column("addr", "pg_catalog", "inet", true),
        catalog_column("addrs", "pg_catalog", "_inet", true),
    ];
    for row in cases {
        let error = PostgresSchema::from_catalog(std::slice::from_ref(&row), "UTC").unwrap_err();
        assert!(
            matches!(&error, ErrorKind::UnsupportedColumnType { typ } if typ.contains(&row.name)),
            "{error:?}"
        );
    }
}

#[test]
fn test_schema_json_round_trip() {
    let rows = [
        catalog_column("id", "pg_catalog", "int4", false),
        catalog_column("amount", "pg_catalog", "numeric", true),
        catalog_column("at", "pg_catalog", "timestamptz", true),
        catalog_column("times", "pg_catalog", "_timestamptz", true),
        catalog_column("docs", "pg_catalog", "_jsonb", true),
    ];
    let schema = PostgresSchema::from_catalog(&rows, "Europe/Paris").unwrap();

    let json = schema.to_json();
    let round_tripped = PostgresSchema::from_json(&json).unwrap();

    assert_eq!(round_tripped.columns, schema.columns);
    assert!(json.contains(r#""type":"_timestamptz""#), "{json}");
    assert!(json.contains(r#""timezone":"Europe/Paris""#), "{json}");
}

#[test]
fn test_invalid_schema_json_is_an_error() {
    let cases = [
        "not json",
        r#"[{"name": "id", "type": "int4"}]"#,
        r#"[{"name": 1, "type": "int4", "nullable": true}]"#,
        r#"[{"name": "id", "type": "nope", "nullable": true}]"#,
    ];
    for json in cases {
        assert!(PostgresSchema::from_json(json).is_err(), "{json}");
    }
}
