use arrow_schema::{Schema, Field, DataType, TimeUnit, SchemaRef};
use std::sync::Arc;
use crate::error::ErrorKind;

#[derive(Debug, Clone, PartialEq)]
pub enum TypeSize {
    Fixed(usize),
    Variable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PostgresType {
    Bool,
    Bytea,
    Int8,
    Int2,
    Int4,
    Char,
    Text,
    Json,
    Jsonb,
    Float4,
    Float8,
    Decimal,
    Date,
    Time,
    Timestamp,
    Interval,
    List(Box<Column>),
}

impl PostgresType {
    pub const fn size(&self) -> TypeSize {
        match &self {
            PostgresType::Bool => TypeSize::Fixed(1),
            PostgresType::Bytea => TypeSize::Variable,
            PostgresType::Int2 => TypeSize::Fixed(2),
            PostgresType::Int4 => TypeSize::Fixed(4),
            PostgresType::Int8 => TypeSize::Fixed(8),
            PostgresType::Char => TypeSize::Fixed(2),
            PostgresType::Text => TypeSize::Variable,
            PostgresType::Json => TypeSize::Variable,
            PostgresType::Jsonb => TypeSize::Variable,
            PostgresType::Float4 => TypeSize::Fixed(4),
            PostgresType::Float8 => TypeSize::Fixed(8),
            PostgresType::Date => TypeSize::Fixed(4),
            PostgresType::Time => TypeSize::Fixed(8),
            PostgresType::Timestamp => TypeSize::Fixed(8),
            PostgresType::Interval => TypeSize::Fixed(12),
            PostgresType::Decimal => TypeSize::Variable,
            PostgresType::List(_) => TypeSize::Variable,
        }
    }
    pub fn oid(&self) -> Option<u32> {
        match &self {
            PostgresType::Bool => Some(16),
            PostgresType::Bytea => Some(17),
            PostgresType::Int8 => Some(20),
            PostgresType::Int2 => Some(21),
            PostgresType::Int4 => Some(23),
            PostgresType::Char => Some(18),
            PostgresType::Text => Some(25),
            PostgresType::Json => Some(3802),
            PostgresType::Jsonb => Some(3802),
            PostgresType::Float4 => Some(700),
            PostgresType::Float8 => Some(701),
            PostgresType::Decimal => Some(1700),
            PostgresType::Date => Some(1082),
            PostgresType::Time => Some(1083),
            PostgresType::Timestamp => Some(1114),
            PostgresType::Interval => Some(1186),
            PostgresType::List(_) => None,
        }
    }
    pub fn name(&self) -> Option<String> {
        let v = match &self {
            PostgresType::Bool => "BOOL".to_string(),
            PostgresType::Bytea => "BYTEA".to_string(),
            PostgresType::Int8 => "INT8".to_string(),
            PostgresType::Int2 => "INT2".to_string(),
            PostgresType::Int4 => "INT4".to_string(),
            PostgresType::Char => "CHAR".to_string(),
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Json => "JSON".to_string(),
            PostgresType::Jsonb => "JSONB".to_string(),
            PostgresType::Float4 => "FLOAT4".to_string(),
            PostgresType::Float8 => "FLOAT8".to_string(),
            PostgresType::Decimal => "DECIMAL".to_string(),
            PostgresType::Date => "DATE".to_string(),
            PostgresType::Time => "TIME".to_string(),
            PostgresType::Timestamp => "TIMESTAMP".to_string(),
            PostgresType::Interval => "INTERVAL".to_string(),
            PostgresType::List(inner) => {
                // arrays of structs and such are not supported
                let inner_tp = inner.data_type.name().unwrap();
                format!("{inner_tp}[]")
            }
        };
        Some(v)
    }

    pub fn from_name(name: &str) -> Option<PostgresType> {
        match name {
            "BOOL" => Some(PostgresType::Bool),
            "BYTEA" => Some(PostgresType::Bytea),
            "INT8" => Some(PostgresType::Int8),
            "INT2" => Some(PostgresType::Int2),
            "INT4" => Some(PostgresType::Int4),
            "CHAR" => Some(PostgresType::Char),
            "TEXT" => Some(PostgresType::Text),
            "JSON" => Some(PostgresType::Json),
            "JSONB" => Some(PostgresType::Jsonb),
            "FLOAT4" => Some(PostgresType::Float4),
            "FLOAT8" => Some(PostgresType::Float8),
            "DECIMAL" => Some(PostgresType::Decimal),
            "DATE" => Some(PostgresType::Date),
            "TIME" => Some(PostgresType::Time),
            "TIMESTAMP" => Some(PostgresType::Timestamp),
            "INTERVAL" => Some(PostgresType::Interval),
            _ => None,
        }
    }
}

impl From<PostgresType> for DataType {
    fn from(pg_type: PostgresType) -> Self {
        match pg_type {
            PostgresType::Bool => DataType::Boolean,
            PostgresType::Bytea => DataType::LargeBinary,
            PostgresType::Int8 => DataType::Int64,
            PostgresType::Int2 => DataType::Int16,
            PostgresType::Int4 => DataType::Int32,
            PostgresType::Char => DataType::LargeUtf8,
            PostgresType::Text => DataType::LargeUtf8,
            PostgresType::Json => DataType::LargeUtf8,
            PostgresType::Jsonb => DataType::LargeUtf8,
            PostgresType::Float4 => DataType::Float32,
            PostgresType::Float8 => DataType::Float64,
            PostgresType::Decimal => DataType::LargeUtf8,
            PostgresType::Date => DataType::Date32,
            PostgresType::Time => DataType::Time64(TimeUnit::Microsecond),
            PostgresType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PostgresType::Interval => DataType::Duration(TimeUnit::Microsecond),
            PostgresType::List(_) => unimplemented!(),
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub data_type: PostgresType,
    pub nullable: bool,
}

impl Column {
    pub fn from_parts(type_str: &str, nullable: &str) -> Result<Self, ErrorKind> {
        match type_str {
            "boolean" => {
                Ok(Column {
                    data_type: PostgresType::Bool,
                    nullable: nullable == "t",
                })
            },
            "bytea" => {
                Ok(Column {
                    data_type: PostgresType::Bytea,
                    nullable: nullable == "t",
                })
            },
            "bigint" => {
                Ok(Column {
                    data_type: PostgresType::Int8,
                    nullable: nullable == "t",
                })
            },
            "smallint" => {
                Ok(Column {
                    data_type: PostgresType::Int2,
                    nullable: nullable == "t",
                })
            },
            "integer" => {
                Ok(Column {
                    data_type: PostgresType::Int4,
                    nullable: nullable == "t",
                })
            },
            "character" | "character varying" => {
                Ok(Column {
                    data_type: PostgresType::Char,
                    nullable: nullable == "t",
                })
            },
            "text" => {
                Ok(Column {
                    data_type: PostgresType::Text,
                    nullable: nullable == "t",
                })
            },
            "json" => {
                Ok(Column {
                    data_type: PostgresType::Json,
                    nullable: nullable == "t",
                })
            },
            "jsonb" => {
                Ok(Column {
                    data_type: PostgresType::Jsonb,
                    nullable: nullable == "t",
                })
            },
            "real" => {
                Ok(Column {
                    data_type: PostgresType::Float4,
                    nullable: nullable == "t",
                })
            },
            "double precision" => {
                Ok(Column {
                    data_type: PostgresType::Float8,
                    nullable: nullable == "t",
                })
            },
            "numeric" => {
                Ok(Column {
                    data_type: PostgresType::Decimal,
                    nullable: nullable == "t",
                })
            },
            "date" => {
                Ok(Column {
                    data_type: PostgresType::Date,
                    nullable: nullable == "t",
                })
            },
            "time" => {
                Ok(Column {
                    data_type: PostgresType::Time,
                    nullable: nullable == "t",
                })
            },
            "timestamp with time zone" | "timestamp without time zone" => {
                Ok(Column {
                    data_type: PostgresType::Timestamp,
                    nullable: nullable == "t",
                })
            },
            "interval" => {
                Ok(Column {
                    data_type: PostgresType::Interval,
                    nullable: nullable == "t",
                })
            },
            _ => {
                Err(ErrorKind::UnsupportedColumnType { typ: type_str.to_string() })
            }
        }

    }
}

#[derive(Debug, Clone)]
pub struct PostgresSchema {
    pub columns: Vec<(String, Column)>,
}


impl From<PostgresSchema> for SchemaRef {
    fn from(pg_schema: PostgresSchema) -> Self {
        let fields: Vec<Field> = pg_schema.columns.iter().map(|(name, col)| {
            Field::new(name, col.data_type.clone().into(), col.nullable)
        }).collect();
        Arc::new(Schema::new(fields))
    }
}

impl PostgresSchema {
    pub fn from_reader<R: std::io::Read>(mut reader: R, delim: char) -> Result<Self, ErrorKind> {
        let mut schema_str = String::new();
        reader.read_to_string(&mut schema_str)?;

        let schema = schema_str.split('\n').filter(|s| !s.is_empty()).map(|s|{
            let parts: Vec<&str> = s.splitn(3, delim).collect();
            if parts.len() != 3 {
                return Err(ErrorKind::InvalidColumnSpec{spec: s.to_string()});
            }
            let name = parts[0];
            let typ = parts[1];
            let nullable = parts[2];
            let col = Column::from_parts(typ, nullable)?;
            Ok((name.to_string(), col))
        }).collect::<Result<Vec<(String, Column)>, ErrorKind>>().map(|columns| {
            PostgresSchema { columns }

        })?;

        Ok(schema)

    }

    pub fn iter(&self) -> impl Iterator<Item = &(String, Column)> {
        self.columns.iter()
    }
}
