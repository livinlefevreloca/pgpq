use crate::error::ErrorKind;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::Arc;

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
    Numeric,
    Date,
    Time,
    Timestamp,
    TimestampTz(String),
    Interval,
    List((String, Box<Column>)),
    Null,
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
            PostgresType::TimestampTz(_) => TypeSize::Fixed(8),
            PostgresType::Interval => TypeSize::Fixed(12),
            PostgresType::Numeric => TypeSize::Variable,
            PostgresType::List(_) => TypeSize::Variable,
            PostgresType::Null => TypeSize::Fixed(0),
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
            PostgresType::Numeric => Some(1700),
            PostgresType::Date => Some(1082),
            PostgresType::Time => Some(1083),
            PostgresType::Timestamp => Some(1114),
            PostgresType::TimestampTz(_) => Some(1182),
            PostgresType::Interval => Some(1186),
            PostgresType::List(_) => None,
            PostgresType::Null => None,
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
            PostgresType::Numeric => "DECIMAL".to_string(),
            PostgresType::Date => "DATE".to_string(),
            PostgresType::Time => "TIME".to_string(),
            PostgresType::Timestamp => "TIMESTAMP".to_string(),
            PostgresType::TimestampTz(_) => "TIMESTAMP WITH ZONE".to_string(),
            PostgresType::Interval => "INTERVAL".to_string(),
            PostgresType::List((_, column)) => {
                // arrays of structs and such are not supported
                let inner_tp = column.data_type.name().unwrap();
                format!("{inner_tp}[]")
            }
            PostgresType::Null => "NULL".to_string(),
        };
        Some(v)
    }
}


impl PostgresType {
}


impl From<PostgresType> for DataType {
    fn from(pg_type: PostgresType) -> Self {
        match pg_type {
            PostgresType::Bool => DataType::Boolean,
            PostgresType::Bytea => DataType::Binary,
            PostgresType::Int8 => DataType::Int64,
            PostgresType::Int2 => DataType::Int16,
            PostgresType::Int4 => DataType::Int32,
            PostgresType::Char => DataType::Utf8,
            PostgresType::Text => DataType::Utf8,
            PostgresType::Json => DataType::Utf8,
            PostgresType::Jsonb => DataType::Utf8,
            PostgresType::Float4 => DataType::Float32,
            PostgresType::Float8 => DataType::Float64,
            PostgresType::Numeric => DataType::Utf8,
            PostgresType::Date => DataType::Date32,
            PostgresType::Time => DataType::Time64(TimeUnit::Microsecond),
            PostgresType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PostgresType::TimestampTz(timezone) => {
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into()))
            }
            PostgresType::Interval => DataType::Duration(TimeUnit::Microsecond),
            PostgresType::List((name, column)) => {
                let name = name.replace("list_", "");
                DataType::List(Arc::new(Field::new(
                    &name,
                    column.data_type.clone().into(),
                    column.nullable,
                )))
            },
            PostgresType::Null => DataType::Null,
        }
    }
}


impl TryFrom<DataType> for PostgresType {
    type Error = ErrorKind;
    fn try_from(data_type: DataType) -> Result<Self, ErrorKind> {
        let pg_type = match data_type {
            DataType::Boolean => PostgresType::Bool,
            DataType::Binary => PostgresType::Bytea,
            DataType::Int64 => PostgresType::Int8,
            DataType::Int32 => PostgresType::Int4,
            DataType::Int16 => PostgresType::Int2,
            DataType::Utf8 => PostgresType::Text,
            DataType::Float32 => PostgresType::Float4,
            DataType::Float64 => PostgresType::Float8,
            DataType::Date32 => PostgresType::Date,
            DataType::Time64(_) => PostgresType::Time,
            DataType::Timestamp(_, tz) => {
                if let Some(timezone) = tz {
                    PostgresType::TimestampTz(timezone.to_string())
                } else {
                    PostgresType::Timestamp
                }
            }
            DataType::Duration(_) => PostgresType::Interval,
            DataType::Null => PostgresType::Null,
            _ => return Err(ErrorKind::UnsupportedArrowType { typ: data_type }),
        };
        Ok(pg_type)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub data_type: PostgresType,
    pub nullable: bool,
}

impl Column {
    pub fn from_parts(name: &str, type_str: &str, nullable: &str, timezone: String) -> Result<Self, ErrorKind> {
        match type_str {
            "boolean" => Ok(Column {
                data_type: PostgresType::Bool,
                nullable: nullable == "t",
            }),
            "bytea" => Ok(Column {
                data_type: PostgresType::Bytea,
                nullable: nullable == "t",
            }),
            "bigint" => Ok(Column {
                data_type: PostgresType::Int8,
                nullable: nullable == "t",
            }),
            "smallint" => Ok(Column {
                data_type: PostgresType::Int2,
                nullable: nullable == "t",
            }),
            "integer" => Ok(Column {
                data_type: PostgresType::Int4,
                nullable: nullable == "t",
            }),
            "character" | "character varying" => Ok(Column {
                data_type: PostgresType::Char,
                nullable: nullable == "t",
            }),
            "text" => Ok(Column {
                data_type: PostgresType::Text,
                nullable: nullable == "t",
            }),
            "json" => Ok(Column {
                data_type: PostgresType::Json,
                nullable: nullable == "t",
            }),
            "jsonb" => Ok(Column {
                data_type: PostgresType::Jsonb,
                nullable: nullable == "t",
            }),
            "real" => Ok(Column {
                data_type: PostgresType::Float4,
                nullable: nullable == "t",
            }),
            "double precision" => Ok(Column {
                data_type: PostgresType::Float8,
                nullable: nullable == "t",
            }),
            "numeric" => Ok(Column {
                data_type: PostgresType::Numeric,
                nullable: nullable == "t",
            }),
            "date" => Ok(Column {
                data_type: PostgresType::Date,
                nullable: nullable == "t",
            }),
            "time" => Ok(Column {
                data_type: PostgresType::Time,
                nullable: nullable == "t",
            }),
            "timestamp without time zone" => Ok(Column {
                data_type: PostgresType::Timestamp,
                nullable: nullable == "t",
            }),
            "timestamp with time zone" => Ok(Column {
                data_type: PostgresType::TimestampTz(timezone),
                nullable: nullable == "t",
            }),
            "interval" => Ok(Column {
                data_type: PostgresType::Interval,
                nullable: nullable == "t",
            }),
            typ if typ.ends_with("[]") => {
                Ok(Column {
                    data_type: PostgresType::List((name.to_string(), Box::new(Column {
                        data_type: Column::from_parts(name, &typ[..typ.len() - 2], "f", timezone)?.data_type,
                        nullable: true,
                    }))),
                    nullable: nullable == "t",
                })
            },
            _ => Err(ErrorKind::UnsupportedColumnType {
                typ: type_str.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostgresSchema {
    pub columns: Vec<(String, Column)>,
}

impl From<PostgresSchema> for SchemaRef {
    fn from(pg_schema: PostgresSchema) -> Self {
        let fields: Vec<Field> = pg_schema
            .columns
            .iter()
            .map(|(name, col)| Field::new(name, col.data_type.clone().into(), col.nullable))
            .collect();
        Arc::new(Schema::new(fields))
    }
}

impl TryFrom<Schema> for PostgresSchema {
    type Error = ErrorKind;
    fn try_from(schema: Schema) -> Result<PostgresSchema, ErrorKind> {
        let columns: Result<Vec<_>, ErrorKind> = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().to_string();
                let data_type = field.data_type().clone();
                let nullable = field.is_nullable();
                let data_type = PostgresType::try_from(data_type)?;
                let col = Column {
                    data_type,
                    nullable,
                };
                Ok((name, col))
            })
            .collect();

        Ok(PostgresSchema { columns: columns? })
    }
}

impl PostgresSchema {
    pub fn from_reader<R: std::io::Read>(
        mut reader: R,
        delim: char,
        timezone: String,
    ) -> Result<Self, ErrorKind> {
        let mut schema_str = String::new();
        reader.read_to_string(&mut schema_str)?;

        let schema = schema_str
            .split('\n')
            .filter(|s| !s.is_empty())
            .map(|s| {
                let parts: Vec<&str> = s.splitn(3, delim).collect();
                if parts.len() != 3 {
                    return Err(ErrorKind::InvalidColumnSpec {
                        spec: s.to_string(),
                    });
                }
                let name = parts[0];
                let typ = parts[1];
                let nullable = parts[2];
                let col = Column::from_parts(name, typ, nullable, timezone.to_string())?;
                Ok((name.to_string(), col))
            })
            .collect::<Result<Vec<(String, Column)>, ErrorKind>>()
            .map(|columns| PostgresSchema { columns })?;

        Ok(schema)
    }

    pub fn nullify_columns(mut self, columns: &[String]) -> Self {
        for (name, col) in self.columns.iter_mut() {
            if columns.contains(name) {
                col.data_type = PostgresType::Null;
            }
        }
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = &(String, Column)> {
        self.columns.iter()
    }
}
