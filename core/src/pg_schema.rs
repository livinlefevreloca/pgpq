use crate::error::ErrorKind;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

/// The Arrow field metadata key that holds a column's Postgres type name.
///
/// The decoder writes it on every field, and the encoder reads it, so data that
/// went through Arrow or Parquet loads back into the type it came from.
pub const PG_TYPE_METADATA_KEY: &str = "pg_type";

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
    Varchar,
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
            PostgresType::Char => TypeSize::Variable,
            PostgresType::Varchar => TypeSize::Variable,
            PostgresType::Text => TypeSize::Variable,
            PostgresType::Json => TypeSize::Variable,
            PostgresType::Jsonb => TypeSize::Variable,
            PostgresType::Float4 => TypeSize::Fixed(4),
            PostgresType::Float8 => TypeSize::Fixed(8),
            PostgresType::Date => TypeSize::Fixed(4),
            PostgresType::Time => TypeSize::Fixed(8),
            PostgresType::Timestamp => TypeSize::Fixed(8),
            PostgresType::TimestampTz(_) => TypeSize::Fixed(8),
            PostgresType::Interval => TypeSize::Fixed(16),
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
            PostgresType::Char => Some(1042),
            PostgresType::Varchar => Some(1043),
            PostgresType::Text => Some(25),
            PostgresType::Json => Some(114),
            PostgresType::Jsonb => Some(3802),
            PostgresType::Float4 => Some(700),
            PostgresType::Float8 => Some(701),
            PostgresType::Numeric => Some(1700),
            PostgresType::Date => Some(1082),
            PostgresType::Time => Some(1083),
            PostgresType::Timestamp => Some(1114),
            PostgresType::TimestampTz(_) => Some(1184),
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
            PostgresType::Char => "BPCHAR".to_string(),
            PostgresType::Varchar => "VARCHAR".to_string(),
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Json => "JSON".to_string(),
            PostgresType::Jsonb => "JSONB".to_string(),
            PostgresType::Float4 => "FLOAT4".to_string(),
            PostgresType::Float8 => "FLOAT8".to_string(),
            PostgresType::Numeric => "DECIMAL".to_string(),
            PostgresType::Date => "DATE".to_string(),
            PostgresType::Time => "TIME".to_string(),
            PostgresType::Timestamp => "TIMESTAMP".to_string(),
            PostgresType::TimestampTz(_) => "TIMESTAMPTZ".to_string(),
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
    /// The type's name in pg_type.typname. An array is named after its element
    /// with a leading underscore, as in the catalog. Null has no Postgres type.
    pub fn typname(&self) -> Option<String> {
        let name = match &self {
            PostgresType::Bool => "bool",
            PostgresType::Bytea => "bytea",
            PostgresType::Int8 => "int8",
            PostgresType::Int2 => "int2",
            PostgresType::Int4 => "int4",
            PostgresType::Char => "bpchar",
            PostgresType::Varchar => "varchar",
            PostgresType::Text => "text",
            PostgresType::Json => "json",
            PostgresType::Jsonb => "jsonb",
            PostgresType::Float4 => "float4",
            PostgresType::Float8 => "float8",
            PostgresType::Numeric => "numeric",
            PostgresType::Date => "date",
            PostgresType::Time => "time",
            PostgresType::Timestamp => "timestamp",
            PostgresType::TimestampTz(_) => "timestamptz",
            PostgresType::Interval => "interval",
            PostgresType::List((_, column)) => {
                return column
                    .data_type
                    .typname()
                    .map(|element| format!("_{element}"))
            }
            PostgresType::Null => return None,
        };
        Some(name.to_string())
    }

    /// The Postgres type a field's metadata names, with the parts the name
    /// leaves out, an array's element and a timestamptz's time zone, taken
    /// from the field's Arrow type.
    fn from_typname(typname: &str, field: &Field) -> Result<Self, ErrorKind> {
        let pg_type = match typname {
            "bool" => PostgresType::Bool,
            "bytea" => PostgresType::Bytea,
            "int8" => PostgresType::Int8,
            "int2" => PostgresType::Int2,
            "int4" => PostgresType::Int4,
            "bpchar" => PostgresType::Char,
            "varchar" => PostgresType::Varchar,
            "text" => PostgresType::Text,
            "json" => PostgresType::Json,
            "jsonb" => PostgresType::Jsonb,
            "float4" => PostgresType::Float4,
            "float8" => PostgresType::Float8,
            "numeric" => PostgresType::Numeric,
            "date" => PostgresType::Date,
            "time" => PostgresType::Time,
            "timestamp" => PostgresType::Timestamp,
            "timestamptz" => match field.data_type() {
                DataType::Timestamp(_, Some(timezone)) => {
                    PostgresType::TimestampTz(timezone.to_string())
                }
                _ => PostgresType::TimestampTz("UTC".to_string()),
            },
            "interval" => PostgresType::Interval,
            array if array.starts_with('_') => match field.data_type() {
                DataType::List(element) | DataType::LargeList(element) => PostgresType::List((
                    element.name().to_string(),
                    Box::new(Column::try_from(element.as_ref())?),
                )),
                other => {
                    return Err(ErrorKind::MetadataTypeMismatch {
                        field: field.name().to_string(),
                        pg_type: typname.to_string(),
                        data_type: other.clone(),
                    })
                }
            },
            _ => {
                return Err(ErrorKind::UnsupportedColumnType {
                    typ: typname.to_string(),
                })
            }
        };
        Ok(pg_type)
    }
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
            PostgresType::Varchar => DataType::Utf8,
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
                DataType::List(Arc::new(column.to_field(&name)))
            }
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
    /// The Arrow field for this column, with its Postgres type in the field's
    /// metadata.
    pub fn to_field(&self, name: &str) -> Field {
        let field = Field::new(name, self.data_type.clone().into(), self.nullable);
        match self.data_type.typname() {
            Some(typname) => {
                field.with_metadata(HashMap::from([(PG_TYPE_METADATA_KEY.to_string(), typname)]))
            }
            None => field,
        }
    }

    pub fn from_parts(
        name: &str,
        type_str: &str,
        nullable: &str,
        timezone: String,
    ) -> Result<Self, ErrorKind> {
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
            "character" => Ok(Column {
                data_type: PostgresType::Char,
                nullable: nullable == "t",
            }),
            "character varying" => Ok(Column {
                data_type: PostgresType::Varchar,
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
            typ if typ.ends_with("[]") => Ok(Column {
                data_type: PostgresType::List((
                    name.to_string(),
                    Box::new(Column {
                        data_type: Column::from_parts(name, &typ[..typ.len() - 2], "f", timezone)?
                            .data_type,
                        nullable: true,
                    }),
                )),
                nullable: nullable == "t",
            }),
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

impl TryFrom<&Field> for Column {
    type Error = ErrorKind;

    /// Reads the Postgres type from the field's metadata, or infers it from the
    /// Arrow type when the field has none.
    fn try_from(field: &Field) -> Result<Column, ErrorKind> {
        let data_type = match field.metadata().get(PG_TYPE_METADATA_KEY) {
            Some(typname) => PostgresType::from_typname(typname, field)?,
            None => PostgresType::try_from(field.data_type().clone())?,
        };
        Ok(Column {
            data_type,
            nullable: field.is_nullable(),
        })
    }
}

impl From<PostgresSchema> for SchemaRef {
    fn from(pg_schema: PostgresSchema) -> Self {
        let fields: Vec<Field> = pg_schema
            .columns
            .iter()
            .map(|(name, col)| col.to_field(name))
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
            .map(|field| Ok((field.name().to_string(), Column::try_from(field.as_ref())?)))
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

    pub fn iter(&self) -> impl Iterator<Item = &(String, Column)> {
        self.columns.iter()
    }
}
