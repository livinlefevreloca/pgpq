#![allow(clippy::redundant_closure_call)]

use arrow_array::{self, ArrayRef};
use std::fmt::Debug;
use std::sync::Arc;
use crate::encoders::{PG_BASE_DATE_OFFSET, PG_BASE_TIMESTAMP_OFFSET_US};
use arrow_array::types::GenericBinaryType;
use arrow_array::builder::GenericByteBuilder;
use arrow_array::{
    BooleanArray, Date32Array, DurationMicrosecondArray, Float32Array, Float64Array,
    GenericStringArray, Int16Array, Int32Array, Int64Array,
    Time64MicrosecondArray, TimestampMicrosecondArray
};

use crate::error::ErrorKind;
use crate::pg_schema::{PostgresSchema, PostgresType};

pub(crate) struct ConsumableBuf<'a> {
    inner: &'a [u8],
    consumed: usize,
}

impl Debug for ConsumableBuf<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.inner[self.consumed..])
    }
}

// const used for stringifying postgres decimals
const DEC_DIGITS: i16 = 4;
// const used for determining sign of numeric
const NUMERIC_NEG: i16 = 0x4000;

impl ConsumableBuf<'_> {
    pub fn new(inner: &'_ [u8]) -> ConsumableBuf<'_> {
        ConsumableBuf { inner, consumed: 0 }
    }

    pub fn consume_into_u32(&mut self) -> Result<u32, ErrorKind> {
        if self.consumed + 4 > self.inner.len() {
            return Err(ErrorKind::DataSize {
                found: self.inner.len() - self.consumed,
                expected: 4,
            });
        }
        let res = u32::from_be_bytes(
            self.inner[self.consumed..self.consumed + 4]
                .try_into().unwrap()
        );
        self.consumed += 4;
        Ok(res)
    }

    pub fn consume_into_u16(&mut self) -> Result<u16, ErrorKind> {
        if self.consumed + 2 > self.inner.len() {
            return Err(ErrorKind::DataSize {
                found: self.inner.len() - self.consumed,
                expected: 2,
            });
        }
        let res = u16::from_be_bytes(
            self.inner[self.consumed..self.consumed + 2]
                .try_into().unwrap()
        );
        self.consumed += 2;
        Ok(res)
    }

    pub fn consume_into_vec_n(&mut self, n: usize) -> Result<Vec<u8>, ErrorKind> {
        if self.consumed + n > self.inner.len() {
            return Err(ErrorKind::DataSize {
                found: self.inner.len() - self.consumed,
                expected: n,
            });
        }
        let data = self.inner[self.consumed..self.consumed + n].to_vec();
        self.consumed += n;
        if data.len() != n {
            return Err(ErrorKind::DataSize {
                found: data.len(),
                expected: n,
            });
        }
        Ok(data)
    }

    pub fn remaining(&self) -> usize {
        self.inner.len() - self.consumed
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn swallow(&mut self, n: usize) {
        self.consumed += n;
    }
}

pub(crate) trait Decode {
    fn decode(&mut self, buf: &mut ConsumableBuf) -> Result<(), ErrorKind>;
    fn finish(&mut self, column_len: usize) -> Result<ArrayRef, ErrorKind>;
    fn column_len(&self) -> usize;
    fn name(&self) -> &str;
}

macro_rules! impl_decode {
    ($struct_name:ident, $size:expr, $transform:expr, $array_kind:ident) => {
        impl Decode for $struct_name {
            fn decode(&mut self, buf: &mut ConsumableBuf<'_>) -> Result<(), ErrorKind> {
                let field_size = buf.consume_into_u32()?;
                if field_size == u32::MAX {
                    self.arr.push(None);
                    return Ok(());
                }
                if field_size != $size {
                    return Err(ErrorKind::DataSize {
                        found: field_size as usize,
                        expected: $size,
                    });
                }


                let data = buf.consume_into_vec_n(field_size as usize)?;
                // Unwrap is safe here because have checked the field size is the expected size
                // above

                let value = $transform(data.try_into().unwrap());
                self.arr.push(Some(value));

                Ok(())
            }

            fn name(&self) -> &str {
                &self.name
            }

            fn column_len(&self) -> usize {
                self.arr.len()
            }

            fn finish(&mut self, column_len: usize) -> Result<ArrayRef, ErrorKind> {
                let mut data = std::mem::take(&mut self.arr);
                data.resize(column_len, None);
                let array = Arc::new($array_kind::from(data));
                Ok(array as ArrayRef)
            }
        }
    };
}

macro_rules! impl_decode_fallible {
    ($struct_name:ident, $size:expr, $transform:expr, $array_kind:ident) => {
        impl Decode for $struct_name {
            fn decode(&mut self, buf: &mut ConsumableBuf<'_>) -> Result<(), ErrorKind> {
                let field_size = buf.consume_into_u32()?;

                if field_size == u32::MAX {
                    self.arr.push(None);
                    return Ok(());
                }

                if field_size != $size {
                    return Err(ErrorKind::DataSize {
                        found: field_size as usize,
                        expected: $size,
                    });
                }

                let data = buf.consume_into_vec_n(field_size as usize)?;
                // Unwrap is safe here because have checked the field size is the expected size
                // above
                match $transform(data.try_into().unwrap()) {
                    Ok(v) => self.arr.push(Some(v)),
                    Err(e) => {
                        // If the error is a decode error, return a decode error with the name of the field
                        return match e {
                            ErrorKind::Decode { reason, .. } => {
                                return Err(ErrorKind::Decode {
                                    reason,
                                    name: self.name.to_string(),
                                })
                            }
                            _ => Err(e),
                        };
                    }
                };

                Ok(())
            }

            fn name(&self) -> &str {
                &self.name
            }

            fn column_len(&self) -> usize {
                self.arr.len()
            }

            fn finish(&mut self, column_len: usize) -> Result<ArrayRef, ErrorKind> {
                let mut data = std::mem::take(&mut self.arr);
                data.resize(column_len, None);
                let array = Arc::new($array_kind::from(data));
                Ok(array as ArrayRef)
            }
        }
    };
}

macro_rules! impl_decode_variable_size {
    ($struct_name:ident, $transform:expr, $extra_bytes:expr, $array_kind:ident, $offset_size:ident) => {
        impl Decode for $struct_name {
            fn decode(&mut self, buf: &mut ConsumableBuf<'_>) -> Result<(), ErrorKind> {
                let field_size = buf.consume_into_u32()?;
                if field_size == u32::MAX {
                    self.arr.push(None);
                    return Ok(());
                }

                if field_size > buf.remaining() as u32 {
                    return Err(ErrorKind::DataSize {
                        found: buf.remaining() as usize,
                        expected: field_size as usize,
                    });
                }

                // Consume and any extra data that is not part of the field
                // This is needed for example on jsonb fields where the first
                // byte is the version number. This is more efficient than
                // using remove in the transform function or creating a new
                // vec from a slice of the data passed into it.
                buf.swallow($extra_bytes);

                let data = buf.consume_into_vec_n(field_size as usize)?;
                match $transform(data.try_into().unwrap()) {
                    Ok(v) => self.arr.push(Some(v)),
                    Err(e) => {
                        // If the error is a decode error, return a decode error with the name of the field
                        return match e {
                            ErrorKind::Decode { reason, .. } => {
                                return Err(ErrorKind::Decode {
                                    reason,
                                    name: self.name.to_string(),
                                })
                            }
                            _ => Err(e),
                        };
                    }
                };

                Ok(())
            }

            fn name(&self) -> &str {
                &self.name
            }

            fn column_len(&self) -> usize {
                self.arr.len()
            }

            fn finish(&mut self, column_len: usize) -> Result<ArrayRef, ErrorKind> {
                let mut data = std::mem::take(&mut self.arr);
                data.resize(column_len, None);
                let array = Arc::new($array_kind::<$offset_size>::from(data));
                Ok(array as ArrayRef)
            }
        }
    };
}

pub struct BooleanDecoder {
    name: String,
    arr: Vec<Option<bool>>,
}

impl_decode!(BooleanDecoder, 1, |b: [u8; 1]| b[0] == 1, BooleanArray);

pub struct Int16Decoder {
    name: String,
    arr: Vec<Option<i16>>,
}

impl_decode!(Int16Decoder, 2, i16::from_be_bytes, Int16Array);

pub struct Int32Decoder {
    name: String,
    arr: Vec<Option<i32>>,
}

impl_decode!(Int32Decoder, 4, i32::from_be_bytes, Int32Array);

pub struct Int64Decoder {
    name: String,
    arr: Vec<Option<i64>>,
}

impl_decode!(Int64Decoder, 8, i64::from_be_bytes, Int64Array);

pub struct Float32Decoder {
    name: String,
    arr: Vec<Option<f32>>,
}

impl_decode!(Float32Decoder, 4, f32::from_be_bytes, Float32Array);

pub struct Float64Decoder {
    name: String,
    arr: Vec<Option<f64>>,
}

impl_decode!(Float64Decoder, 8, f64::from_be_bytes, Float64Array);

/// Convert Postgres timestamps (microseconds since 2000-01-01) to Arrow timestamps (mircroseconds since 1970-01-01)
#[inline(always)]
fn convert_pg_timestamp_to_arrow_timestamp_microseconds(
    timestamp_us: i64,
) -> Result<i64, ErrorKind> {
    // adjust the timestamp from microseconds since 2000-01-01 to microseconds since 1970-01-01 checking for overflows and underflow
    println!("timestamp_us: {}", timestamp_us);
    timestamp_us
        .checked_add(PG_BASE_TIMESTAMP_OFFSET_US)
        .ok_or_else(|| ErrorKind::Decode {
            reason: "Overflow converting microseconds since 2000-01-01 (Postgres) to microseconds since 1970-01-01 (Arrow)".to_string(),
            name: "".to_string(),
        })
}

pub struct TimestampMicrosecondDecoder {
    name: String,
    arr: Vec<Option<i64>>,
}

impl_decode_fallible!(
    TimestampMicrosecondDecoder,
    8,
    |b| {
        let timestamp_us = i64::from_be_bytes(b);
        convert_pg_timestamp_to_arrow_timestamp_microseconds(timestamp_us)
    },
    TimestampMicrosecondArray
);

/// Convert Postgres dates (days since 2000-01-01) to Arrow dates (days since 1970-01-01)
#[inline(always)]
fn convert_pg_date_to_arrow_date(date: i32) -> Result<i32, ErrorKind> {
    date.checked_add(PG_BASE_DATE_OFFSET).ok_or_else(|| ErrorKind::Decode {
        reason: "Overflow converting days since 2000-01-01 (Postgres) to days since 1970-01-01 (Arrow)".to_string(),
        name: "".to_string(),
    })
}

pub struct Date32Decoder {
    name: String,
    arr: Vec<Option<i32>>,
}

impl_decode_fallible!(
    Date32Decoder,
    4,
    |b| {
        let date = i32::from_be_bytes(b);
        convert_pg_date_to_arrow_date(date)
    },
    Date32Array
);

pub struct Time64MicrosecondDecoder {
    name: String,
    arr: Vec<Option<i64>>,
}

impl_decode!(
    Time64MicrosecondDecoder,
    8,
    i64::from_be_bytes,
    Time64MicrosecondArray
);

/// Convert Postgres durations to Arrow durations (milliseconds)
fn convert_pg_duration_to_arrow_duration(
    duration_us: i64,
    duration_days: i32,
    duration_months: i32,
) -> Result<i64, ErrorKind> {
    let days = (duration_days as i64)
        .checked_mul(24 * 60 * 60 * 1_000_000)
        .ok_or_else(|| ErrorKind::Decode {
            reason: "Overflow converting days to microseconds".to_string(),
            name: "".to_string(),
        })?;
    let months = (duration_months as i64)
        .checked_mul(30 * 24 * 60 * 60 * 1_000_000)
        .ok_or_else(|| ErrorKind::Decode {
            reason: "Overflow converting months to microseconds".to_string(),
            name: "".to_string(),
        })?;

    duration_us
        .checked_add(days)
        .ok_or_else(|| ErrorKind::Decode {
            reason: "Overflow adding days in microseconds to duration microseconds".to_string(),
            name: "".to_string(),
        })
        .map(|v| {
            v.checked_add(months).ok_or_else(|| ErrorKind::Decode {
                reason: "Overflow adding months in microseconds to duration microseconds"
                    .to_string(),
                name: "".to_string(),
            })
        })?
}

pub struct DurationMicrosecondDecoder {
    name: String,
    arr: Vec<Option<i64>>,
}

impl_decode_fallible!(
    DurationMicrosecondDecoder,
    16,
    |b: [u8; 16]| {
        // Unwrap here since we know the exact size of the array we are passing
        let duration_us = i64::from_be_bytes(b[..8].try_into().unwrap());
        let duration_days = i32::from_be_bytes(b[8..12].try_into().unwrap());
        let duration_months = i32::from_be_bytes(b[12..16].try_into().unwrap());
        convert_pg_duration_to_arrow_duration(duration_us, duration_days, duration_months)
    },
    DurationMicrosecondArray
);

pub struct StringDecoder {
    name: String,
    arr: Vec<Option<String>>,
}

impl_decode_variable_size!(
    StringDecoder,
    |b: Vec<u8>| {
        String::from_utf8(b).map_err(|_| ErrorKind::Decode {
            reason: "Invalid UTF-8 string".to_string(),
            name: "".to_string(),
        })
    },
    0,
    GenericStringArray,
    i64
);

pub struct BinaryDecoder {
    name: String,
    arr: Vec<Option<Vec<u8>>>,
}

impl Decode for BinaryDecoder {
    fn decode(&mut self, buf: &mut ConsumableBuf<'_>) -> Result<(), ErrorKind> {
        let field_size = buf.consume_into_u32()?;
        if field_size == u32::MAX {
            self.arr.push(None);
            return Ok(());
        }

        let data = buf.consume_into_vec_n(field_size as usize)?;
        self.arr.push(Some(data));

        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn column_len(&self) -> usize {
        self.arr.len()
    }

    fn finish(&mut self, column_len: usize) -> Result<ArrayRef, ErrorKind> {
        let mut data = std::mem::take(&mut self.arr);
        data.resize(column_len, None);

        let mut builder: GenericByteBuilder<GenericBinaryType<i64>>  =  GenericByteBuilder::new();
        for v in data {
            match v {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        let array = Arc::new(builder.finish());
        Ok(array as ArrayRef)
    }
}

pub struct JsonbDecoder {
    name: String,
    arr: Vec<Option<String>>,
}

impl_decode_variable_size!(
    JsonbDecoder,
    |b: Vec<u8>| {
        String::from_utf8(b).map_err(|_| ErrorKind::Decode {
            reason: "Invalid UTF-8 string".to_string(),
            name: "".to_string(),
        })
    },
    // Remove the first byte which is the version number
    // https://www.postgresql.org/docs/13/datatype-json.html
    1,
    GenericStringArray,
    i64
);

/// Logic ported from src/backend/utils/adt/numeric.c:get_str_from_var
fn parse_pg_decimal_to_string(data: Vec<u8>) -> Result<String, ErrorKind> {
    // Decimals will be decoded to strings since rust does not have a ubiquitos
    // decimal type and arrow does not implment `From` for any of them. Arrow
    // does have a Decimal128 array but its only accepts i128s as input
    // TODO: Seems like there could be a fast path here for simpler numbers
    let ndigits = i16::from_be_bytes(data[0..2].try_into().unwrap());
    let weight = i16::from_be_bytes(data[2..4].try_into().unwrap());
    let sign = i16::from_be_bytes(data[4..6].try_into().unwrap());
    let scale = i16::from_be_bytes(data[6..8].try_into().unwrap());
    let digits: Vec<i16> = data[8..8 + ndigits as usize].chunks(2).map(|c| i16::from_be_bytes(c.try_into().unwrap())).collect();

    // the number of digits before the decimal place
    let pre_decimal = (weight + 1) * DEC_DIGITS;

    // scale is the number digits after the decimal place.
    // 2 is for a possible sign and decimal point
    let str_len: usize = (pre_decimal + DEC_DIGITS + 2 + scale) as usize;

    // -1 because we dont need to account for the null terminator
    let mut decimal: Vec<u8> = Vec::with_capacity(str_len - 1);

    // put a negative sign if the numeric is negative
    if sign == NUMERIC_NEG {
        decimal.push(b'-');
    }

    let mut digits_index = 0;
    // If weight is less than 0 we have a fractional number.
    // Put a 0 before the decimal.
    if weight < 0 {
        decimal.push(b'0');
    // Otherwise put digits in the decimal string by computing the value for each place in decimal
    } else {
        while digits_index <= weight {
           let mut dig = if digits_index  < ndigits { digits[digits_index as usize] } else { 0 };
           let mut putit = digits_index > 0;

           /* below unwraps too:
                d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				*cp++ = dig + '0';
            */

            let mut place = 1000;
            while place > 1 {
                let d1 = dig / place;
                dig -= d1 * place;
                putit |= d1 > 0;
                if putit { decimal.push(d1 as u8 + b'0') }
                place /= 10;
            }
            decimal.push(dig as u8 + b'0');
            digits_index += 1;
        }

        // If scale is > 0 we have digits after the decimal point
        if scale > 0 {
            decimal.push(b'.');
        }
    }

    let mut i = 0;
    while  i < scale {
        let mut dig = if digits_index >= 0 && digits_index < ndigits { digits[digits_index as usize] } else { 0 };
        let mut place = 1000;
        // Same as the loop above but no putit since all digits prior to the
        // scale-TH digit are significant
        while place > 1 {
            let d1 = dig / place;
            dig -= d1 * place;
            decimal.push(d1 as u8 + b'0');
            place /= 10;
        }
        decimal.push(dig as u8 + b'0');
        i += 1;
    }

    // unwrap will not fail here as we know ever value in our decimal vec is ascii;
    Ok(String::from_utf8(decimal).unwrap())
}

pub struct DecimalDecoder {
    name: String,
    arr: Vec<Option<String>>,
}

impl_decode_variable_size!(
    DecimalDecoder,
    parse_pg_decimal_to_string,
    0,
    GenericStringArray,
    i64
);


//
pub enum Decoder {
    Boolean(BooleanDecoder),
    Int16(Int16Decoder),
    Int32(Int32Decoder),
    Int64(Int64Decoder),
    Float32(Float32Decoder),
    Float64(Float64Decoder),
    Decimal(DecimalDecoder),
    TimestampMicrosecond(TimestampMicrosecondDecoder),
    Date32(Date32Decoder),
    Time64Microsecond(Time64MicrosecondDecoder),
    DurationMicrosecond(DurationMicrosecondDecoder),
    String(StringDecoder),
    Binary(BinaryDecoder),
    Jsonb(JsonbDecoder),
}
//
impl Decoder {
    pub fn new(schema: &PostgresSchema) -> Vec<Self> {
        schema
            .iter()
            .map(|(name, column)| match column.data_type {
                PostgresType::Bool => Decoder::Boolean(BooleanDecoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Int2 => Decoder::Int16(Int16Decoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Int4 => Decoder::Int32(Int32Decoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Int8 => Decoder::Int64(Int64Decoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Float4 => Decoder::Float32(Float32Decoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Float8 => Decoder::Float64(Float64Decoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Decimal => {
                    Decoder::Decimal(DecimalDecoder { name: name.to_string(), arr: vec![] })
                }
                PostgresType::Timestamp => {
                    Decoder::TimestampMicrosecond(TimestampMicrosecondDecoder {
                        name: name.to_string(),
                        arr: vec![],
                    })
                }
                PostgresType::Date => Decoder::Date32(Date32Decoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Time => Decoder::Time64Microsecond(Time64MicrosecondDecoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Interval => {
                    Decoder::DurationMicrosecond(DurationMicrosecondDecoder {
                        name: name.to_string(),
                        arr: vec![],
                    })
                }
                PostgresType::Text | PostgresType::Char | PostgresType::Json => Decoder::String(StringDecoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                PostgresType::Bytea => Decoder::Binary(BinaryDecoder {
                    name: name.to_string(),
                    arr: vec![],
                }),
                _ => unimplemented!(),
            })
            .collect()
    }

    pub(crate) fn apply(&mut self, buf: &mut ConsumableBuf) -> Result<(), ErrorKind> {
        match *self {
            Decoder::Boolean(ref mut decoder) => decoder.decode(buf),
            Decoder::Int16(ref mut decoder) => decoder.decode(buf),
            Decoder::Int32(ref mut decoder) => decoder.decode(buf),
            Decoder::Int64(ref mut decoder) => decoder.decode(buf),
            Decoder::Float32(ref mut decoder) => decoder.decode(buf),
            Decoder::Float64(ref mut decoder) => decoder.decode(buf),
            Decoder::Decimal(ref mut decoder) => decoder.decode(buf),
            Decoder::TimestampMicrosecond(ref mut decoder) => decoder.decode(buf),
            Decoder::Date32(ref mut decoder) => decoder.decode(buf),
            Decoder::Time64Microsecond(ref mut decoder) => decoder.decode(buf),
            Decoder::DurationMicrosecond(ref mut decoder) => decoder.decode(buf),
            Decoder::String(ref mut decoder) => decoder.decode(buf),
            Decoder::Binary(ref mut decoder) => decoder.decode(buf),
            Decoder::Jsonb(ref mut decoder) => decoder.decode(buf),
        }
    }

    pub(crate) fn column_len(&self) -> usize {
        match *self {
            Decoder::Boolean(ref decoder) => decoder.column_len(),
            Decoder::Int16(ref decoder) => decoder.column_len(),
            Decoder::Int32(ref decoder) => decoder.column_len(),
            Decoder::Int64(ref decoder) => decoder.column_len(),
            Decoder::Float32(ref decoder) => decoder.column_len(),
            Decoder::Float64(ref decoder) => decoder.column_len(),
            Decoder::Decimal(ref decoder) => decoder.column_len(),
            Decoder::TimestampMicrosecond(ref decoder) => decoder.column_len(),
            Decoder::Date32(ref decoder) => decoder.column_len(),
            Decoder::Time64Microsecond(ref decoder) => decoder.column_len(),
            Decoder::DurationMicrosecond(ref decoder) => decoder.column_len(),
            Decoder::String(ref decoder) => decoder.column_len(),
            Decoder::Binary(ref decoder) => decoder.column_len(),
            Decoder::Jsonb(ref decoder) => decoder.column_len(),
        }
    }

    pub(crate) fn finish(&mut self, column_len: usize) -> Result<ArrayRef, ErrorKind> {
        match *self {
            Decoder::Boolean(ref mut decoder) => decoder.finish(column_len),
            Decoder::Int16(ref mut decoder) => decoder.finish(column_len),
            Decoder::Int32(ref mut decoder) => decoder.finish(column_len),
            Decoder::Int64(ref mut decoder) => decoder.finish(column_len),
            Decoder::Float32(ref mut decoder) => decoder.finish(column_len),
            Decoder::Float64(ref mut decoder) => decoder.finish(column_len),
            Decoder::Decimal(ref mut decoder) => decoder.finish(column_len),
            Decoder::TimestampMicrosecond(ref mut decoder) => decoder.finish(column_len),
            Decoder::Date32(ref mut decoder) => decoder.finish(column_len),
            Decoder::Time64Microsecond(ref mut decoder) => decoder.finish(column_len),
            Decoder::DurationMicrosecond(ref mut decoder) => decoder.finish(column_len),
            Decoder::String(ref mut decoder) => decoder.finish(column_len),
            Decoder::Binary(ref mut decoder) => decoder.finish(column_len),
            Decoder::Jsonb(ref mut decoder) => decoder.finish(column_len),
        }
    }
}
