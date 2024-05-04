use std::collections::HashMap;
use std::io::{BufRead, Seek};

use arrow_array::RecordBatch;
use arrow_schema::Fields;
use arrow_schema::Schema;
use bytes::{Buf, BufMut, BytesMut};
use error::ErrorKind;

pub mod decoders;
pub mod encoders;
pub mod error;
pub mod pg_schema;
mod buffer_view;

use crate::decoders::PostgresDecoder;
use crate::buffer_view::BufferView;
use crate::encoders::{BuildEncoder, Encode, EncoderBuilder};
use crate::pg_schema::PostgresSchema;

const HEADER_MAGIC_BYTES: &[u8] = b"PGCOPY\n\xff\r\n\0";

#[derive(Debug, PartialEq)]
enum EncoderState {
    Created,
    Encoding,
    Finished,
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Fields,
    state: EncoderState,
    encoder_builders: Vec<EncoderBuilder>,
}

pub fn build_encoders(
    fields: &arrow_schema::Fields,
) -> Vec<(String, Result<EncoderBuilder, ErrorKind>)> {
    fields
        .iter()
        .map(|f| (f.name().clone(), EncoderBuilder::try_new(f.clone())))
        .collect()
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn try_new(schema: &Schema) -> Result<Self, ErrorKind> {
        let fields = schema.fields();

        let maybe_encoder_builders: Result<Vec<EncoderBuilder>, ErrorKind> = build_encoders(fields)
            .into_iter()
            .map(|(_, maybe_encoder)| maybe_encoder)
            .collect();

        Ok(ArrowToPostgresBinaryEncoder {
            fields: fields.clone(),
            state: EncoderState::Created,
            encoder_builders: maybe_encoder_builders?,
        })
    }

    pub fn try_new_with_encoders(
        schema: &Schema,
        encoders: &HashMap<String, EncoderBuilder>,
    ) -> Result<Self, ErrorKind> {
        let mut encoders = encoders.clone();
        let maybe_encoder_builders: Result<Vec<EncoderBuilder>, ErrorKind> = schema
            .fields()
            .iter()
            .map(|f| {
                encoders.remove(f.name()).map_or_else(
                    || {
                        Err(ErrorKind::EncoderMissing {
                            field: f.name().to_string(),
                        })
                    },
                    Ok,
                )
            })
            .collect();
        if !encoders.is_empty() {
            return Err(ErrorKind::UnknownFields {
                fields: encoders.keys().cloned().collect(),
            });
        }
        Ok(ArrowToPostgresBinaryEncoder {
            fields: schema.fields.clone(),
            state: EncoderState::Created,
            encoder_builders: maybe_encoder_builders?,
        })
    }

    pub fn schema(&self) -> PostgresSchema {
        PostgresSchema {
            columns: self
                .encoder_builders
                .iter()
                .zip(&self.fields)
                .map(|(builder, field)| (field.name().clone(), builder.schema()))
                .collect(),
        }
    }

    pub fn write_header(&mut self, out: &mut BytesMut) {
        assert_eq!(self.state, EncoderState::Created);
        out.put(HEADER_MAGIC_BYTES);
        out.put_i32(0); // flags
        out.put_i32(0); // header extension
        self.state = EncoderState::Encoding;
    }

    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        buf: &mut BytesMut,
    ) -> Result<(), ErrorKind> {
        assert_eq!(self.state, EncoderState::Encoding);
        assert!(
            batch.num_columns() == self.fields.len(),
            "expected {} values but got {}",
            self.fields.len(),
            batch.num_columns(),
        );
        let n_rows = batch.num_rows();
        let n_cols = batch.num_columns();

        let encoders = batch
            .columns()
            .iter()
            .zip(&self.encoder_builders)
            .map(|(col, builder)| builder.try_new(col))
            .collect::<Result<Vec<_>, _>>()?;

        let mut required_size: usize = 0;
        for encoder in &encoders {
            required_size += encoder.size_hint()?
        }
        buf.reserve(required_size);

        for row in 0..n_rows {
            buf.put_i16(n_cols as i16);
            for encoder in &encoders {
                encoder.encode(row, buf)?
            }
        }
        Ok(())
    }

    pub fn write_footer(&mut self, out: &mut BytesMut) -> Result<(), ErrorKind> {
        assert_eq!(self.state, EncoderState::Encoding);
        out.put_i16(-1);
        self.state = EncoderState::Finished;
        Ok(())
    }
}

enum BatchDecodeResult {
    Batch(RecordBatch),
    Incomplete(usize),
    Error(ErrorKind),
    PartialConsume { batch: RecordBatch, consumed: usize },
}

pub struct PostgresBinaryToArrowDecoder<R> {
    schema: PostgresSchema,
    decoders: Vec<PostgresDecoder>,
    source: R,
    state: EncoderState,
    capacity: usize,
}

impl<R: BufRead> PostgresBinaryToArrowDecoder<R> {
    pub fn new(schema: PostgresSchema, source: R, capacity: usize) -> Result<Self, ErrorKind> {
        let decoders = PostgresDecoder::new(&schema);
        Ok(PostgresBinaryToArrowDecoder {
            schema,
            decoders,
            source,
            state: EncoderState::Created,
            capacity,
        })
    }

    /// Try to create a new decoder using an arrow schema. Will fail if the types in the schema
    /// are not supported by the decoder.
    pub fn try_new_with_arrow_schema(
        schema: Schema,
        source: R,
        capacity: usize,
    ) -> Result<Self, ErrorKind> {
        let pg_schema = PostgresSchema::try_from(schema)?;
        let decoders = PostgresDecoder::new(&pg_schema);
        Ok(PostgresBinaryToArrowDecoder {
            decoders,
            schema: pg_schema,
            source,
            state: EncoderState::Created,
            capacity,
        })
    }

    /// Reads the header from the source and validates it.
    pub fn read_header(&mut self) -> Result<(), ErrorKind> {
        assert_eq!(self.state, EncoderState::Created);

        // Header is always 11 bytes long. b"PGCOPY\n\xff\r\n\0"
        let mut header = [0; 11];
        self.source.read_exact(&mut header)?;
        if header != HEADER_MAGIC_BYTES {
            return Err(ErrorKind::InvalidBinaryHeader { bytes: header });
        }

        // read flags and header extension both of which we ignore the values of.
        let mut flags = [0; 4];
        self.source.read_exact(&mut flags)?;

        let mut header_extension = [0; 4];
        self.source.read_exact(&mut header_extension)?;

        self.state = EncoderState::Encoding;
        Ok(())
    }

    /// read batches of bytes from the source and decode them into RecordBatches.
    pub fn decode_batches(&mut self) -> Result<Vec<RecordBatch>, ErrorKind> {
        let mut batches = Vec::new();
        let mut buf = BytesMut::with_capacity(self.capacity);
        let mut eof = false;
        while self.state == EncoderState::Encoding {
            // Read loop. Read from source until buf is full.
            let mut data = self.source.fill_buf()?;
            loop {
                // If there is no data left in the source, break the loop and finish the batch.
                // set the eof flag to true to indicate that the source has been fully read.
                if data.is_empty() {
                    eof = true;
                    break;
                // if data was read from the source, put it into the buffer and single
                // to source that we have consumed the data by calling BufRead::consume.
                } else {
                    buf.put(data);
                    let read = data.len();
                    self.source.consume(read);
                }
                data = self.source.fill_buf()?;

                // If the remaining capacity of the buffer is less than the length of the data,
                // break the loop.
                let remaining = buf.capacity() - buf.len();
                if remaining < data.len() {
                    break;
                }
            }

            // If the eof flag is not set and there remains capacity in the buffer, read the
            // ${remaining_capacity} bytes from the source and put them into the buffer.
            let remaining = buf.capacity() - buf.len();
            if !eof && remaining > 0 {
                let read = std::cmp::min(data.len(), buf.remaining());
                buf.put(&data[..read]);
                self.source.consume(read);
            }

            // If we have reached the end of the source decode the batch and return error if
            // there is any remaining data in the buffer indicated by and IncompleteDecode
            // or PartialConsume BatchDecodeResult.
            if eof {
                if !buf.is_empty() {
                    match self.decode_batch(&mut buf) {
                        BatchDecodeResult::Batch(batch) => batches.push(batch),
                        BatchDecodeResult::Error(e) => return Err(e),
                        BatchDecodeResult::Incomplete(consumed)
                        | BatchDecodeResult::PartialConsume { batch: _, consumed } => {
                            return Err(ErrorKind::IncompleteDecode {
                                remaining_bytes: buf[consumed..].to_vec(),
                            })
                        }
                    }
                }
                self.state = EncoderState::Finished;
            // If we have not reached the end of the source, decode the batch.
            } else {
                match self.decode_batch(&mut buf) {
                    BatchDecodeResult::Batch(batch) => {
                        batches.push(batch);
                        buf.clear()
                    }
                    // If we receive a PartialConsume BatchDecodeResult, store the batches we did
                    // manage to decode and continue reading from the source with the remaining
                    // data from the previous read in the buffer.
                    BatchDecodeResult::PartialConsume { batch, consumed } => {
                        batches.push(batch);
                        let old_buf = buf;
                        buf = BytesMut::with_capacity(self.capacity);
                        buf.put(&old_buf[consumed..]);
                    }
                    // If we receive an Incomplete BatchDecodeResult, increase the capacity of the
                    // buffer reading more data from the source and try to decode the batch again.
                    BatchDecodeResult::Incomplete(_) => {
                        // increase the capacity attribute of the decoder by a factor of 2.
                        buf.reserve(self.capacity);
                        self.capacity *= 2;
                    }
                    BatchDecodeResult::Error(e) => return Err(e),
                }
            }
        }
        Ok(batches)
    }

    /// Decode a single batch of bytes from the buffer. This method is called by decode_batches
    /// and has several different completion states each represented by a BatchDecodeResult.
    fn decode_batch(&mut self, buf: &mut BytesMut) -> BatchDecodeResult {
        // ensure that the decoder is in the correct state before proceeding.
        assert_eq!(self.state, EncoderState::Encoding);

        // create a new BufferView from the buffer.
        let mut local_buf = BufferView::new(buf);
        // Keep track of the number of rows in the batch.
        let mut rows = 0;

        // Each iteration of the loop reads a tuple from the data.
        // If we are not able to read a tuple, return a BatchDecodeResult::Incomplete.
        // If we were able to read some tuples, but not all the data in the buffer was consumed,
        // return a BatchDecodeResult::PartialConsume.
        while local_buf.remaining() > 0 {
            // Store the number of bytes consumed before reading the tuple.
            let consumed = local_buf.consumed();

            // Read the number of columns in the tuple. This number is
            // stored as a 16-bit integer. This value is the same for all
            // tuples in the batch.
            let tuple_len: u16 = match local_buf.consume_into_u16() {
                Ok(len) => len,
                Err(e) => {
                    return BatchDecodeResult::Error(e);
                }
            };

            // If the tuple length is 0xffff we have reached the end of the
            // snapshot and we can break the loop and finish the batch.
            if tuple_len == 0xffff {
                break;
            }

            // Each iteration of the loop reads a column from the tuple using the
            // decoder specfied via the schema.
            for decoder in self.decoders.iter_mut() {
                // If local_buf has been fully consumed and we have not read any rows,
                // return a BatchDecodeResult::Incomplete.
                if local_buf.remaining() == 0 && rows == 0 {
                    return BatchDecodeResult::Incomplete(local_buf.consumed());
                // If local_buf has been fully consumed and we have read some rows,
                // return a BatchDecodeResult::PartialConsume, passing the number of bytes
                // consumed before reading the tuple to the caller so it can know how much data
                // was consumed.
                } else if local_buf.remaining() == 0 {
                    return match self.finish_batch() {
                        Ok(batch) => BatchDecodeResult::PartialConsume { batch, consumed },
                        Err(e) => BatchDecodeResult::Error(e),
                    };
                }
                // Apply the decoder to the local_buf. Cosume the data from the buffer as needed
                match decoder.apply(&mut local_buf) {
                    // If the decoder was able to decode the data, continue to the next column.
                    Ok(_) => {}
                    // If we receive a IncompleteData error, we have reached the end of the data in
                    // the buffer. If we have decoded some tuples, return a BatchDecodeResult::PartialConsume,
                    // otherwise return a BatchDecodeResult::Incomplete.
                    Err(ErrorKind::IncompleteData) => {
                        // If we have not read any rows, return a BatchDecodeResult::Incomplete.
                        if rows == 0 {
                            return BatchDecodeResult::Incomplete(local_buf.consumed());
                        } else {
                            // If we have read some rows, return a BatchDecodeResult::PartialConsume,
                            return match self.finish_batch() {
                                Ok(batch) => BatchDecodeResult::PartialConsume { batch, consumed },
                                Err(e) => BatchDecodeResult::Error(e),
                            };
                        }
                    }
                    Err(e) => return BatchDecodeResult::Error(e),
                }
            }
            // Increment the number of rows in the batch.
            rows += 1;
        }

        match self.finish_batch() {
            Ok(batch) => BatchDecodeResult::Batch(batch),
            Err(e) => BatchDecodeResult::Error(e),
        }
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, ErrorKind> {
        // Find the mininum length column in the decoders. These can be different if
        // we are in a partial consume state. We will truncate the columns to the length
        // of the shortest column and pick up the lost data in the next batch.
        let column_len = self.decoders.iter().map(|d| d.column_len()).min().unwrap();

        // Determine which columns in the batch are fully null so that we can alter the schema
        // to reflect this.
        let null_columns = self
            .decoders
            .iter()
            .filter(|decoder| decoder.is_null())
            .map(|decoder| decoder.name())
            .collect::<Vec<String>>();

        // For each decoder call its finish method to coerce the data into an Arrow array.
        // and append the array to the columns vector.
        let columns = self
            .decoders
            .iter_mut()
            .map(|decoder| decoder.finish(column_len))
            .collect();

        Ok(RecordBatch::try_new(self.schema.clone().nullify_columns(&null_columns).into(), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{encoders::StringEncoderBuilder, pg_schema::Column};

    use super::*;
    use arrow_array::{Int32Array, Int8Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn make_test_data() -> RecordBatch {
        let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let int8_array = Int8Array::from(vec![1, 2, 3, 4, 5]);
        let string_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let json_array = StringArray::from(vec!["\"a\"", "[]", "{\"f\":123}", "1", "{}"]);

        let schema = Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("json", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(int32_array),
                Arc::new(int8_array),
                Arc::new(string_array),
                Arc::new(json_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_build_with_encoders() {
        let batch = make_test_data();
        let encoders = build_encoders(batch.schema().fields());
        let encoders: HashMap<String, EncoderBuilder> = encoders
            .into_iter()
            .map(|(field_name, maybe_enc)| match field_name.as_str() {
                "json" => (
                    field_name.to_string(),
                    EncoderBuilder::String(
                        StringEncoderBuilder::new_with_output(
                            Arc::new(batch.schema().field_with_name("json").unwrap().clone()),
                            pg_schema::PostgresType::Jsonb,
                        )
                        .unwrap(),
                    ),
                ),
                field_name => (field_name.to_string(), maybe_enc.unwrap()),
            })
            .collect();
        let encoder = ArrowToPostgresBinaryEncoder::try_new_with_encoders(
            &batch.schema(),
            &encoders.into_iter().collect(),
        )
        .unwrap();
        let schema = encoder.schema();
        assert_eq!(
            schema.columns,
            vec![
                (
                    "int32".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Int4,
                        nullable: false,
                    }
                ),
                (
                    "int8".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Int2,
                        nullable: false,
                    },
                ),
                (
                    "string".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Text,
                        nullable: false,
                    },
                ),
                (
                    "json".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Jsonb,
                        nullable: false,
                    },
                ),
            ]
        )
    }
}
