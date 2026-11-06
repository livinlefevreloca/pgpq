use std::collections::{HashMap, VecDeque};
use std::io::BufRead;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema, SchemaRef};
use bytes::{Buf, BufMut, BytesMut};
use error::ErrorKind;

mod buffer_view;
pub mod decoders;
pub mod encoders;
pub mod error;
mod numeric;
pub mod pg_schema;

use crate::buffer_view::BufferView;
use crate::decoders::{create_decoders, PostgresDecoder};
use crate::encoders::{BuildEncoder, Encode, EncoderBuilder};
use crate::pg_schema::PostgresSchema;

/// The signature that opens every binary COPY stream.
const HEADER_MAGIC_BYTES: &[u8; HEADER_MAGIC_LEN] = b"PGCOPY\n\xff\r\n\0";
const HEADER_MAGIC_LEN: usize = 11;

/// The flags field follows the signature, and the header extension length
/// follows the flags. Both are 32-bit integers.
const HEADER_EXTENSION_LEN_OFFSET: usize = HEADER_MAGIC_LEN + 4;
const HEADER_FIXED_LEN: usize = HEADER_EXTENSION_LEN_OFFSET + 4;

/// Each row starts with a 16-bit field count.
const FIELD_COUNT_LEN: usize = std::mem::size_of::<i16>();

/// Each field starts with a 32-bit length.
const FIELD_LEN_LEN: usize = std::mem::size_of::<i32>();

/// A field count of -1 marks the end of the stream.
const TRAILER_FIELD_COUNT: i16 = -1;

/// A field length of -1 marks a null value.
const NULL_FIELD_LEN: i32 = -1;

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
        out.put_slice(HEADER_MAGIC_BYTES);
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
        out.put_i16(TRAILER_FIELD_COUNT);
        self.state = EncoderState::Finished;
        Ok(())
    }
}

/// Rows per record batch when the caller does not choose a size.
pub const DEFAULT_BATCH_SIZE: usize = 65_536;

/// Header flag bit 16 means every row carries an OID field. Only servers before
/// Postgres 12 write it, for tables created WITH OIDS.
const HEADER_OIDS_FLAG: u32 = 1 << 16;

#[derive(Debug, Clone, Copy, PartialEq)]
enum StreamState {
    Header,
    Rows,
    Finished,
    Failed,
}

/// The extent of the next row in a buffer.
enum RowExtent {
    /// The buffer ends before the row does.
    Incomplete,
    /// The next thing in the buffer is the trailer.
    Trailer,
    /// A complete row of this many bytes, field count included.
    Complete(usize),
}

/// Decodes a binary COPY stream fed to it in pieces of any size.
///
/// A row is decoded only once all of its bytes have arrived. Every field in the
/// format carries its length, so the decoder finds where a row ends before
/// decoding it, and a column decoder never sees part of a row.
pub struct PostgresBinaryToArrowDecoder {
    schema: PostgresSchema,
    decoders: Vec<PostgresDecoder>,
    batch_size: usize,
    rows_in_batch: usize,
    pending: BytesMut,
    state: StreamState,
}

impl PostgresBinaryToArrowDecoder {
    /// Creates a decoder that emits a batch every batch_size rows.
    pub fn new(schema: PostgresSchema, batch_size: usize) -> Result<Self, ErrorKind> {
        if batch_size == 0 {
            return Err(ErrorKind::InvalidBatchSize);
        }
        let decoders = create_decoders(&schema);
        Ok(Self {
            schema,
            decoders,
            batch_size,
            rows_in_batch: 0,
            pending: BytesMut::new(),
            state: StreamState::Header,
        })
    }

    /// Creates a decoder from an Arrow schema. Fails if a field's type has no
    /// Postgres decoder.
    pub fn try_new_with_arrow_schema(schema: Schema, batch_size: usize) -> Result<Self, ErrorKind> {
        Self::new(PostgresSchema::try_from(schema)?, batch_size)
    }

    /// The Arrow schema of the batches this decoder produces.
    pub fn arrow_schema(&self) -> SchemaRef {
        self.schema.clone().into()
    }

    /// Whether the header has been read.
    pub fn header_read(&self) -> bool {
        self.state != StreamState::Header
    }

    /// Whether the trailer has been read.
    pub fn is_finished(&self) -> bool {
        self.state == StreamState::Finished
    }

    /// Adds bytes from the stream and returns every batch they complete.
    ///
    /// The trailer flushes the last, partly filled batch. An error leaves the
    /// decoder failed, and every later call returns DecoderFailed.
    pub fn write(&mut self, data: &[u8]) -> Result<Vec<RecordBatch>, ErrorKind> {
        match self.state {
            // The stream already failed and its column decoders may hold part of a row.
            StreamState::Failed => return Err(ErrorKind::DecoderFailed),

            // Nothing may follow the trailer.
            StreamState::Finished if !data.is_empty() => return Err(ErrorKind::DataAfterTrailer),

            StreamState::Finished => return Ok(vec![]),
            StreamState::Header | StreamState::Rows => {}
        }

        self.pending.extend_from_slice(data);
        let result = self.decode_pending();
        if result.is_err() {
            self.state = StreamState::Failed;
        }
        result
    }

    /// Confirms the stream ended with its trailer.
    ///
    /// Call it once the source is exhausted. A stream cut off before the trailer
    /// is an error, so a truncated COPY is never mistaken for a complete one.
    pub fn finish(&mut self) -> Result<(), ErrorKind> {
        match self.state {
            StreamState::Finished => Ok(()),
            StreamState::Failed => Err(ErrorKind::DecoderFailed),
            StreamState::Header | StreamState::Rows => {
                self.state = StreamState::Failed;
                if self.pending.is_empty() {
                    Err(ErrorKind::MissingTrailer)
                } else {
                    Err(ErrorKind::IncompleteDecode {
                        remaining_bytes: self.pending.to_vec(),
                    })
                }
            }
        }
    }

    fn decode_pending(&mut self) -> Result<Vec<RecordBatch>, ErrorKind> {
        // The buffer is moved out so its rows can be decoded while the column
        // decoders are borrowed mutably.
        let pending = std::mem::take(&mut self.pending);
        let mut batches = vec![];
        let mut pos = 0;

        if self.state == StreamState::Header {
            match header_len(&pending)? {
                None => {
                    self.pending = pending;
                    return Ok(batches);
                }
                Some(len) => {
                    pos = len;
                    self.state = StreamState::Rows;
                }
            }
        }

        while self.state == StreamState::Rows {
            match row_extent(&pending[pos..], self.decoders.len())? {
                RowExtent::Incomplete => break,
                RowExtent::Trailer => {
                    pos += FIELD_COUNT_LEN;

                    // Postgres ends the stream at the trailer.
                    if pos != pending.len() {
                        return Err(ErrorKind::DataAfterTrailer);
                    }

                    if self.rows_in_batch > 0 {
                        batches.push(self.finish_batch()?);
                    }
                    self.state = StreamState::Finished;
                }
                RowExtent::Complete(len) => {
                    // The field count was checked by row_extent.
                    self.decode_row(&pending[pos + FIELD_COUNT_LEN..pos + len])?;
                    pos += len;
                    self.rows_in_batch += 1;
                    if self.rows_in_batch == self.batch_size {
                        batches.push(self.finish_batch()?);
                    }
                }
            }
        }

        self.pending = pending;
        self.pending.advance(pos);
        Ok(batches)
    }

    /// Decodes one complete row, given its fields without the field count.
    fn decode_row(&mut self, fields: &[u8]) -> Result<(), ErrorKind> {
        let mut view = BufferView::new(fields);
        for decoder in self.decoders.iter_mut() {
            decoder.decode(&mut view).map_err(|e| match e {
                // The row is complete, so a decoder that runs short read a field
                // whose length does not fit the column's type.
                ErrorKind::IncompleteData => ErrorKind::MalformedRow {
                    reason: format!(
                        "the field for column {} does not match its type",
                        decoder.name()
                    ),
                },
                other => other,
            })?;
        }

        // Each decoder reads its field's length prefix, so a decoder that read
        // more or less than its field leaves the row misaligned.
        if view.remaining() != 0 {
            return Err(ErrorKind::MalformedRow {
                reason: format!(
                    "{} bytes were left over after decoding every column",
                    view.remaining()
                ),
            });
        }
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, ErrorKind> {
        let column_len = self.rows_in_batch;
        self.rows_in_batch = 0;

        let columns = self
            .decoders
            .iter_mut()
            .map(|decoder| decoder.finish(column_len))
            .collect();

        Ok(RecordBatch::try_new(self.schema.clone().into(), columns)?)
    }
}

/// Returns the header's length once all of it is in buf, or None if more bytes
/// are needed.
fn header_len(buf: &[u8]) -> Result<Option<usize>, ErrorKind> {
    if buf.len() < HEADER_MAGIC_LEN {
        return Ok(None);
    }

    if &buf[..HEADER_MAGIC_LEN] != HEADER_MAGIC_BYTES {
        let bytes: [u8; HEADER_MAGIC_LEN] = buf[..HEADER_MAGIC_LEN].try_into().unwrap();
        return Err(ErrorKind::InvalidBinaryHeader { bytes });
    }

    if buf.len() < HEADER_FIXED_LEN {
        return Ok(None);
    }

    let flags = u32::from_be_bytes(
        buf[HEADER_MAGIC_LEN..HEADER_EXTENSION_LEN_OFFSET]
            .try_into()
            .unwrap(),
    );
    if flags & HEADER_OIDS_FLAG != 0 {
        return Err(ErrorKind::UnsupportedHeaderFlags { flags });
    }

    // The extension area is reserved for future use, and decoders skip it.
    let extension_len = u32::from_be_bytes(
        buf[HEADER_EXTENSION_LEN_OFFSET..HEADER_FIXED_LEN]
            .try_into()
            .unwrap(),
    ) as usize;
    let header_len = HEADER_FIXED_LEN + extension_len;
    if buf.len() < header_len {
        return Ok(None);
    }
    Ok(Some(header_len))
}

/// Finds where the row at the start of buf ends, reading only length prefixes.
fn row_extent(buf: &[u8], expected_fields: usize) -> Result<RowExtent, ErrorKind> {
    if buf.len() < FIELD_COUNT_LEN {
        return Ok(RowExtent::Incomplete);
    }

    let field_count = i16::from_be_bytes([buf[0], buf[1]]);
    if field_count == TRAILER_FIELD_COUNT {
        return Ok(RowExtent::Trailer);
    }

    if field_count < 0 || field_count as usize != expected_fields {
        return Err(ErrorKind::UnexpectedFieldCount {
            expected: expected_fields,
            actual: field_count,
        });
    }

    let mut pos = FIELD_COUNT_LEN;
    for _ in 0..field_count {
        if buf.len() < pos + FIELD_LEN_LEN {
            return Ok(RowExtent::Incomplete);
        }
        let field_len = i32::from_be_bytes(buf[pos..pos + FIELD_LEN_LEN].try_into().unwrap());
        pos += FIELD_LEN_LEN;

        if field_len == NULL_FIELD_LEN {
            continue;
        }

        if field_len < 0 {
            return Err(ErrorKind::MalformedRow {
                reason: format!("negative field length {field_len}"),
            });
        }

        pos += field_len as usize;
        if buf.len() < pos {
            return Ok(RowExtent::Incomplete);
        }
    }
    Ok(RowExtent::Complete(pos))
}

/// Decodes a binary COPY stream read from source, one batch at a time.
///
/// Iterating yields each batch as soon as it fills, so memory holds about one
/// batch at a time. decode_batches reads the whole stream at once.
pub struct PostgresBinaryToArrowReader<R> {
    source: R,
    inner: PostgresBinaryToArrowDecoder,
    ready: VecDeque<RecordBatch>,
    done: bool,
}

impl<R: BufRead> PostgresBinaryToArrowReader<R> {
    /// Creates a decoder that emits a batch every batch_size rows.
    pub fn new(schema: PostgresSchema, source: R, batch_size: usize) -> Result<Self, ErrorKind> {
        Ok(Self::from_decoder(
            PostgresBinaryToArrowDecoder::new(schema, batch_size)?,
            source,
        ))
    }

    /// Creates a decoder from an Arrow schema. Fails if a field's type has no
    /// Postgres decoder.
    pub fn try_new_with_arrow_schema(
        schema: Schema,
        source: R,
        batch_size: usize,
    ) -> Result<Self, ErrorKind> {
        Ok(Self::from_decoder(
            PostgresBinaryToArrowDecoder::try_new_with_arrow_schema(schema, batch_size)?,
            source,
        ))
    }

    fn from_decoder(inner: PostgresBinaryToArrowDecoder, source: R) -> Self {
        Self {
            source,
            inner,
            ready: VecDeque::new(),
            done: false,
        }
    }

    /// The Arrow schema of the batches this decoder produces.
    pub fn arrow_schema(&self) -> SchemaRef {
        self.inner.arrow_schema()
    }

    /// Reads and validates the header.
    ///
    /// Iterating reads the header itself, so calling this first is optional. It
    /// reports a bad header before any rows are requested.
    pub fn read_header(&mut self) -> Result<(), ErrorKind> {
        while !self.inner.header_read() {
            if !self.read_more()? {
                break;
            }
        }
        Ok(())
    }

    /// Reads the rest of the stream and returns every batch in it.
    pub fn decode_batches(&mut self) -> Result<Vec<RecordBatch>, ErrorKind> {
        self.collect()
    }

    /// Feeds the next piece of the source to the decoder. Returns false
    /// once the source is exhausted.
    fn read_more(&mut self) -> Result<bool, ErrorKind> {
        let data = self.source.fill_buf()?;
        if data.is_empty() {
            self.done = true;
            self.inner.finish()?;
            return Ok(false);
        }

        let read = data.len();
        let batches = self.inner.write(data)?;
        self.source.consume(read);
        self.ready.extend(batches);
        Ok(true)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ErrorKind> {
        loop {
            if let Some(batch) = self.ready.pop_front() {
                return Ok(Some(batch));
            }

            if self.done {
                return Ok(None);
            }

            if let Err(e) = self.read_more() {
                self.done = true;
                return Err(e);
            }
        }
    }
}

impl<R: BufRead> Iterator for PostgresBinaryToArrowReader<R> {
    type Item = Result<RecordBatch, ErrorKind>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
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
