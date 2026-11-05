//! Decodes Postgres binary COPY output into Arrow record batches.
//!
//! PostgresBinaryToArrowDecoder takes bytes as they arrive and returns each
//! batch as soon as it fills. PostgresBinaryToArrowReader reads from any
//! BufRead source and yields the same batches as an iterator.
//!
//! A row is decoded only once all of its bytes have arrived. Every field in the
//! format carries its length, so the decoder finds where a row ends before
//! decoding it, and a column decoder never sees part of a row.

use std::collections::VecDeque;
use std::io::BufRead;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use bytes::{Buf, BytesMut};

use crate::buffer_view::BufferView;
use crate::decoders::{create_decoders, PostgresDecoder};
use crate::error::ErrorKind;
use crate::pg_schema::PostgresSchema;
use crate::HEADER_MAGIC_BYTES;

/// Rows per record batch when the caller does not choose a size.
pub const DEFAULT_BATCH_SIZE: usize = 65_536;

/// The signature, the flags field and the header extension length.
const HEADER_FIXED_LEN: usize = 19;

/// Header flag bit 16 means every row carries an OID field. Only servers before
/// Postgres 12 write it, for tables created WITH OIDS.
const HEADER_OIDS_FLAG: u32 = 1 << 16;

/// A field count of -1 marks the end of the stream.
const TRAILER_FIELD_COUNT: i16 = -1;

/// A field length of -1 marks a null value.
const NULL_FIELD_LEN: i32 = -1;

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
                    pos += 2;

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
                    // The first two bytes are the field count, already checked.
                    self.decode_row(&pending[pos + 2..pos + len])?;
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

        // An all-null column becomes a NullArray, so its field's type in this
        // batch's schema must be Null as well.
        let null_columns = self
            .decoders
            .iter()
            .filter(|decoder| decoder.is_null())
            .map(|decoder| decoder.name())
            .collect::<Vec<String>>();

        let columns = self
            .decoders
            .iter_mut()
            .map(|decoder| decoder.finish(column_len))
            .collect();

        Ok(RecordBatch::try_new(
            self.schema.clone().nullify_columns(&null_columns).into(),
            columns,
        )?)
    }
}

/// Returns the header's length once all of it is in buf, or None if more bytes
/// are needed.
fn header_len(buf: &[u8]) -> Result<Option<usize>, ErrorKind> {
    if buf.len() < HEADER_MAGIC_BYTES.len() {
        return Ok(None);
    }

    if &buf[..HEADER_MAGIC_BYTES.len()] != HEADER_MAGIC_BYTES {
        let mut bytes = [0; 11];
        bytes.copy_from_slice(&buf[..HEADER_MAGIC_BYTES.len()]);
        return Err(ErrorKind::InvalidBinaryHeader { bytes });
    }

    if buf.len() < HEADER_FIXED_LEN {
        return Ok(None);
    }

    let flags = u32::from_be_bytes(buf[11..15].try_into().unwrap());
    if flags & HEADER_OIDS_FLAG != 0 {
        return Err(ErrorKind::UnsupportedHeaderFlags { flags });
    }

    // The extension area is reserved for future use, and decoders skip it.
    let extension_len = u32::from_be_bytes(buf[15..19].try_into().unwrap()) as usize;
    let header_len = HEADER_FIXED_LEN + extension_len;
    if buf.len() < header_len {
        return Ok(None);
    }
    Ok(Some(header_len))
}

/// Finds where the row at the start of buf ends, reading only length prefixes.
fn row_extent(buf: &[u8], expected_fields: usize) -> Result<RowExtent, ErrorKind> {
    if buf.len() < 2 {
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

    let mut pos = 2;
    for _ in 0..field_count {
        if buf.len() < pos + 4 {
            return Ok(RowExtent::Incomplete);
        }
        let field_len = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

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
