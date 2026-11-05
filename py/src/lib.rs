use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use encoders::EncoderBuilder;
use pyo3::create_exception;
use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyDict};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use pgpq::error::ErrorKind;

mod encoders;
mod pg_schema;
mod utils;

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
struct ArrowToPostgresBinaryEncoder {
    encoder: pgpq::ArrowToPostgresBinaryEncoder,
    buf: BytesMut,
}

const BUFF_SIZE: usize = 1024 * 1024;

#[pymethods]
impl ArrowToPostgresBinaryEncoder {
    #[new]
    fn new(_py: Python, pyschema: &Bound<'_, PyAny>) -> PyResult<Self> {
        let encoder = pgpq::ArrowToPostgresBinaryEncoder::try_new(
            &ArrowSchema::from_pyarrow_bound(pyschema).unwrap(),
        )
        .unwrap();
        Ok(Self {
            encoder,
            buf: BytesMut::with_capacity(BUFF_SIZE),
        })
    }
    #[staticmethod]
    fn infer_encoder(py: Python, py_field: &Bound<'_, PyAny>) -> PyResult<EncoderBuilder> {
        EncoderBuilder::try_new(py, py_field)
    }
    #[staticmethod]
    fn new_with_encoders(
        _py: Python,
        py_schema: &Bound<'_, PyAny>,
        py_encoders: &Bound<'_, PyDict>,
    ) -> PyResult<Self> {
        let mut encoders: HashMap<String, pgpq::encoders::EncoderBuilder> = HashMap::new();
        for (key, value) in py_encoders.iter() {
            let name: String = key.extract()?;
            let py_builder: crate::encoders::EncoderBuilder = value.extract()?;
            let encoder: pgpq::encoders::EncoderBuilder = py_builder.into();
            encoders.insert(name, encoder);
        }
        let schema = &ArrowSchema::from_pyarrow_bound(py_schema).unwrap();
        let encoder =
            pgpq::ArrowToPostgresBinaryEncoder::try_new_with_encoders(schema, &encoders).unwrap();
        Ok(Self {
            encoder,
            buf: BytesMut::with_capacity(BUFF_SIZE),
        })
    }
    fn write_header(&mut self, py: Python) -> PyObject {
        self.encoder.write_header(&mut self.buf);
        PyBytes::new(py, &self.buf.split()[..]).into_any().unbind()
    }
    fn write_batch(&mut self, py: Python, py_batch: &Bound<'_, PyAny>) -> PyObject {
        let batch = &RecordBatch::from_pyarrow_bound(py_batch).unwrap();
        self.encoder.write_batch(batch, &mut self.buf).unwrap();

        if self.buf.len() > BUFF_SIZE {
            PyBytes::new(py, &self.buf.split()[..]).into_any().unbind()
        } else {
            PyBytes::new(py, &[][..]).into_any().unbind()
        }
    }
    fn finish(&mut self, py: Python) -> PyObject {
        self.encoder.write_footer(&mut self.buf).unwrap();
        PyBytes::new(py, &self.buf[..]).into_any().unbind()
    }
    fn schema(&self) -> crate::pg_schema::PostgresSchema {
        self.encoder.schema().into()
    }
}

create_exception!(_pgpq, DecodeError, PyValueError);

/// Converts a decoding error to a Python exception. IO errors become OSError,
/// and everything else becomes DecodeError.
fn decode_error_to_py(error: ErrorKind) -> PyErr {
    match error {
        ErrorKind::IOError { io_error } => PyOSError::new_err(io_error.to_string()),
        other => DecodeError::new_err(other.to_string()),
    }
}

/// Reads bytes and bytearray directly. A memoryview, which psycopg's COPY
/// yields, is copied once with tobytes, since the buffer protocol is not part of
/// the stable ABI before Python 3.11.
fn extract_bytes(data: &Bound<'_, PyAny>) -> PyResult<PyBackedBytes> {
    match data.extract::<PyBackedBytes>() {
        Ok(bytes) => Ok(bytes),
        Err(_) if data.hasattr("tobytes")? => data.call_method0("tobytes")?.extract(),
        Err(e) => Err(e),
    }
}

fn batches_to_pyarrow(py: Python, batches: Vec<RecordBatch>) -> PyResult<Vec<PyObject>> {
    batches.iter().map(|batch| batch.to_pyarrow(py)).collect()
}

/// Decodes a binary COPY stream fed to it in pieces of any size.
#[pyclass(module = "pgpq._pgpq")]
struct PostgresBinaryToArrowReader {
    decoder: pgpq::PostgresBinaryToArrowDecoder,
}

#[pymethods]
impl PostgresBinaryToArrowReader {
    #[new]
    #[pyo3(signature = (schema, batch_size = pgpq::DEFAULT_BATCH_SIZE))]
    fn new(schema: crate::pg_schema::PostgresSchema, batch_size: usize) -> PyResult<Self> {
        let decoder = pgpq::PostgresBinaryToArrowDecoder::new(schema.into(), batch_size)
            .map_err(decode_error_to_py)?;
        Ok(Self { decoder })
    }

    /// Adds bytes from the stream and returns the record batches they complete.
    ///
    /// data is bytes, bytearray or memoryview. bytes is read in place, and the
    /// GIL is released while the data is decoded.
    fn write(&mut self, py: Python, data: &Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let bytes = extract_bytes(data)?;
        let decoder = &mut self.decoder;
        let batches = py
            .allow_threads(|| decoder.write(&bytes))
            .map_err(decode_error_to_py)?;
        batches_to_pyarrow(py, batches)
    }

    /// Confirms the stream ended with its trailer. Raises DecodeError if it did not.
    fn finish(&mut self) -> PyResult<()> {
        self.decoder.finish().map_err(decode_error_to_py)
    }

    /// The pyarrow schema of the batches this decoder produces.
    #[getter]
    fn arrow_schema(&self, py: Python) -> PyResult<PyObject> {
        self.decoder.arrow_schema().as_ref().to_pyarrow(py)
    }
}

/// Decodes a binary COPY file, yielding one record batch at a time.
#[pyclass(module = "pgpq._pgpq")]
struct PostgresBinaryFileDecoder {
    decoder: pgpq::PostgresBinaryToArrowReader<BufReader<File>>,
}

/// Bytes read from the file per call to read.
const FILE_READ_SIZE: usize = 1024 * 1024;

#[pymethods]
impl PostgresBinaryFileDecoder {
    #[new]
    #[pyo3(signature = (path, schema, batch_size = pgpq::DEFAULT_BATCH_SIZE))]
    fn new(
        path: PathBuf,
        schema: crate::pg_schema::PostgresSchema,
        batch_size: usize,
    ) -> PyResult<Self> {
        let file = File::open(&path)
            .map_err(|e| PyOSError::new_err(format!("{}: {e}", path.display())))?;
        let reader = BufReader::with_capacity(FILE_READ_SIZE, file);
        let decoder = pgpq::PostgresBinaryToArrowReader::new(schema.into(), reader, batch_size)
            .map_err(decode_error_to_py)?;
        Ok(Self { decoder })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        let decoder = &mut self.decoder;
        match py.allow_threads(|| decoder.next()) {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch.to_pyarrow(py)?)),
            Some(Err(e)) => Err(decode_error_to_py(e)),
        }
    }

    /// The pyarrow schema of the batches this decoder produces.
    #[getter]
    fn arrow_schema(&self, py: Python) -> PyResult<PyObject> {
        self.decoder.arrow_schema().as_ref().to_pyarrow(py)
    }
}

#[pymodule]
fn _pgpq(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrowToPostgresBinaryEncoder>()?;
    m.add_class::<PostgresBinaryToArrowReader>()?;
    m.add_class::<PostgresBinaryFileDecoder>()?;
    m.add("DecodeError", py.get_type::<DecodeError>())?;
    m.add("DEFAULT_BATCH_SIZE", pgpq::DEFAULT_BATCH_SIZE)?;
    m.add_class::<crate::encoders::Int8EncoderBuilder>()?;
    m.add_class::<crate::encoders::ListEncoderBuilder>()?;
    m.add_class::<crate::pg_schema::Char>()?;
    m.add_class::<crate::pg_schema::Int2>()?;

    m.add_class::<crate::encoders::BooleanEncoderBuilder>()?;
    m.add_class::<crate::encoders::UInt8EncoderBuilder>()?;
    m.add_class::<crate::encoders::UInt16EncoderBuilder>()?;
    m.add_class::<crate::encoders::UInt32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int8EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int16EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int64EncoderBuilder>()?;
    m.add_class::<crate::encoders::Float16EncoderBuilder>()?;
    m.add_class::<crate::encoders::Float32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Float64EncoderBuilder>()?;
    m.add_class::<crate::encoders::TimestampMicrosecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::TimestampMillisecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::TimestampSecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::Date32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Time32MillisecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::Time32SecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::Time64MicrosecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::DurationMicrosecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::DurationMillisecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::DurationSecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::StringEncoderBuilder>()?;
    m.add_class::<crate::encoders::LargeStringEncoderBuilder>()?;
    m.add_class::<crate::encoders::BinaryEncoderBuilder>()?;
    m.add_class::<crate::encoders::LargeBinaryEncoderBuilder>()?;
    m.add_class::<crate::encoders::ListEncoderBuilder>()?;
    m.add_class::<crate::encoders::LargeListEncoderBuilder>()?;

    m.add_class::<crate::pg_schema::Bool>()?;
    m.add_class::<crate::pg_schema::Bytea>()?;
    m.add_class::<crate::pg_schema::Int8>()?;
    m.add_class::<crate::pg_schema::Int2>()?;
    m.add_class::<crate::pg_schema::Int4>()?;
    m.add_class::<crate::pg_schema::Char>()?;
    m.add_class::<crate::pg_schema::Text>()?;
    m.add_class::<crate::pg_schema::Json>()?;
    m.add_class::<crate::pg_schema::Jsonb>()?;
    m.add_class::<crate::pg_schema::Numeric>()?;
    m.add_class::<crate::pg_schema::Float4>()?;
    m.add_class::<crate::pg_schema::Float8>()?;
    m.add_class::<crate::pg_schema::Date>()?;
    m.add_class::<crate::pg_schema::Time>()?;
    m.add_class::<crate::pg_schema::Timestamp>()?;
    m.add_class::<crate::pg_schema::TimestampTz>()?;
    m.add_class::<crate::pg_schema::Interval>()?;
    m.add_class::<crate::pg_schema::List>()?;
    m.add_class::<crate::pg_schema::Column>()?;
    m.add_class::<crate::pg_schema::PostgresSchema>()?;
    Ok(())
}
