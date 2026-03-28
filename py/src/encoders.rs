use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::Field;
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyNotImplemented, PyType};
use pyo3::{exceptions::PyValueError, prelude::*};

use pgpq::encoders::BuildEncoder;

use crate::pg_schema::PostgresType;
use crate::utils::{call_repr, PythonRepr};

macro_rules! impl_passthrough_encoder_builder {
    ($py_class:ident) => {
        #[pymethods]
        impl $py_class {
            #[new]
            fn new(_py: Python, py_field: &Bound<'_, PyAny>) -> PyResult<Self> {
                let field = Field::from_pyarrow_bound(py_field)?;
                let inner = match pgpq::encoders::EncoderBuilder::try_new(Arc::new(field)) {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            stringify!($py_class),
                            e
                        )));
                    }
                };
                Ok(Self {
                    field: py_field.clone().unbind(),
                    inner,
                })
            }
            fn __repr__(&self, py: Python) -> String {
                crate::utils::PythonRepr::py_repr(self, py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
            fn __richcmp__(
                &self,
                other: &Self,
                op: CompareOp,
                py: Python<'_>,
            ) -> PyResult<PyObject> {
                let res = match op {
                    CompareOp::Eq => (self.inner == other.inner)
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    CompareOp::Ne => (self.inner != other.inner)
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    _ => PyNotImplemented::get(py).to_owned().into_any().unbind(),
                };
                Ok(res)
            }
        }
        impl PythonRepr for $py_class {
            fn py_repr(&self, py: Python) -> String {
                format!(
                    "{}({})",
                    stringify!($py_class),
                    call_repr(py, &self.field),
                )
            }
        }
    };
}

macro_rules! impl_passthrough_encoder_builder_variable_output {
    ($py_class:ident, $pgpq_encoder_builder:ty, $pgpq_encoder_builder_enum_variant:path) => {
        #[pymethods]
        impl $py_class {
            #[new]
            fn new(_py: Python, py_field: &Bound<'_, PyAny>) -> PyResult<Self> {
                let field = Field::from_pyarrow_bound(py_field)?;
                let inner = match <$pgpq_encoder_builder>::new(Arc::new(field)) {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            stringify!($py_class),
                            e
                        )));
                    }
                };
                let py_output: crate::pg_schema::PostgresType = inner.schema().data_type.into();
                Ok(Self {
                    field: py_field.clone().unbind(),
                    output: py_output,
                    inner: $pgpq_encoder_builder_enum_variant(inner),
                })
            }
            #[classmethod]
            fn new_with_output(
                cls: &Bound<'_, PyType>,
                _py: Python,
                py_field: &Bound<'_, PyAny>,
                py_output: PostgresType,
            ) -> PyResult<Self> {
                let field = Field::from_pyarrow_bound(py_field)?;
                let output = pgpq::pg_schema::PostgresType::from(py_output.clone());
                let inner = match <$pgpq_encoder_builder>::new_with_output(Arc::new(field), output)
                {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            cls.name()?,
                            e
                        )));
                    }
                };
                Ok(Self {
                    field: py_field.clone().unbind(),
                    output: py_output,
                    inner: $pgpq_encoder_builder_enum_variant(inner),
                })
            }
            fn __repr__(&self, py: Python) -> String {
                crate::utils::PythonRepr::py_repr(self, py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
            fn __richcmp__<'py>(
                &self,
                other: &Self,
                op: CompareOp,
                py: Python<'py>,
            ) -> PyResult<PyObject> {
                let res = match op {
                    CompareOp::Eq => (self.inner == other.inner)
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    CompareOp::Ne => (self.inner != other.inner)
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    _ => PyNotImplemented::get(py).to_owned().into_any().unbind(),
                };
                Ok(res)
            }
        }
        impl crate::utils::PythonRepr for $py_class {
            fn py_repr(&self, py: Python) -> String {
                format!(
                    "{}({}, {})",
                    stringify!($py_class),
                    crate::utils::call_repr(py, &self.field),
                    self.output.py_repr(py)
                )
            }
        }
    };
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct BooleanEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(BooleanEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct UInt8EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(UInt8EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct UInt16EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(UInt16EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct UInt32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(UInt32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Int8EncoderBuilder {
    field: Py<PyAny>,
    output: PostgresType,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder_variable_output!(
    Int8EncoderBuilder,
    pgpq::encoders::Int8EncoderBuilder,
    pgpq::encoders::EncoderBuilder::Int8
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Int16EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Int16EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Int32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Int32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Int64EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Int64EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Float16EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Float16EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Float32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Float32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Float64EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Float64EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct TimestampMicrosecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(TimestampMicrosecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct TimestampMillisecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(TimestampMillisecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct TimestampSecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(TimestampSecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Date32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Date32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Time32MillisecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Time32MillisecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Time32SecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Time32SecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct Time64MicrosecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Time64MicrosecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct DurationMicrosecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(DurationMicrosecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct DurationMillisecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(DurationMillisecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct DurationSecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(DurationSecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct StringEncoderBuilder {
    field: Py<PyAny>,
    output: crate::pg_schema::PostgresType,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder_variable_output!(
    StringEncoderBuilder,
    pgpq::encoders::StringEncoderBuilder,
    pgpq::encoders::EncoderBuilder::String
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct LargeStringEncoderBuilder {
    field: Py<PyAny>,
    output: crate::pg_schema::PostgresType,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder_variable_output!(
    LargeStringEncoderBuilder,
    pgpq::encoders::LargeStringEncoderBuilder,
    pgpq::encoders::EncoderBuilder::LargeString
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct BinaryEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(BinaryEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct LargeBinaryEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(LargeBinaryEncoderBuilder);

macro_rules! impl_list {
    ($struct:ident, $encoder_builder_enum_variant:path, $encoder_builder_new_with_inner:expr) => {
        #[pymethods]
        impl $struct {
            #[new]
            fn new(_py: Python, py_field: Bound<'_, PyAny>) -> PyResult<Self> {
                let field = Field::from_pyarrow_bound(&py_field)?;
                let inner = match pgpq::encoders::EncoderBuilder::try_new(Arc::new(field)) {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            stringify!($struct),
                            e
                        )));
                    }
                };
                Ok(Self {
                    field: py_field.unbind(),
                    inner,
                })
            }
            #[classmethod]
            fn new_with_inner(
                _cls: &Bound<'_, PyType>,
                _py: Python,
                py_field: Bound<'_, PyAny>,
                py_inner_encoder_builder: EncoderBuilder,
            ) -> PyResult<Self> {
                let field = Field::from_pyarrow_bound(&py_field)?;
                let inner_encoder_builder: pgpq::encoders::EncoderBuilder =
                    py_inner_encoder_builder.into();
                Ok(Self {
                    field: py_field.unbind(),
                    inner: $encoder_builder_enum_variant(
                        $encoder_builder_new_with_inner(Arc::new(field), inner_encoder_builder)
                            .unwrap(),
                    ),
                })
            }
            fn __repr__(&self, py: Python) -> String {
                crate::utils::PythonRepr::py_repr(self, py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
            fn __richcmp__(
                &self,
                other: &Self,
                op: CompareOp,
                py: Python<'_>,
            ) -> PyResult<PyObject> {
                let res = match op {
                    CompareOp::Eq => (self.inner == other.inner)
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    CompareOp::Ne => (self.inner != other.inner)
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    _ => PyNotImplemented::get(py).to_owned().into_any().unbind(),
                };
                Ok(res)
            }
        }
        impl crate::utils::PythonRepr for $struct {
            fn py_repr(&self, py: Python) -> String {
                let inner_encoder_builder = match &self.inner {
                    pgpq::encoders::EncoderBuilder::List(inner) => {
                        EncoderBuilder::from(inner.inner_encoder_builder())
                    }
                    _ => unreachable!(),
                };
                format!(
                    "{}({}, {})",
                    "ListEncoderBuilder",
                    call_repr(py, &self.field),
                    inner_encoder_builder.py_repr(py),
                )
            }
        }
    };
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct ListEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_list!(
    ListEncoderBuilder,
    pgpq::encoders::EncoderBuilder::List,
    pgpq::encoders::ListEncoderBuilder::new_with_inner
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
pub struct LargeListEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_list!(
    LargeListEncoderBuilder,
    pgpq::encoders::EncoderBuilder::LargeList,
    pgpq::encoders::LargeListEncoderBuilder::new_with_inner
);

#[derive(Debug)]
pub enum EncoderBuilder {
    Boolean(BooleanEncoderBuilder),
    UInt8(UInt8EncoderBuilder),
    UInt16(UInt16EncoderBuilder),
    UInt32(UInt32EncoderBuilder),
    Int8(Int8EncoderBuilder),
    Int16(Int16EncoderBuilder),
    Int32(Int32EncoderBuilder),
    Int64(Int64EncoderBuilder),
    Float16(Float16EncoderBuilder),
    Float32(Float32EncoderBuilder),
    Float64(Float64EncoderBuilder),
    TimestampMicrosecond(TimestampMicrosecondEncoderBuilder),
    TimestampMillisecond(TimestampMillisecondEncoderBuilder),
    TimestampSecond(TimestampSecondEncoderBuilder),
    Date32(Date32EncoderBuilder),
    Time32Millisecond(Time32MillisecondEncoderBuilder),
    Time32Second(Time32SecondEncoderBuilder),
    Time64Microsecond(Time64MicrosecondEncoderBuilder),
    DurationMicrosecond(DurationMicrosecondEncoderBuilder),
    DurationMillisecond(DurationMillisecondEncoderBuilder),
    DurationSecond(DurationSecondEncoderBuilder),
    String(StringEncoderBuilder),
    LargeString(LargeStringEncoderBuilder),
    Binary(BinaryEncoderBuilder),
    LargeBinary(LargeBinaryEncoderBuilder),
    List(ListEncoderBuilder),
    LargeList(LargeListEncoderBuilder),
}

macro_rules! try_extract_encoder {
    ($ob:expr, $variant:ident, $ty:ident) => {
        if let Ok(r) = $ob.downcast::<$ty>() {
            let inner_ref = r.borrow();
            return Ok(EncoderBuilder::$variant($ty {
                field: inner_ref.field.clone_ref($ob.py()),
                inner: inner_ref.inner.clone(),
            }));
        }
    };
    ($ob:expr, $variant:ident, $ty:ident, with_output) => {
        if let Ok(r) = $ob.downcast::<$ty>() {
            let inner_ref = r.borrow();
            return Ok(EncoderBuilder::$variant($ty {
                field: inner_ref.field.clone_ref($ob.py()),
                output: inner_ref.output.clone(),
                inner: inner_ref.inner.clone(),
            }));
        }
    };
}

impl<'py> FromPyObject<'py> for EncoderBuilder {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        try_extract_encoder!(ob, Boolean, BooleanEncoderBuilder);
        try_extract_encoder!(ob, UInt8, UInt8EncoderBuilder);
        try_extract_encoder!(ob, UInt16, UInt16EncoderBuilder);
        try_extract_encoder!(ob, UInt32, UInt32EncoderBuilder);
        try_extract_encoder!(ob, Int8, Int8EncoderBuilder, with_output);
        try_extract_encoder!(ob, Int16, Int16EncoderBuilder);
        try_extract_encoder!(ob, Int32, Int32EncoderBuilder);
        try_extract_encoder!(ob, Int64, Int64EncoderBuilder);
        try_extract_encoder!(ob, Float16, Float16EncoderBuilder);
        try_extract_encoder!(ob, Float32, Float32EncoderBuilder);
        try_extract_encoder!(ob, Float64, Float64EncoderBuilder);
        try_extract_encoder!(ob, TimestampMicrosecond, TimestampMicrosecondEncoderBuilder);
        try_extract_encoder!(ob, TimestampMillisecond, TimestampMillisecondEncoderBuilder);
        try_extract_encoder!(ob, TimestampSecond, TimestampSecondEncoderBuilder);
        try_extract_encoder!(ob, Date32, Date32EncoderBuilder);
        try_extract_encoder!(ob, Time32Millisecond, Time32MillisecondEncoderBuilder);
        try_extract_encoder!(ob, Time32Second, Time32SecondEncoderBuilder);
        try_extract_encoder!(ob, Time64Microsecond, Time64MicrosecondEncoderBuilder);
        try_extract_encoder!(ob, DurationMicrosecond, DurationMicrosecondEncoderBuilder);
        try_extract_encoder!(ob, DurationMillisecond, DurationMillisecondEncoderBuilder);
        try_extract_encoder!(ob, DurationSecond, DurationSecondEncoderBuilder);
        try_extract_encoder!(ob, String, StringEncoderBuilder, with_output);
        try_extract_encoder!(ob, LargeString, LargeStringEncoderBuilder, with_output);
        try_extract_encoder!(ob, Binary, BinaryEncoderBuilder);
        try_extract_encoder!(ob, LargeBinary, LargeBinaryEncoderBuilder);
        try_extract_encoder!(ob, List, ListEncoderBuilder);
        try_extract_encoder!(ob, LargeList, LargeListEncoderBuilder);
        Err(PyValueError::new_err("Unknown encoder builder type"))
    }
}

impl crate::utils::PythonRepr for EncoderBuilder {
    fn py_repr(&self, py: Python) -> String {
        match self {
            EncoderBuilder::Boolean(inner) => inner.py_repr(py),
            EncoderBuilder::UInt8(inner) => inner.py_repr(py),
            EncoderBuilder::UInt16(inner) => inner.py_repr(py),
            EncoderBuilder::UInt32(inner) => inner.py_repr(py),
            EncoderBuilder::Int8(inner) => inner.py_repr(py),
            EncoderBuilder::Int16(inner) => inner.py_repr(py),
            EncoderBuilder::Int32(inner) => inner.py_repr(py),
            EncoderBuilder::Int64(inner) => inner.py_repr(py),
            EncoderBuilder::Float16(inner) => inner.py_repr(py),
            EncoderBuilder::Float32(inner) => inner.py_repr(py),
            EncoderBuilder::Float64(inner) => inner.py_repr(py),
            EncoderBuilder::TimestampMicrosecond(inner) => inner.py_repr(py),
            EncoderBuilder::TimestampMillisecond(inner) => inner.py_repr(py),
            EncoderBuilder::TimestampSecond(inner) => inner.py_repr(py),
            EncoderBuilder::Date32(inner) => inner.py_repr(py),
            EncoderBuilder::Time32Millisecond(inner) => inner.py_repr(py),
            EncoderBuilder::Time32Second(inner) => inner.py_repr(py),
            EncoderBuilder::Time64Microsecond(inner) => inner.py_repr(py),
            EncoderBuilder::DurationMicrosecond(inner) => inner.py_repr(py),
            EncoderBuilder::DurationMillisecond(inner) => inner.py_repr(py),
            EncoderBuilder::DurationSecond(inner) => inner.py_repr(py),
            EncoderBuilder::String(inner) => inner.py_repr(py),
            EncoderBuilder::LargeString(inner) => inner.py_repr(py),
            EncoderBuilder::Binary(inner) => inner.py_repr(py),
            EncoderBuilder::LargeBinary(inner) => inner.py_repr(py),
            EncoderBuilder::List(inner) => inner.py_repr(py),
            EncoderBuilder::LargeList(inner) => inner.py_repr(py),
        }
    }
}

impl EncoderBuilder {
    pub fn try_new(_py: Python, py_field: &Bound<'_, PyAny>) -> PyResult<Self> {
        let field = Field::from_pyarrow_bound(py_field)?;
        let inner = match pgpq::encoders::EncoderBuilder::try_new(Arc::new(field)) {
            Ok(inner) => inner,
            Err(_e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Unable to infer encoder for {:?}",
                    py_field.repr().unwrap()
                )))
            }
        };
        let pg_output_type: crate::pg_schema::PostgresType = inner.schema().data_type.into();
        let field_obj = py_field.clone().unbind();
        let wrapped = match inner {
            pgpq::encoders::EncoderBuilder::Boolean(_) => {
                EncoderBuilder::Boolean(BooleanEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt8(_) => {
                EncoderBuilder::UInt8(UInt8EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt16(_) => {
                EncoderBuilder::UInt16(UInt16EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt32(_) => {
                EncoderBuilder::UInt32(UInt32EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Int8(_) => EncoderBuilder::Int8(Int8EncoderBuilder {
                field: field_obj,
                output: pg_output_type,
                inner,
            }),
            pgpq::encoders::EncoderBuilder::Int16(_) => {
                EncoderBuilder::Int16(Int16EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Int32(_) => {
                EncoderBuilder::Int32(Int32EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Int64(_) => {
                EncoderBuilder::Int64(Int64EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Float16(_) => {
                EncoderBuilder::Float16(Float16EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Float32(_) => {
                EncoderBuilder::Float32(Float32EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Float64(_) => {
                EncoderBuilder::Float64(Float64EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampMicrosecond(_) => {
                EncoderBuilder::TimestampMicrosecond(TimestampMicrosecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampMillisecond(_) => {
                EncoderBuilder::TimestampMillisecond(TimestampMillisecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampSecond(_) => {
                EncoderBuilder::TimestampSecond(TimestampSecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Date32(_) => {
                EncoderBuilder::Date32(Date32EncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Time32Millisecond(_) => {
                EncoderBuilder::Time32Millisecond(Time32MillisecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Time32Second(_) => {
                EncoderBuilder::Time32Second(Time32SecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Time64Microsecond(_) => {
                EncoderBuilder::Time64Microsecond(Time64MicrosecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationMicrosecond(_) => {
                EncoderBuilder::DurationMicrosecond(DurationMicrosecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationMillisecond(_) => {
                EncoderBuilder::DurationMillisecond(DurationMillisecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationSecond(_) => {
                EncoderBuilder::DurationSecond(DurationSecondEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::String(_) => {
                EncoderBuilder::String(StringEncoderBuilder {
                    field: field_obj,
                    output: pg_output_type,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeString(_) => {
                EncoderBuilder::LargeString(LargeStringEncoderBuilder {
                    field: field_obj,
                    output: pg_output_type,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Binary(_) => {
                EncoderBuilder::Binary(BinaryEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeBinary(_) => {
                EncoderBuilder::LargeBinary(LargeBinaryEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::List(_) => EncoderBuilder::List(ListEncoderBuilder {
                field: field_obj,
                inner,
            }),
            pgpq::encoders::EncoderBuilder::LargeList(_) => {
                EncoderBuilder::LargeList(LargeListEncoderBuilder {
                    field: field_obj,
                    inner,
                })
            }
        };
        Ok(wrapped)
    }
}

macro_rules! convert_encoder_builder {
    ($py:expr, $inner:expr, $value:expr, $variant:ident, $builder:ident) => {{
        let field = $inner.field().to_pyarrow($py).unwrap();
        EncoderBuilder::$variant($builder {
            field,
            inner: $value,
        })
    }};
    ($py:expr, $inner:expr, $value:expr, $variant:ident, $builder:ident, with_output) => {{
        let field = $inner.field().to_pyarrow($py).unwrap();
        let output: crate::pg_schema::PostgresType = $inner.schema().data_type.into();
        EncoderBuilder::$variant($builder {
            field,
            inner: $value,
            output,
        })
    }};
}

impl From<pgpq::encoders::EncoderBuilder> for EncoderBuilder {
    fn from(value: pgpq::encoders::EncoderBuilder) -> Self {
        Python::with_gil(|py| match &value {
            pgpq::encoders::EncoderBuilder::Boolean(inner) => convert_encoder_builder!(py, inner, value, Boolean, BooleanEncoderBuilder),
            pgpq::encoders::EncoderBuilder::UInt8(inner) => convert_encoder_builder!(py, inner, value, UInt8, UInt8EncoderBuilder),
            pgpq::encoders::EncoderBuilder::UInt16(inner) => convert_encoder_builder!(py, inner, value, UInt16, UInt16EncoderBuilder),
            pgpq::encoders::EncoderBuilder::UInt32(inner) => convert_encoder_builder!(py, inner, value, UInt32, UInt32EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Int8(inner) => convert_encoder_builder!(py, inner, value, Int8, Int8EncoderBuilder, with_output),
            pgpq::encoders::EncoderBuilder::Int16(inner) => convert_encoder_builder!(py, inner, value, Int16, Int16EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Int32(inner) => convert_encoder_builder!(py, inner, value, Int32, Int32EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Int64(inner) => convert_encoder_builder!(py, inner, value, Int64, Int64EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Float16(inner) => convert_encoder_builder!(py, inner, value, Float16, Float16EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Float32(inner) => convert_encoder_builder!(py, inner, value, Float32, Float32EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Float64(inner) => convert_encoder_builder!(py, inner, value, Float64, Float64EncoderBuilder),
            pgpq::encoders::EncoderBuilder::TimestampMicrosecond(inner) => convert_encoder_builder!(py, inner, value, TimestampMicrosecond, TimestampMicrosecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::TimestampMillisecond(inner) => convert_encoder_builder!(py, inner, value, TimestampMillisecond, TimestampMillisecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::TimestampSecond(inner) => convert_encoder_builder!(py, inner, value, TimestampSecond, TimestampSecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::Date32(inner) => convert_encoder_builder!(py, inner, value, Date32, Date32EncoderBuilder),
            pgpq::encoders::EncoderBuilder::Time32Millisecond(inner) => convert_encoder_builder!(py, inner, value, Time32Millisecond, Time32MillisecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::Time32Second(inner) => convert_encoder_builder!(py, inner, value, Time32Second, Time32SecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::Time64Microsecond(inner) => convert_encoder_builder!(py, inner, value, Time64Microsecond, Time64MicrosecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::DurationMicrosecond(inner) => convert_encoder_builder!(py, inner, value, DurationMicrosecond, DurationMicrosecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::DurationMillisecond(inner) => convert_encoder_builder!(py, inner, value, DurationMillisecond, DurationMillisecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::DurationSecond(inner) => convert_encoder_builder!(py, inner, value, DurationSecond, DurationSecondEncoderBuilder),
            pgpq::encoders::EncoderBuilder::String(inner) => convert_encoder_builder!(py, inner, value, String, StringEncoderBuilder, with_output),
            pgpq::encoders::EncoderBuilder::LargeString(inner) => convert_encoder_builder!(py, inner, value, LargeString, LargeStringEncoderBuilder, with_output),
            pgpq::encoders::EncoderBuilder::Binary(inner) => convert_encoder_builder!(py, inner, value, Binary, BinaryEncoderBuilder),
            pgpq::encoders::EncoderBuilder::LargeBinary(inner) => convert_encoder_builder!(py, inner, value, LargeBinary, LargeBinaryEncoderBuilder),
            pgpq::encoders::EncoderBuilder::List(inner) => convert_encoder_builder!(py, inner, value, List, ListEncoderBuilder),
            pgpq::encoders::EncoderBuilder::LargeList(inner) => convert_encoder_builder!(py, inner, value, LargeList, LargeListEncoderBuilder),
        })
    }
}

impl From<EncoderBuilder> for pgpq::encoders::EncoderBuilder {
    fn from(val: EncoderBuilder) -> Self {
        match val {
            EncoderBuilder::Boolean(inner) => inner.inner,
            EncoderBuilder::UInt8(inner) => inner.inner,
            EncoderBuilder::UInt16(inner) => inner.inner,
            EncoderBuilder::UInt32(inner) => inner.inner,
            EncoderBuilder::Int8(inner) => inner.inner,
            EncoderBuilder::Int16(inner) => inner.inner,
            EncoderBuilder::Int32(inner) => inner.inner,
            EncoderBuilder::Int64(inner) => inner.inner,
            EncoderBuilder::Float16(inner) => inner.inner,
            EncoderBuilder::Float32(inner) => inner.inner,
            EncoderBuilder::Float64(inner) => inner.inner,
            EncoderBuilder::TimestampMicrosecond(inner) => inner.inner,
            EncoderBuilder::TimestampMillisecond(inner) => inner.inner,
            EncoderBuilder::TimestampSecond(inner) => inner.inner,
            EncoderBuilder::Date32(inner) => inner.inner,
            EncoderBuilder::Time32Millisecond(inner) => inner.inner,
            EncoderBuilder::Time32Second(inner) => inner.inner,
            EncoderBuilder::Time64Microsecond(inner) => inner.inner,
            EncoderBuilder::DurationMicrosecond(inner) => inner.inner,
            EncoderBuilder::DurationMillisecond(inner) => inner.inner,
            EncoderBuilder::DurationSecond(inner) => inner.inner,
            EncoderBuilder::String(inner) => inner.inner,
            EncoderBuilder::LargeString(inner) => inner.inner,
            EncoderBuilder::Binary(inner) => inner.inner,
            EncoderBuilder::LargeBinary(inner) => inner.inner,
            EncoderBuilder::List(inner) => inner.inner,
            EncoderBuilder::LargeList(inner) => inner.inner,
        }
    }
}

impl<'py> IntoPyObject<'py> for EncoderBuilder {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            EncoderBuilder::Boolean(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::UInt8(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::UInt16(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::UInt32(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Int8(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Int16(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Int32(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Int64(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Float16(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Float32(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Float64(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::TimestampMicrosecond(inner) => {
                Ok(inner.into_pyobject(py)?.into_any())
            }
            EncoderBuilder::TimestampMillisecond(inner) => {
                Ok(inner.into_pyobject(py)?.into_any())
            }
            EncoderBuilder::TimestampSecond(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Date32(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Time32Millisecond(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Time32Second(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Time64Microsecond(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::DurationMicrosecond(inner) => {
                Ok(inner.into_pyobject(py)?.into_any())
            }
            EncoderBuilder::DurationMillisecond(inner) => {
                Ok(inner.into_pyobject(py)?.into_any())
            }
            EncoderBuilder::DurationSecond(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::String(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::LargeString(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::Binary(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::LargeBinary(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::List(inner) => Ok(inner.into_pyobject(py)?.into_any()),
            EncoderBuilder::LargeList(inner) => Ok(inner.into_pyobject(py)?.into_any()),
        }
    }
}
