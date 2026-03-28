use std::sync::Arc;

use arrow::array::Array;
use arrow::array::StringBuilder;
use pyo3::prelude::*;
use pyo3::Python;

use arrow::array::{make_array, ArrayData, LargeStringBuilder};
use arrow::datatypes::{Field, Schema};
use arrow::json::LineDelimitedWriter;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use serde_json::{from_str, Value};

fn array_to_json_values(array: Arc<dyn Array>) -> Vec<Value> {
    let field = Field::new("v", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let s = std::str::from_utf8(&buf).unwrap();
    s.lines()
        .map(|line| {
            let obj: Value = from_str(line).unwrap();
            match obj {
                Value::Object(mut map) => map.remove("v").unwrap_or(Value::Null),
                _ => Value::Null,
            }
        })
        .collect()
}

#[pyfunction]
#[pyo3(signature = (array, large = true))]
fn array_to_utf8_json_array(
    py: Python,
    array: &Bound<'_, PyAny>,
    large: bool,
) -> PyResult<PyObject> {
    // This is super inefficient, leaving optimization as a TODO
    let array = make_array(ArrayData::from_pyarrow_bound(array)?);
    let json = array_to_json_values(array);
    if large {
        let mut builder = LargeStringBuilder::new();
        for value in json.into_iter() {
            match value {
                Value::Null => builder.append_null(),
                value => builder.append_value(serde_json::to_string(&value).unwrap()),
            }
        }
        let json_arr = builder.finish();
        json_arr.into_data().to_pyarrow(py)
    } else {
        let mut builder = StringBuilder::new();
        for value in json.into_iter() {
            match value {
                Value::Null => builder.append_null(),
                value => builder.append_value(serde_json::to_string(&value).unwrap()),
            }
        }
        let json_arr = builder.finish();
        json_arr.into_data().to_pyarrow(py)
    }
}

#[pymodule]
fn _arrow_json(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(array_to_utf8_json_array, m)?)?;
    Ok(())
}
