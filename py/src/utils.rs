use pyo3::prelude::*;
use pyo3::Python;

pub(crate) trait PythonRepr {
    fn py_repr(&self, py: Python) -> String;
}

pub(crate) fn call_repr(py: Python, obj: &Py<PyAny>) -> String {
    let builtins = py.import("builtins").unwrap();
    let repr = builtins.getattr("repr").unwrap();
    repr.call1((obj,)).unwrap().extract::<String>().unwrap()
}
