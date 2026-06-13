//! Arrow ↔ PyArrow zero-copy bridge via the Arrow PyCapsule Interface.
//!
//! Implements [Arrow C Data Interface PyCapsule]
//! ([protocol]) using arrow-array's [`FFI_ArrowArray`] / arrow-schema's
//! [`FFI_ArrowSchema`] and pyo3's [`PyCapsule`]. No third-party crate
//! ([`pyo3-arrow`]) is needed.
//!
//! # Why we don't use `pyo3-arrow`
//!
//! The published `pyo3-arrow` 0.15.x requires Python buffer-protocol
//! exports that pyo3 0.27 gates behind `Py_3_11` or `not(Py_LIMITED_API)`.
//! Our workspace pin is `pyo3 = "0.27"` with `abi3-py310`, so the
//! buffer-protocol exports are unavailable and `pyo3-arrow` fails to
//! compile. The Arrow PyCapsule Interface itself does not need the
//! buffer protocol — only [`pyo3::types::PyCapsule`] and the FFI
//! structs — so the bridge below is portable across pyo3 0.26–0.28 and
//! abi3-py310+.
//!
//! [protocol]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
//! [`pyo3-arrow`]: https://crates.io/crates/pyo3-arrow

#![cfg(feature = "pyo3")]

use std::ffi::CStr;

use arrow_array::ffi::{FFI_ArrowArray, from_ffi};
use arrow_array::{Array, ArrayRef, make_array};
use arrow_schema::DataType;
use arrow_schema::ffi::FFI_ArrowSchema;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyTuple};

use crate::error::PyPluginError;

const SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";

/// Producer-side: build a `(schema_capsule, array_capsule)` 2-tuple
/// PyTuple that pyarrow consumers (or any object exposing
/// `__arrow_c_array__`) can ingest.
///
/// The capsules transfer ownership of the FFI structs to Python; the
/// FFI structs internally hold `Arc<Buffer>` references that keep the
/// Arrow buffers alive across the boundary. When pyarrow drops the
/// capsules, the destructors release the references — at which point
/// the buffers can be freed.
///
/// # Errors
///
/// Returns [`PyPluginError::ArrowConversion`] if the schema cannot be
/// exported (e.g., unsupported logical type).
pub fn arrow_array_to_pyarrow_capsules<'py>(
    py: Python<'py>,
    arr: &dyn Array,
) -> Result<Bound<'py, PyTuple>, PyPluginError> {
    let data = arr.to_data();
    let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())
        .map_err(|e| PyPluginError::ArrowConversion(format!("schema export: {e}")))?;
    let ffi_array = FFI_ArrowArray::new(&data);

    let schema_cap = make_arrow_capsule(py, ffi_schema, SCHEMA_CAPSULE_NAME)?;
    let array_cap = make_arrow_capsule(py, ffi_array, ARRAY_CAPSULE_NAME)?;

    let tuple =
        PyTuple::new(py, [schema_cap.as_any(), array_cap.as_any()]).map_err(PyPluginError::from)?;
    Ok(tuple)
}

/// Build a `_PyArrowCapsuleHolder`-shaped object that pyarrow can
/// consume via `pa.array(obj)`.
///
/// pyarrow's `pa.array(obj)` looks for `obj.__arrow_c_array__()` and
/// expects a `(schema_capsule, array_capsule)` 2-tuple. We expose this
/// via a small Python lambda built at the Python level — no `pyclass`
/// needed, which keeps the abi3 surface minimal.
///
/// Returns a Python object on which `pa.array(...)` will succeed.
///
/// # Errors
///
/// Returns [`PyPluginError::ArrowConversion`] on capsule construction failure.
pub fn arrow_array_to_pyarrow<'py>(
    py: Python<'py>,
    arr: &dyn Array,
) -> Result<Bound<'py, PyAny>, PyPluginError> {
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};

    let capsules = arrow_array_to_pyarrow_capsules(py, arr)?;
    // Build a small adapter class via `type(name, bases, dict)` whose
    // `__arrow_c_array__` method returns the capsule tuple. The class
    // is fresh per call (cheap; the cost is dominated by the FFI struct
    // construction, not the class).
    let builtins = py.import("builtins").map_err(PyPluginError::from)?;
    let type_fn = builtins.getattr("type").map_err(PyPluginError::from)?;
    let object_cls = builtins.getattr("object").map_err(PyPluginError::from)?;

    let dict = PyDict::new(py);
    // Build a closure-bearing method via `py.eval`:
    //   lambda _t: (lambda self, requested_schema=None, _t=_t: _t)
    let make_method_code =
        std::ffi::CString::new("lambda _t: (lambda self, requested_schema=None, _t=_t: _t)")
            .map_err(|e| PyPluginError::ArrowConversion(format!("CString eval body: {e}")))?;
    let make_method = py
        .eval(make_method_code.as_c_str(), None, None)
        .map_err(PyPluginError::from)?;
    let method = make_method
        .call1((capsules,))
        .map_err(PyPluginError::from)?;
    dict.set_item("__arrow_c_array__", method)
        .map_err(PyPluginError::from)?;

    let bases = PyTuple::new(py, [object_cls]).map_err(PyPluginError::from)?;
    let cls = type_fn
        .call1(("_UniDbArrowCapsuleHolder", bases, dict))
        .map_err(PyPluginError::from)?;
    let instance = cls.call0().map_err(PyPluginError::from)?;

    // The capsule-protocol holder is *consumer-protocol*: any object
    // that calls `__arrow_c_array__` (including our own
    // `pyarrow_to_arrow_array`) accepts it. But pyarrow's compute
    // kernels and other typed APIs expect a `pyarrow.Array` instance.
    // If pyarrow is importable, wrap via `pa.array(holder)` so the
    // user sees a normal pyarrow Array. If pyarrow is not available
    // (an embedder running pure-Python plugins without pyarrow), fall
    // back to the holder — it still round-trips through
    // `pyarrow_to_arrow_array`.
    match py.import("pyarrow") {
        Ok(pa) => {
            let arr = pa
                .getattr("array")
                .map_err(PyPluginError::from)?
                .call1((instance,))
                .map_err(PyPluginError::from)?;
            Ok(arr)
        }
        Err(_) => Ok(instance),
    }
}

/// Consumer-side: pull a Python value that implements
/// `__arrow_c_array__` (or is a pyarrow Array directly) into an
/// [`ArrayRef`].
///
/// # Errors
///
/// Returns [`PyPluginError::ArrowConversion`] when the object does not
/// expose the protocol, returns an ill-shaped tuple, or the capsule
/// pointers are null / wrong-named.
pub fn pyarrow_to_arrow_array(
    _py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> Result<ArrayRef, PyPluginError> {
    use pyo3::types::{PyAnyMethods, PyTupleMethods};

    let method = obj.getattr("__arrow_c_array__").map_err(|e| {
        PyPluginError::ArrowConversion(format!("object lacks __arrow_c_array__: {e}"))
    })?;
    let result = method
        .call0()
        .map_err(|e| PyPluginError::ArrowConversion(format!("__arrow_c_array__ raised: {e}")))?;
    let tuple = result.cast::<PyTuple>().map_err(|_| {
        PyPluginError::ArrowConversion("__arrow_c_array__ did not return a tuple".into())
    })?;
    if tuple.len() != 2 {
        return Err(PyPluginError::ArrowConversion(format!(
            "__arrow_c_array__ returned tuple of length {}, expected 2",
            tuple.len()
        )));
    }

    let schema_obj = tuple.get_item(0).map_err(PyPluginError::from)?;
    let array_obj = tuple.get_item(1).map_err(PyPluginError::from)?;
    let schema_cap = schema_obj
        .cast::<PyCapsule>()
        .map_err(|_| {
            PyPluginError::ArrowConversion("__arrow_c_array__ element 0 is not a PyCapsule".into())
        })?
        .clone();
    let array_cap = array_obj
        .cast::<PyCapsule>()
        .map_err(|_| {
            PyPluginError::ArrowConversion("__arrow_c_array__ element 1 is not a PyCapsule".into())
        })?
        .clone();

    // SAFETY: the producer placed `Box<FFI_ArrowSchema>` and
    // `Box<FFI_ArrowArray>` in the capsules and named them per the
    // Arrow PyCapsule protocol. `pointer_checked` validates the
    // capsule name matches the expected literal before returning the
    // raw pointer. We then *move out* of the array capsule via
    // ptr::read + write-back of an empty struct so the producer's
    // destructor doesn't double-release.
    let schema_ptr = schema_cap
        .pointer_checked(Some(SCHEMA_CAPSULE_NAME))
        .map_err(|e| PyPluginError::ArrowConversion(format!("schema capsule pointer: {e}")))?
        .as_ptr() as *const FFI_ArrowSchema;
    let array_ptr = array_cap
        .pointer_checked(Some(ARRAY_CAPSULE_NAME))
        .map_err(|e| PyPluginError::ArrowConversion(format!("array capsule pointer: {e}")))?
        .as_ptr() as *mut FFI_ArrowArray;

    // Schema is read-only via from_ffi(array, &schema). Borrow it.
    // SAFETY: the capsule keeps the schema alive while this Bound is
    // alive; from_ffi copies what it needs.
    let schema_ref: &FFI_ArrowSchema = unsafe { &*schema_ptr };

    // Array: we move out of the capsule. The producer's destructor
    // will run when the capsule drops; we replace the underlying value
    // with `FFI_ArrowArray::empty()` (a no-op-release struct) so the
    // destructor doesn't double-release.
    let ffi_array = unsafe { std::ptr::read(array_ptr) };
    let empty = FFI_ArrowArray::empty();
    unsafe { std::ptr::write(array_ptr, empty) };

    let data = unsafe { from_ffi(ffi_array, schema_ref) }
        .map_err(|e| PyPluginError::ArrowConversion(format!("from_ffi: {e}")))?;
    Ok(make_array(data))
}

/// Convert an [`arrow_schema::DataType`] expectation into a strict
/// runtime check against the array PyArrow returned. The pyarrow side
/// can return a logically-equivalent-but-physically-different type
/// (e.g., `LargeUtf8` for `Utf8`); for v1 we accept exact match and
/// surface mismatches as `ArrowConversion`.
///
/// # Errors
///
/// Returns [`PyPluginError::ArrowConversion`] when the actual data
/// type differs from the expected one.
pub fn assert_array_datatype(arr: &dyn Array, expected: &DataType) -> Result<(), PyPluginError> {
    if arr.data_type() != expected {
        return Err(PyPluginError::ArrowConversion(format!(
            "pyarrow returned `{}`, expected `{}`",
            arr.data_type(),
            expected
        )));
    }
    Ok(())
}

fn make_arrow_capsule<'py, T>(
    py: Python<'py>,
    value: T,
    name: &'static CStr,
) -> Result<Bound<'py, PyCapsule>, PyPluginError>
where
    T: 'static + Send,
{
    PyCapsule::new_with_value(py, value, name).map_err(PyPluginError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};

    fn ensure_python() -> bool {
        Python::initialize();
        true
    }

    #[test]
    fn pyo3_arrow_roundtrip_float64() {
        if !ensure_python() {
            return;
        }
        Python::attach(|py| {
            let arr: ArrayRef =
                std::sync::Arc::new(Float64Array::from(vec![1.5_f64, 2.5, -3.0, 0.0]));
            let pa_obj = arrow_array_to_pyarrow(py, arr.as_ref()).expect("export to pyarrow");
            // Round-trip back. The wrapper object exposes __arrow_c_array__
            // directly, so we can re-import without going through pyarrow.
            let back = pyarrow_to_arrow_array(py, &pa_obj).expect("import back");
            assert_eq!(back.data_type(), &DataType::Float64);
            assert_eq!(back.len(), 4);
            let f = back
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64");
            assert!((f.value(0) - 1.5).abs() < 1e-12);
            assert!((f.value(2) - -3.0).abs() < 1e-12);
        });
    }

    #[test]
    fn pyo3_arrow_roundtrip_int64() {
        if !ensure_python() {
            return;
        }
        Python::attach(|py| {
            let arr: ArrayRef =
                std::sync::Arc::new(Int64Array::from(vec![1_i64, 2, 3, -7, 100_000_000]));
            let pa_obj = arrow_array_to_pyarrow(py, arr.as_ref()).expect("export");
            let back = pyarrow_to_arrow_array(py, &pa_obj).expect("import");
            assert_eq!(back.data_type(), &DataType::Int64);
            assert_eq!(back.len(), 5);
            let i = back.as_any().downcast_ref::<Int64Array>().expect("int64");
            assert_eq!(i.value(0), 1);
            assert_eq!(i.value(4), 100_000_000);
        });
    }

    #[test]
    fn pyo3_arrow_roundtrip_utf8() {
        if !ensure_python() {
            return;
        }
        Python::attach(|py| {
            let arr: ArrayRef =
                std::sync::Arc::new(StringArray::from(vec!["hello", "world", "ünïcödé"]));
            let pa_obj = arrow_array_to_pyarrow(py, arr.as_ref()).expect("export");
            let back = pyarrow_to_arrow_array(py, &pa_obj).expect("import");
            assert_eq!(back.data_type(), &DataType::Utf8);
            assert_eq!(back.len(), 3);
            let s = back.as_any().downcast_ref::<StringArray>().expect("utf8");
            assert_eq!(s.value(0), "hello");
            assert_eq!(s.value(2), "ünïcödé");
        });
    }

    #[test]
    fn pyo3_arrow_roundtrip_with_nulls() {
        if !ensure_python() {
            return;
        }
        Python::attach(|py| {
            let arr: ArrayRef =
                std::sync::Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0), None]));
            let pa_obj = arrow_array_to_pyarrow(py, arr.as_ref()).expect("export");
            let back = pyarrow_to_arrow_array(py, &pa_obj).expect("import");
            assert_eq!(back.len(), 4);
            assert_eq!(back.null_count(), 2);
            let f = back
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64");
            assert!(f.is_null(1));
            assert!(f.is_null(3));
            assert!((f.value(0) - 1.0).abs() < 1e-12);
        });
    }

    #[test]
    fn object_without_protocol_yields_error() {
        if !ensure_python() {
            return;
        }
        Python::attach(|py| {
            let dict = pyo3::types::PyDict::new(py);
            let err = pyarrow_to_arrow_array(py, dict.as_any()).unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains("__arrow_c_array__"), "unexpected error: {msg}");
        });
    }

    #[test]
    fn datatype_mismatch_detected() {
        let arr: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![1_i64]));
        let err = assert_array_datatype(arr.as_ref(), &DataType::Float64).unwrap_err();
        assert!(matches!(err, PyPluginError::ArrowConversion(_)));
    }
}
