use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyBufferError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyMemoryView};
use std::ffi::{c_int, c_void};
use std::ptr;
use std::slice;

use crate::types::{PickleReduction, PyBufferResult, PyBufferStatus};

pub(crate) enum InputBytes {
    ReadOnly(PyBuffer<u8>),
    Owned(Vec<u8>),
}

pub(crate) struct WritableBuffer {
    buffer: PyBuffer<u8>,
}

impl WritableBuffer {
    pub(crate) fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let buffer = PyBuffer::<u8>::get(value)?;
        if buffer.readonly() {
            return Err(PyBufferError::new_err("output buffer is read-only"));
        }
        if !buffer.is_c_contiguous() {
            return Err(crate::error::input_error(
                "output buffer must be C-contiguous",
            ));
        }
        Ok(Self { buffer })
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        if self.buffer.len_bytes() == 0 {
            return &mut [];
        }
        // SAFETY: extraction accepts only writable, C-contiguous u8 buffers.
        // Into-buffer calls stay attached to the GIL for the whole write.
        unsafe {
            slice::from_raw_parts_mut(self.buffer.buf_ptr().cast::<u8>(), self.buffer.len_bytes())
        }
    }
}

pub(crate) fn copy_single_column(
    columns: Option<Vec<Option<Bytes>>>,
    output: &Bound<'_, PyAny>,
) -> PyResult<PyBufferResult> {
    let Some(column) = columns.and_then(|columns| columns.into_iter().next().flatten()) else {
        return Ok(PyBufferResult::new(PyBufferStatus::NotFound, 0, 0, 0));
    };
    let required = column.len();
    let mut output = WritableBuffer::extract(output)?;
    if output.as_mut_slice().len() < required {
        return Ok(PyBufferResult::new(
            PyBufferStatus::BufferTooSmall,
            0,
            required,
            1,
        ));
    }
    output.as_mut_slice()[..required].copy_from_slice(&column);
    Ok(PyBufferResult::new(
        PyBufferStatus::Ok,
        required,
        required,
        1,
    ))
}

impl InputBytes {
    pub(crate) fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let buffer = PyBuffer::<u8>::get(value)?;
        if !buffer.is_c_contiguous() {
            return Err(crate::error::input_error(
                "byte buffer must be C-contiguous",
            ));
        }
        // Only Python `bytes` has an immutable backing allocation. A read-only
        // memoryview can still reference a mutable bytearray which another
        // Python thread may modify while a native operation has detached.
        if value.cast::<PyBytes>().is_ok() {
            Ok(Self::ReadOnly(buffer))
        } else {
            Ok(Self::Owned(buffer.to_vec(value.py())?))
        }
    }
}

impl AsRef<[u8]> for InputBytes {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::ReadOnly(buffer) => {
                if buffer.len_bytes() == 0 {
                    return &[];
                }
                // SAFETY: extraction accepts only C-contiguous u8 buffers. A
                // read-only PyBuffer pins its exporter until this owner drops.
                unsafe { slice::from_raw_parts(buffer.buf_ptr().cast::<u8>(), buffer.len_bytes()) }
            }
            Self::Owned(bytes) => bytes,
        }
    }
}

#[pyclass(module = "pycobble._native", frozen)]
pub(crate) struct OwnedBytes {
    bytes: Bytes,
}

impl OwnedBytes {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    #[cfg(test)]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

#[pymethods]
impl OwnedBytes {
    #[staticmethod]
    fn _restore(value: Vec<u8>) -> Self {
        Self::new(Bytes::from(value))
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Vec<u8>,)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.bytes.to_vec(),),
        ))
    }

    /// Export the Rust-owned allocation through Python's read-only buffer protocol.
    ///
    /// # Safety
    ///
    /// PyO3 calls this slot with a valid Python object. The exported pointer remains
    /// valid because `view.obj` owns a reference to that object until the consumer
    /// releases the buffer.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("buffer view is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("OwnedBytes is read-only"));
        }

        let bytes = slf.borrow();
        let len = isize::try_from(bytes.bytes.len())
            .map_err(|_| PyBufferError::new_err("buffer length exceeds Py_ssize_t"))?;
        unsafe {
            (*view).obj = slf.clone().into_any().into_ptr();
            (*view).buf = bytes.bytes.as_ptr() as *mut c_void;
            (*view).len = len;
            (*view).readonly = 1;
            (*view).itemsize = 1;
            (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
                c"B".as_ptr() as *mut _
            } else {
                ptr::null_mut()
            };
            (*view).ndim = 1;
            (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
                &mut (*view).len
            } else {
                ptr::null_mut()
            };
            (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
                &mut (*view).itemsize
            } else {
                ptr::null_mut()
            };
            (*view).suboffsets = ptr::null_mut();
            (*view).internal = ptr::null_mut();
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}

    fn __len__(&self) -> usize {
        self.bytes.len()
    }

    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    fn view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    fn __repr__(&self) -> String {
        format!("OwnedBytes(len={})", self.bytes.len())
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<OwnedBytes>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_bytes_preserve_the_rust_allocation() {
        let bytes = Bytes::from_static(b"cobble");
        let pointer = bytes.as_ptr();
        let owner = OwnedBytes::new(bytes);
        assert_eq!(owner.as_slice(), b"cobble");
        assert_eq!(owner.as_slice().as_ptr(), pointer);
    }

    #[test]
    fn immutable_python_input_is_borrowed_and_mutable_input_is_copied() {
        Python::initialize();
        Python::attach(|py| {
            let immutable = PyBytes::new(py, b"immutable").into_any();
            let input = InputBytes::extract(&immutable).expect("bytes input");
            assert!(matches!(input, InputBytes::ReadOnly(_)));
            assert_eq!(input.as_ref(), b"immutable");

            let mutable = pyo3::types::PyByteArray::new(py, b"mutable").into_any();
            let input = InputBytes::extract(&mutable).expect("bytearray input");
            assert!(matches!(input, InputBytes::Owned(_)));
            assert_eq!(input.as_ref(), b"mutable");
        });
    }
}
