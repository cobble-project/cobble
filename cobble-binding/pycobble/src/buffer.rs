use bytes::Bytes;
use pyo3::exceptions::PyBufferError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyMemoryView};
use std::ffi::{c_int, c_void};
use std::ptr;

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
}
