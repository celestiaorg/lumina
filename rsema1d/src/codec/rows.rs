use crate::error::{Error, Result};
use crate::params::Parameters;
use bytes::Bytes;
use std::fmt;

#[cfg(target_os = "linux")]
const HUGE_PAGE_THRESHOLD: usize = 4 * 1024 * 1024;

enum Storage {
    Heap(Vec<u8>),
    #[cfg(target_os = "linux")]
    Mapped(memmap2::MmapMut),
    Shared(Bytes),
}

impl Storage {
    fn zeroed(len: usize) -> Self {
        #[cfg(target_os = "linux")]
        if len >= HUGE_PAGE_THRESHOLD {
            if let Ok(data) = memmap2::MmapMut::map_anon(len) {
                let _ = data.advise(memmap2::Advice::HugePage);
                return Self::Mapped(data);
            }
        }

        Self::Heap(vec![0u8; len])
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Heap(data) => data,
            #[cfg(target_os = "linux")]
            Self::Mapped(data) => data,
            Self::Shared(data) => data,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        if let Self::Shared(data) = self {
            *self = Self::Heap(Vec::from(std::mem::take(data)));
        }
        match self {
            Self::Heap(data) => data,
            #[cfg(target_os = "linux")]
            Self::Mapped(data) => data,
            Self::Shared(_) => unreachable!("converted to heap above"),
        }
    }

    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Heap(data) => data,
            #[cfg(target_os = "linux")]
            Self::Mapped(data) => data.to_vec(),
            Self::Shared(data) => Vec::from(data),
        }
    }

    fn freeze(&mut self) {
        let data = std::mem::replace(self, Self::Heap(Vec::new()));
        *self = match data {
            Self::Heap(data) => Self::Shared(Bytes::from(data)),
            #[cfg(target_os = "linux")]
            Self::Mapped(data) => Self::Shared(Bytes::from_owner(data)),
            Self::Shared(data) => Self::Shared(data),
        };
    }
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        match self {
            Self::Heap(data) => Self::Heap(data.clone()),
            #[cfg(target_os = "linux")]
            Self::Mapped(data) => {
                let mut clone = Self::zeroed(data.len());
                clone.as_mut_slice().copy_from_slice(data);
                clone
            }
            Self::Shared(data) => Self::Shared(data.clone())
        }
    }
}

impl PartialEq for Storage {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Storage {}

/// Contiguous row-major byte matrix (rows × row_size).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMatrix {
    data: Storage,
    row_size: usize,
    rows: usize,
}

impl RowMatrix {
    fn checked_len(rows: usize, row_size: usize) -> Result<usize> {
        rows.checked_mul(row_size).ok_or_else(|| {
            Error::InvalidParameters(format!(
                "row buffer size overflow: rows={rows} row_size={row_size}"
            ))
        })
    }

    /// Create a matrix from a flat byte buffer with the given shape.
    pub fn with_shape(data: Vec<u8>, rows: usize, row_size: usize) -> Result<Self> {
        if row_size == 0 {
            return Err(Error::InvalidParameters("row_size must be > 0".to_string()));
        }
        let expected_len = Self::checked_len(rows, row_size)?;
        if expected_len != data.len() {
            return Err(Error::InvalidParameters(format!(
                "row buffer size mismatch: expected {} bytes (rows={} row_size={}), got {}",
                expected_len,
                rows,
                row_size,
                data.len()
            )));
        }
        Ok(Self {
            data: Storage::Heap(data),
            row_size,
            rows,
        })
    }

    /// Creates a zero-initialized matrix with the given shape.
    pub fn zeroed(rows: usize, row_size: usize) -> Result<Self> {
        if row_size == 0 {
            return Err(Error::InvalidParameters("row_size must be > 0".to_string()));
        }
        let len = Self::checked_len(rows, row_size)?;
        Ok(Self {
            data: Storage::zeroed(len),
            row_size,
            rows,
        })
    }

    /// Returns the number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the byte length of each row.
    pub fn row_size(&self) -> usize {
        self.row_size
    }

    /// Returns the underlying flat buffer as a byte slice.
    pub fn as_row_major(&self) -> &[u8] {
        self.data.as_slice()
    }

    /// Returns the underlying flat buffer as a mutable byte slice.
    ///
    /// If the matrix was frozen and rows are still shared, this copies the
    /// buffer first.
    pub fn as_row_major_mut(&mut self) -> &mut [u8] {
        self.data.as_mut_slice()
    }

    /// Consumes the matrix and returns the underlying byte buffer.
    ///
    /// Heap storage is returned directly. Memory-mapped storage is copied into a new allocation.
    pub fn into_row_major(self) -> Vec<u8> {
        self.data.into_vec()
    }

    pub(crate) fn freeze(&mut self) {
        self.data.freeze();
    }

    /// Returns the row at `index`, or an error if out of bounds.
    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.rows {
            return Err(Error::InvalidIndex(index, self.rows));
        }
        Ok(self.row_unchecked(index))
    }

    pub(crate) fn row_bytes(&self, index: usize) -> Result<Bytes> {
        if index >= self.rows {
            return Err(Error::InvalidIndex(index, self.rows));
        }
        let start = index * self.row_size;
        let end = start + self.row_size;
        Ok(match &self.data {
            Storage::Shared(b) => b.slice(start..end),
            data => Bytes::copy_from_slice(&data.as_slice()[start..end]),
        })
    }

    /// Returns a mutable reference to the row at `index`.
    pub fn row_mut(&mut self, index: usize) -> Result<&mut [u8]> {
        if index >= self.rows {
            return Err(Error::InvalidIndex(index, self.rows));
        }
        Ok(self.row_mut_unchecked(index))
    }

    pub(crate) fn row_unchecked(&self, index: usize) -> &[u8] {
        let start = index * self.row_size;
        let end = start + self.row_size;
        &self.data.as_slice()[start..end]
    }

    pub(crate) fn row_mut_unchecked(&mut self, index: usize) -> &mut [u8] {
        let start = index * self.row_size;
        let end = start + self.row_size;
        &mut self.data.as_mut_slice()[start..end]
    }

    /// Creates a new matrix containing only the rows at the given indices.
    pub fn sample(&self, indices: &[usize]) -> Result<RowMatrix> {
        let mut out = vec![0u8; indices.len() * self.row_size];
        for (dst_i, &src_i) in indices.iter().enumerate() {
            let src = self.row(src_i)?;
            let start = dst_i * self.row_size;
            let end = start + self.row_size;
            out[start..end].copy_from_slice(src);
        }
        RowMatrix::with_shape(out, indices.len(), self.row_size)
    }

    /// Returns a typed view over the original K rows, validating the shape.
    pub fn original_view<'a>(&'a self, params: &Parameters) -> Result<OriginalRowsView<'a>> {
        if self.rows != params.k || self.row_size != params.row_size {
            return Err(Error::InvalidParameters(format!(
                "original rows shape mismatch: expected {}x{}, got {}x{}",
                params.k, params.row_size, self.rows, self.row_size
            )));
        }
        Ok(OriginalRowsView {
            matrix: self,
            params: *params,
        })
    }

    /// Returns a typed view over all K+N rows, validating the shape.
    pub fn extended_view<'a>(&'a self, params: &Parameters) -> Result<ExtendedRowsView<'a>> {
        if self.rows != params.total_rows() || self.row_size != params.row_size {
            return Err(Error::InvalidParameters(format!(
                "extended rows shape mismatch: expected {}x{}, got {}x{}",
                params.total_rows(),
                params.row_size,
                self.rows,
                self.row_size
            )));
        }
        Ok(ExtendedRowsView {
            matrix: self,
            params: *params,
        })
    }

    /// Returns a mutable typed view over all K+N rows.
    pub fn extended_view_mut<'a>(
        &'a mut self,
        params: &Parameters,
    ) -> Result<ExtendedRowsViewMut<'a>> {
        if self.rows != params.total_rows() || self.row_size != params.row_size {
            return Err(Error::InvalidParameters(format!(
                "extended rows shape mismatch: expected {}x{}, got {}x{}",
                params.total_rows(),
                params.row_size,
                self.rows,
                self.row_size
            )));
        }
        Ok(ExtendedRowsViewMut {
            matrix: self,
            params: *params,
        })
    }
}

impl AsRef<[u8]> for RowMatrix {
    fn as_ref(&self) -> &[u8] {
        self.as_row_major()
    }
}

impl From<RowMatrix> for Vec<u8> {
    /// Uses the same allocation behavior as [`RowMatrix::into_row_major`].
    fn from(value: RowMatrix) -> Self {
        value.into_row_major()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeroed_matrix_round_trip() {
        let mut matrix = RowMatrix::zeroed(4, 64).unwrap();
        assert_eq!(matrix.as_row_major(), &[0u8; 256]);

        matrix.row_mut(2).unwrap()[7] = 42;
        let clone = matrix.clone();
        assert_eq!(clone, matrix);
        assert_eq!(clone.into_row_major(), matrix.as_row_major());
    }

    #[test]
    fn shape_size_overflow_is_rejected() {
        for result in [
            RowMatrix::with_shape(Vec::new(), 2, usize::MAX),
            RowMatrix::zeroed(2, usize::MAX),
        ] {
            let Error::InvalidParameters(message) = result.unwrap_err() else {
                panic!("expected invalid parameters error");
            };
            assert!(message.contains("overflow"));
        }
    }

    #[test]
    fn mutating_frozen_matrix_does_not_modify_shared_rows() {
        let mut matrix = RowMatrix::with_shape(vec![0; 128], 2, 64).unwrap();
        matrix.freeze();
        let row = matrix.row_bytes(0).unwrap();

        matrix.row_mut(0).unwrap()[0] = 1;

        assert_eq!(row[0], 0);
        assert_eq!(matrix.row(0).unwrap()[0], 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn large_zeroed_matrix_round_trip() {
        let mut matrix = RowMatrix::zeroed(HUGE_PAGE_THRESHOLD / 64, 64).unwrap();
        assert!(matrix.as_row_major().iter().all(|byte| *byte == 0));

        matrix.row_mut(1).unwrap()[1] = 7;
        let clone = matrix.clone();
        assert_eq!(clone, matrix);
        assert_eq!(clone.into_row_major(), matrix.as_row_major());

        let ptr = matrix.as_row_major().as_ptr();
        matrix.freeze();
        assert!(matches!(matrix.data, Storage::Shared(_)));
        assert_eq!(matrix.as_row_major().as_ptr(), ptr);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn large_heap_clone_stays_heap() {
        let rows = HUGE_PAGE_THRESHOLD / 64;
        let matrix = RowMatrix::with_shape(vec![1u8; HUGE_PAGE_THRESHOLD], rows, 64).unwrap();
        assert!(matches!(&matrix.data, RowStorage::Heap(_)));

        let clone = matrix.clone();
        assert!(matches!(&clone.data, RowStorage::Heap(_)));
        assert_eq!(clone, matrix);
    }
}

/// Read-only view over the original K rows of a [`RowMatrix`].
#[derive(Debug, Clone, Copy)]
pub struct OriginalRowsView<'a> {
    matrix: &'a RowMatrix,
    params: Parameters,
}

impl<'a> OriginalRowsView<'a> {
    /// Returns the number of original rows (K).
    pub fn rows(&self) -> usize {
        self.params.k
    }

    /// Returns the byte length of each row.
    pub fn row_size(&self) -> usize {
        self.params.row_size
    }

    /// Returns the underlying flat buffer as a byte slice.
    pub fn as_row_major(&self) -> &'a [u8] {
        self.matrix.as_row_major()
    }

    /// Returns the row at `index`, or an error if out of bounds.
    pub fn row(&self, index: usize) -> Result<&'a [u8]> {
        if index >= self.params.k {
            return Err(Error::InvalidIndex(index, self.params.k));
        }
        Ok(self.matrix.row_unchecked(index))
    }
}

/// Read-only view over all K+N rows of a [`RowMatrix`].
#[derive(Debug, Clone, Copy)]
pub struct ExtendedRowsView<'a> {
    matrix: &'a RowMatrix,
    params: Parameters,
}

impl<'a> ExtendedRowsView<'a> {
    /// Returns the total number of rows (K+N).
    pub fn rows(&self) -> usize {
        self.params.total_rows()
    }

    /// Returns the byte length of each row.
    pub fn row_size(&self) -> usize {
        self.params.row_size
    }

    /// Returns the underlying flat buffer as a byte slice.
    pub fn as_row_major(&self) -> &'a [u8] {
        self.matrix.as_row_major()
    }

    /// Returns the row at `index`, or an error if out of bounds.
    pub fn row(&self, index: usize) -> Result<&'a [u8]> {
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }
        Ok(self.matrix.row_unchecked(index))
    }
}

/// Mutable view over all K+N rows, used for in-place parity encoding.
pub struct ExtendedRowsViewMut<'a> {
    matrix: &'a mut RowMatrix,
    params: Parameters,
}

impl<'a> ExtendedRowsViewMut<'a> {
    /// Splits the buffer into an immutable original-rows slice and a mutable parity slice.
    pub fn split_original_parity(&mut self) -> (&[u8], &mut [u8]) {
        let split_at = self.params.k * self.params.row_size;
        let (orig, parity) = self.matrix.as_row_major_mut().split_at_mut(split_at);
        (orig, parity)
    }
}
