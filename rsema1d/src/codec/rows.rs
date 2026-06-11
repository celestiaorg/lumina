use crate::error::{Error, Result};
use crate::params::Parameters;

/// Contiguous row-major byte matrix (rows × row_size).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMatrix {
    data: Vec<u8>,
    row_size: usize,
    rows: usize,
}

impl RowMatrix {
    /// Create a matrix from a flat byte buffer with the given shape.
    pub fn with_shape(data: Vec<u8>, rows: usize, row_size: usize) -> Result<Self> {
        if row_size == 0 {
            return Err(Error::InvalidParameters("row_size must be > 0".to_string()));
        }
        if rows * row_size != data.len() {
            return Err(Error::InvalidParameters(format!(
                "row buffer size mismatch: expected {} bytes (rows={} row_size={}), got {}",
                rows * row_size,
                rows,
                row_size,
                data.len()
            )));
        }
        Ok(Self {
            data,
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
        &self.data
    }

    /// Returns the underlying flat buffer as a mutable byte slice.
    pub fn as_row_major_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Consumes the matrix and returns the underlying byte buffer.
    pub fn into_row_major(self) -> Vec<u8> {
        self.data
    }

    /// Returns the row at `index`, or an error if out of bounds.
    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.rows {
            return Err(Error::InvalidIndex(index, self.rows));
        }
        Ok(self.row_unchecked(index))
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
        &self.data[start..end]
    }

    pub(crate) fn row_mut_unchecked(&mut self, index: usize) -> &mut [u8] {
        let start = index * self.row_size;
        let end = start + self.row_size;
        &mut self.data[start..end]
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
    fn from(value: RowMatrix) -> Self {
        value.into_row_major()
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
