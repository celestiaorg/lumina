use crate::error::{Error, Result};
use crate::params::Parameters;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMatrix {
    data: Vec<u8>,
    row_size: usize,
    rows: usize,
}

impl RowMatrix {
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

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn row_size(&self) -> usize {
        self.row_size
    }

    pub fn as_row_major(&self) -> &[u8] {
        &self.data
    }

    pub fn as_row_major_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn into_row_major(self) -> Vec<u8> {
        self.data
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.rows {
            return Err(Error::InvalidIndex(index, self.rows));
        }
        Ok(self.row_unchecked(index))
    }

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

#[derive(Debug, Clone, Copy)]
pub struct OriginalRowsView<'a> {
    matrix: &'a RowMatrix,
    params: Parameters,
}

impl<'a> OriginalRowsView<'a> {
    pub fn rows(&self) -> usize {
        self.params.k
    }

    pub fn row_size(&self) -> usize {
        self.params.row_size
    }

    pub fn as_row_major(&self) -> &'a [u8] {
        self.matrix.as_row_major()
    }

    pub fn row(&self, index: usize) -> Result<&'a [u8]> {
        if index >= self.params.k {
            return Err(Error::InvalidIndex(index, self.params.k));
        }
        Ok(self.matrix.row_unchecked(index))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExtendedRowsView<'a> {
    matrix: &'a RowMatrix,
    params: Parameters,
}

impl<'a> ExtendedRowsView<'a> {
    pub fn rows(&self) -> usize {
        self.params.total_rows()
    }

    pub fn row_size(&self) -> usize {
        self.params.row_size
    }

    pub fn as_row_major(&self) -> &'a [u8] {
        self.matrix.as_row_major()
    }

    pub fn row(&self, index: usize) -> Result<&'a [u8]> {
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }
        Ok(self.matrix.row_unchecked(index))
    }
}

pub struct ExtendedRowsViewMut<'a> {
    matrix: &'a mut RowMatrix,
    params: Parameters,
}

impl<'a> ExtendedRowsViewMut<'a> {
    pub fn split_original_parity(&mut self) -> (&[u8], &mut [u8]) {
        let split_at = self.params.k * self.params.row_size;
        let (orig, parity) = self.matrix.as_row_major_mut().split_at_mut(split_at);
        (orig, parity)
    }
}
