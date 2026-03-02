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

    pub fn from_bytes(data: Vec<u8>, row_size: usize) -> Result<Self> {
        if row_size == 0 {
            return Err(Error::InvalidParameters("row_size must be > 0".to_string()));
        }
        if data.len() % row_size != 0 {
            return Err(Error::InvalidParameters(format!(
                "row buffer size mismatch: {} is not divisible by row_size {}",
                data.len(),
                row_size
            )));
        }
        let rows = data.len() / row_size;
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

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.rows {
            return Err(Error::InvalidIndex(index, self.rows));
        }
        Ok(self.row_unchecked(index))
    }

    pub(crate) fn row_unchecked(&self, index: usize) -> &[u8] {
        let start = index * self.row_size;
        let end = start + self.row_size;
        &self.data[start..end]
    }
}

impl AsRef<[u8]> for RowMatrix {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginalRows(RowMatrix);

impl OriginalRows {
    pub fn new(data: Vec<u8>, params: &Parameters) -> Result<Self> {
        let matrix = RowMatrix::with_shape(data, params.k, params.row_size)?;
        Ok(Self(matrix))
    }

    pub fn from_row_major(data: Vec<u8>, params: &Parameters) -> Result<Self> {
        Self::new(data, params)
    }

    pub fn rows(&self) -> usize {
        self.0.rows()
    }

    pub fn row_size(&self) -> usize {
        self.0.row_size()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_bytes()
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        self.0.row(index)
    }
}

impl AsRef<[u8]> for OriginalRows {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<OriginalRows> for Vec<u8> {
    fn from(value: OriginalRows) -> Self {
        value.into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendedRows(RowMatrix);

impl ExtendedRows {
    pub fn new(data: Vec<u8>, params: &Parameters) -> Result<Self> {
        let matrix = RowMatrix::with_shape(data, params.total_rows(), params.row_size)?;
        Ok(Self(matrix))
    }

    pub fn from_row_major(data: Vec<u8>, params: &Parameters) -> Result<Self> {
        Self::new(data, params)
    }

    pub fn rows(&self) -> usize {
        self.0.rows()
    }

    pub fn row_size(&self) -> usize {
        self.0.row_size()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_bytes()
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        self.0.row(index)
    }

    pub fn sample(&self, indices: &[usize]) -> Result<SampledRows> {
        let mut out = vec![0u8; indices.len() * self.row_size()];
        for (dst_i, &src_i) in indices.iter().enumerate() {
            let src = self.row(src_i)?;
            let start = dst_i * self.row_size();
            let end = start + self.row_size();
            out[start..end].copy_from_slice(src);
        }
        SampledRows::from_row_major(out, self.row_size())
    }

    pub(crate) fn matrix(&self) -> &RowMatrix {
        &self.0
    }
}

impl AsRef<[u8]> for ExtendedRows {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<ExtendedRows> for Vec<u8> {
    fn from(value: ExtendedRows) -> Self {
        value.into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampledRows(RowMatrix);

impl SampledRows {
    pub fn from_bytes(data: Vec<u8>, row_size: usize) -> Result<Self> {
        let matrix = RowMatrix::from_bytes(data, row_size)?;
        Ok(Self(matrix))
    }

    pub fn from_row_major(data: Vec<u8>, row_size: usize) -> Result<Self> {
        Self::from_bytes(data, row_size)
    }

    pub fn rows(&self) -> usize {
        self.0.rows()
    }

    pub fn row_size(&self) -> usize {
        self.0.row_size()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        self.0.row(index)
    }
}

impl AsRef<[u8]> for SampledRows {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<SampledRows> for Vec<u8> {
    fn from(value: SampledRows) -> Self {
        value.0.into_bytes()
    }
}
