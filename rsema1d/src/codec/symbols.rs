use crate::field::GF128;

/// Extract GF(2^16) symbols from 64-byte chunk (Leopard interleaved format)
pub fn extract_symbols(chunk: &[u8; 64]) -> [u16; 32] {
    let mut symbols = [0u16; 32];
    for i in 0..32 {
        // Leopard interleaved format:
        // Low bytes in positions 0-31, high bytes in positions 32-63
        symbols[i] = u16::from_le_bytes([chunk[i], chunk[32 + i]]);
    }
    symbols
}

/// Compute RLC for a single row
pub fn compute_rlc(row: &[u8], coeffs: &[GF128]) -> GF128 {
    let num_chunks = row.len() / 64;
    let mut rlc = GF128::zero();

    for chunk_idx in 0..num_chunks {
        let chunk_start = chunk_idx * 64;

        // Process symbols directly without allocating array
        for j in 0..32 {
            // Leopard interleaved format
            let symbol = u16::from_le_bytes([row[chunk_start + j], row[chunk_start + 32 + j]]);

            if symbol != 0 {
                let symbol_index = chunk_idx * 32 + j;
                rlc += coeffs[symbol_index].scalar_mul(symbol);
            }
        }
    }

    rlc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_symbols() {
        let mut chunk = [0u8; 64];
        chunk[0] = 0x01;
        chunk[32] = 0x10;
        chunk[1] = 0x02;
        chunk[33] = 0x20;

        let symbols = extract_symbols(&chunk);
        assert_eq!(symbols[0], 0x1001);
        assert_eq!(symbols[1], 0x2002);
    }

    #[test]
    fn test_compute_rlc() {
        let row = vec![0u8; 64];
        let coeffs = vec![GF128::zero(); 32];

        let rlc = compute_rlc(&row, &coeffs);
        assert_eq!(rlc, GF128::zero());
    }
}
