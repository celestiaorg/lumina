use crate::field::GF128;
use reed_solomon_simd::engine::tables::get_exp_log;

/// GF(2^16) has 65535 non-zero elements, so logarithms are `0..=65534`.
const GF_MODULUS: u32 = 65535;

/// Log-table sentinel for a coefficient limb that is zero (no logarithm).
const ZERO_LIMB: u16 = u16::MAX;

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

/// `(a + b) mod 65535` for two logarithms.
#[inline(always)]
const fn add_mod(a: u16, b: u16) -> u16 {
    let sum = a as u32 + b as u32;
    (if sum >= GF_MODULUS {
        sum - GF_MODULUS
    } else {
        sum
    }) as u16
}

/// GF(2^16) logarithms precomputed for every limb of the RLC coefficients.
///
/// The coefficients are fixed for a blob (they are derived from the row root),
/// while RLC computation runs over every symbol of every row. Precomputing
/// `log(coefficient limb)` once turns the per-symbol work into one `log`
/// lookup for the symbol plus one `exp` lookup per limb.
#[derive(Debug, Clone)]
pub(crate) struct RlcCoefficientLogs {
    /// `log(coefficients[i].limbs[l])`, or [`ZERO_LIMB`] when the limb is 0.
    logs: Vec<[u16; 8]>,
}

impl RlcCoefficientLogs {
    /// Precompute limb logarithms for `coefficients`.
    pub(crate) fn new(coefficients: Vec<GF128>) -> Self {
        let log = &get_exp_log().log;
        let logs = coefficients
            .into_iter()
            .map(|c| {
                let mut limb_logs = [ZERO_LIMB; 8];
                for (dst, limb) in limb_logs.iter_mut().zip(c.limbs) {
                    if limb != 0 {
                        *dst = log[limb as usize];
                    }
                }
                limb_logs
            })
            .collect();
        Self { logs }
    }

    /// Compute the RLC of `row`: the GF(2^128) sum over all symbols of
    /// `symbol * coefficient[symbol_index]`.
    ///
    /// Only complete 64-byte chunks of `row` are used, matching
    /// [`compute_rlc`]. Panics if `row` has more symbols than there are
    /// coefficients.
    pub(crate) fn compute_rlc(&self, row: &[u8]) -> GF128 {
        let exp_log = get_exp_log();
        let exp = &exp_log.exp;
        let log = &exp_log.log;
        let mut acc = [0u16; 8];

        let (chunks, _) = row.as_chunks::<64>();
        for (chunk_idx, chunk) in chunks.iter().enumerate() {
            let chunk_logs = &self.logs[chunk_idx * 32..chunk_idx * 32 + 32];
            for (j, limb_logs) in chunk_logs.iter().enumerate() {
                // Leopard interleaved format
                let symbol = u16::from_le_bytes([chunk[j], chunk[32 + j]]);
                if symbol == 0 {
                    continue;
                }
                let log_symbol = log[symbol as usize];
                for (dst, &limb_log) in acc.iter_mut().zip(limb_logs) {
                    if limb_log != ZERO_LIMB {
                        *dst ^= exp[add_mod(log_symbol, limb_log) as usize];
                    }
                }
            }
        }

        GF128 { limbs: acc }
    }
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
    use rand::{Rng, RngCore, SeedableRng};
    use rand_chacha::ChaCha8Rng;

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

    #[test]
    fn precomputed_logs_match_scalar_mul() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        for row_size in [64usize, 128, 1024, 4096, 32768] {
            let mut coeffs: Vec<GF128> = (0..row_size / 2)
                .map(|_| {
                    let mut limbs = [0u16; 8];
                    for limb in &mut limbs {
                        // Force some zero limbs and zero coefficients.
                        *limb = if rng.gen_ratio(1, 16) { 0 } else { rng.gen() };
                    }
                    GF128 { limbs }
                })
                .collect();
            coeffs[0] = GF128::zero();
            let table = RlcCoefficientLogs::new(coeffs.clone());
            assert_eq!(table.logs.len(), coeffs.len());

            for _ in 0..8 {
                let mut row = vec![0u8; row_size];
                rng.fill_bytes(&mut row);
                // Sprinkle zero symbols.
                for _ in 0..row_size / 16 {
                    let i = rng.gen_range(0..row_size / 2);
                    let chunk = i / 32;
                    let j = i % 32;
                    row[chunk * 64 + j] = 0;
                    row[chunk * 64 + 32 + j] = 0;
                }
                assert_eq!(table.compute_rlc(&row), compute_rlc(&row, &coeffs));
            }
            let zero_row = vec![0u8; row_size];
            assert_eq!(table.compute_rlc(&zero_row), GF128::zero());
        }
    }

    #[test]
    fn precomputed_logs_wrap_exponents() {
        let exp = &get_exp_log().exp;
        let mut coeffs = vec![GF128::zero(); 32];
        coeffs[0].limbs[0] = exp[65534];

        let mut row = [0u8; 64];
        let symbol = exp[1].to_le_bytes();
        row[0] = symbol[0];
        row[32] = symbol[1];

        let table = RlcCoefficientLogs::new(coeffs.clone());
        assert_eq!(table.compute_rlc(&row), compute_rlc(&row, &coeffs));
    }
}
