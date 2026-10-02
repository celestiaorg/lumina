use crate::field::GF128;
use rayon::prelude::*;
use reed_solomon_simd::engine::tables::get_exp_log;
use reed_solomon_simd::engine::{utils::xor, DefaultEngine, Engine};

pub(crate) fn compute_rlcs(data: &[u8], row_size: usize, coefficients: Vec<GF128>) -> Vec<GF128> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if let Some(engine) = super::rlc_simd::Avx2Rlc::new() {
        return compute_rlcs_with_kernel(
            data,
            row_size,
            coefficients,
            |input, outputs, scalars| {
                engine.mul_add8(input, outputs, scalars);
            },
        );
    }
    compute_rlcs_with_engine(data, row_size, coefficients, &DefaultEngine::new())
}

fn compute_rlcs_with_engine(
    data: &[u8],
    row_size: usize,
    coefficients: Vec<GF128>,
    engine: &(impl Engine + Sync),
) -> Vec<GF128> {
    compute_rlcs_with_kernel(data, row_size, coefficients, |input, outputs, scalars| {
        mul_add8_portable(engine, input, outputs, scalars);
    })
}

fn mul_add8_portable(
    engine: &impl Engine,
    input: &[[u8; 64]],
    outputs: &mut [&mut [[u8; 64]]; 8],
    coefficients: &[u16; 8],
) {
    let log = &get_exp_log().log;
    for (output, &coefficient) in outputs.iter_mut().zip(coefficients) {
        assert_eq!(output.len(), input.len());
        if coefficient == 0 {
            continue;
        }
        let log = log[coefficient as usize];
        for (src, dst) in input.iter().zip(output.iter_mut()) {
            let mut product = [*src];
            engine.mul(&mut product, log);
            xor(std::slice::from_mut(dst), &product);
        }
    }
}

fn compute_rlcs_with_kernel(
    data: &[u8],
    row_size: usize,
    coefficients: Vec<GF128>,
    mul_add8: impl Fn(&[[u8; 64]], &mut [&mut [[u8; 64]]; 8], &[u16; 8]) + Sync,
) -> Vec<GF128> {
    let k = data.len() / row_size;
    if k == 0 {
        return Vec::new();
    }
    let blocks = k.div_ceil(32);
    let chunks = row_size / 64;
    let workers = rayon::current_num_threads().min(chunks);
    let accumulate = |worker: usize| {
        let span = chunks / workers;
        let extra = chunks % workers;
        let start = worker * span + worker.min(extra);
        let end = start + span + usize::from(worker < extra);
        let mut acc = vec![[0u8; 64]; 8 * blocks];
        let mut columns = vec![[0u8; 64]; 32 * blocks];
        let mut components = acc.chunks_exact_mut(blocks);
        let mut outputs = std::array::from_fn(|_| components.next().unwrap());
        for c in start..end {
            transpose_chunk(data, row_size, c, &mut columns);
            for (j, coefficient) in coefficients[c * 32..(c + 1) * 32].iter().enumerate() {
                let column = &columns[j * blocks..(j + 1) * blocks];
                mul_add8(column, &mut outputs, &coefficient.limbs);
            }
        }
        acc
    };
    let total = if workers == 1 {
        accumulate(0)
    } else {
        let mut partials = (0..workers)
            .into_par_iter()
            .map(accumulate)
            .collect::<Vec<_>>();
        let (total, rest) = partials.split_first_mut().unwrap();
        for partial in rest {
            xor(total, partial);
        }
        std::mem::take(total)
    };
    (0..k)
        .map(|r| GF128 {
            limbs: std::array::from_fn(|comp| {
                let block = &total[comp * blocks + r / 32];
                u16::from_le_bytes([block[r % 32], block[32 + r % 32]])
            }),
        })
        .collect()
}

fn transpose_chunk(data: &[u8], row_size: usize, chunk: usize, columns: &mut [[u8; 64]]) {
    let k = data.len() / row_size;
    let blocks = k.div_ceil(32);
    for rb in 0..blocks {
        let mut tile = [[0u8; 64]; 32];
        for (r, row) in tile.iter_mut().enumerate().take((k - rb * 32).min(32)) {
            let start = (rb * 32 + r) * row_size + chunk * 64;
            row.copy_from_slice(&data[start..start + 64]);
        }
        for j in 0..32 {
            let window = &mut columns[j * blocks + rb];
            for (r, row) in tile.iter().enumerate() {
                window[r] = row[j];
                window[32 + r] = row[32 + j];
            }
        }
    }
}

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

    #[test]
    fn batched_rlcs_match_scalar() {
        for workers in [1, 2, 3, 16, 32] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            pool.install(|| {
                for k in [1, 31, 32, 33, 63, 64, 65, 1023, 1024, 1025] {
                    for row_size in [64, 128, 320, 1088] {
                        let mut rng = ChaCha8Rng::seed_from_u64((k * row_size) as u64);
                        let mut data = vec![0; k * row_size];
                        rng.fill_bytes(&mut data);
                        let mut coefficients: Vec<_> = (0..row_size / 2)
                            .map(|i| GF128 {
                                limbs: std::array::from_fn(|comp| {
                                    if (i + comp) % 7 == 0 {
                                        0
                                    } else {
                                        rng.gen()
                                    }
                                }),
                            })
                            .collect();
                        coefficients[0] = GF128::zero();
                        for pattern in 0..3 {
                            if pattern == 1 {
                                for (i, byte) in data.iter_mut().enumerate() {
                                    if i % 37 != 0 {
                                        *byte = 0;
                                    }
                                }
                            } else if pattern == 2 {
                                data.fill(0);
                            }
                            let expected: Vec<_> = data
                                .chunks_exact(row_size)
                                .map(|row| compute_rlc(row, &coefficients))
                                .collect();
                            assert_eq!(
                                compute_rlcs(&data, row_size, coefficients.clone()),
                                expected,
                                "k={k}, row_size={row_size}, workers={workers}, pattern={pattern}"
                            );
                        }
                    }
                }
            });
        }
    }

    #[test]
    fn mul_add8_matches_scalar_accumulation() {
        fn check(kernel: impl Fn(&[[u8; 64]], &mut [&mut [[u8; 64]]; 8], &[u16; 8])) {
            let mut rng = ChaCha8Rng::seed_from_u64(4242);
            for blocks in [0, 1, 3, 32] {
                let mut input = vec![[0u8; 64]; blocks];
                for block in &mut input {
                    rng.fill_bytes(block);
                }
                for pattern in 0..3 {
                    if pattern == 1 {
                        for block in &mut input {
                            for i in 0..32 {
                                if i % 7 != 0 {
                                    block[i] = 0;
                                    block[32 + i] = 0;
                                }
                            }
                        }
                    } else if pattern == 2 {
                        input.fill([0; 64]);
                    }
                    for coefficients in [[0; 8], [1; 8], [0, 1, 65535, 1234, 0, 32768, 91, 65534]] {
                        let original_input = input.clone();
                        let mut actual: [Vec<[u8; 64]>; 8] = std::array::from_fn(|_| {
                            (0..blocks)
                                .map(|_| {
                                    let mut block = [0; 64];
                                    rng.fill_bytes(&mut block);
                                    block
                                })
                                .collect()
                        });
                        let mut expected = actual.clone();
                        for _ in 0..2 {
                            for (block, src) in input.iter().enumerate() {
                                for i in 0..32 {
                                    let symbol = u16::from_le_bytes([src[i], src[32 + i]]);
                                    let product = GF128 {
                                        limbs: coefficients,
                                    }
                                    .scalar_mul(symbol);
                                    for (out, limb) in expected.iter_mut().zip(product.limbs) {
                                        let bytes = limb.to_le_bytes();
                                        out[block][i] ^= bytes[0];
                                        out[block][32 + i] ^= bytes[1];
                                    }
                                }
                            }
                            let mut outputs = actual.each_mut().map(|out| out.as_mut_slice());
                            kernel(&input, &mut outputs, &coefficients);
                            assert_eq!(actual, expected, "blocks={blocks}, pattern={pattern}");
                            assert_eq!(
                                input, original_input,
                                "multiplication must preserve its input"
                            );
                        }
                    }
                }
            }
        }
        let portable = reed_solomon_simd::engine::NoSimd::new();
        check(|input, outputs, coefficients| {
            mul_add8_portable(&portable, input, outputs, coefficients)
        });
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if let Some(engine) = super::super::rlc_simd::Avx2Rlc::new() {
            check(|input, outputs, coefficients| engine.mul_add8(input, outputs, coefficients));
        }
    }

    #[test]
    fn batched_rlcs_wrap_exponents_and_support_portable_engine() {
        let exp = &get_exp_log().exp;
        let k = 33;
        let row_size = 320;
        let mut data = vec![0; k * row_size];
        let coefficients: Vec<_> = (0..row_size / 2)
            .map(|i| GF128 {
                limbs: std::array::from_fn(|comp| {
                    if comp == 0 {
                        0
                    } else {
                        exp[[0, 1, 32767, 32768, 65533, 65534][(i + comp) % 6]]
                    }
                }),
            })
            .collect();
        for (r, row) in data.chunks_exact_mut(row_size).enumerate() {
            for i in 0..row_size / 2 {
                let bytes = exp[[0, 1, 32767, 32768, 65533, 65534][(i + r) % 6]].to_le_bytes();
                row[i / 32 * 64 + i % 32] = bytes[0];
                row[i / 32 * 64 + 32 + i % 32] = bytes[1];
            }
        }
        let expected: Vec<_> = data
            .chunks_exact(row_size)
            .map(|row| compute_rlc(row, &coefficients))
            .collect();
        assert_eq!(
            compute_rlcs_with_engine(
                &data,
                row_size,
                coefficients.clone(),
                &reed_solomon_simd::engine::NoSimd::new()
            ),
            expected
        );
        assert_eq!(
            compute_rlcs(&data, row_size, coefficients.clone()),
            expected
        );
        assert_eq!(
            compute_rlcs(&[], row_size, coefficients),
            Vec::<GF128>::new()
        );
    }
}
