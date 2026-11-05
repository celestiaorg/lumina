//! AArch64 PMULL backend for original-row RLCs, ported from celestia-app's
//! `pkg/rsema1d/rlc/poly.go` and `poly_arm64.s`.
//!
//! Leopard stores GF(2^16) elements in a Cantor basis. Mapping both operands to
//! the polynomial basis turns each product into a carry-less multiply, so a row
//! is summed with unreduced 8x8-bit products (Karatsuba over the low/high bytes)
//! and reduced once per GF128 limb.

use crate::field::GF128;
use core::arch::aarch64::*;
use rayon::prelude::*;

/// Leopard's GF(2^16) Cantor basis in polynomial form.
const CANTOR_BASIS: [u16; 16] = [
    0x0001, 0xACCA, 0x3C0E, 0x163E, 0xC582, 0xED2E, 0x914C, 0x4012, 0x6C98, 0x10D8, 0x6A72, 0xB900,
    0xFDB8, 0xFB34, 0xFF38, 0x991E,
];
const POLY_MODULUS: u32 = 0x1002D;

/// An operand block holds 16 symbols as low bytes, high bytes and their XOR.
const BLOCK_SYMBOLS: usize = 16;
const OPERAND_BYTES: usize = 3 * BLOCK_SYMBOLS;
/// A coefficient table block holds four limbs' operands.
const COEFF_BYTES: usize = 4 * OPERAND_BYTES;

/// `TO_POLY[0][lo] ^ TO_POLY[1][hi]` maps a Cantor element to polynomial form.
const TO_POLY: [[u16; 256]; 2] = byte_maps(&CANTOR_BASIS);
const FROM_POLY: [[u16; 256]; 2] = byte_maps(&inverse_basis());
/// `TO_POLY` split per input nibble for `vqtbl1q_u8`: four tables for the
/// low output byte, then four for the high output byte.
const TO_POLY_NIBBLES: [[u8; 16]; 8] = nibble_tables();

const fn byte_maps(basis: &[u16; 16]) -> [[u16; 256]; 2] {
    let mut maps = [[0; 256]; 2];
    let mut x = 0;
    while x < 256 {
        let mut i = 0;
        while i < 8 {
            if x >> i & 1 == 1 {
                maps[0][x] ^= basis[i];
                maps[1][x] ^= basis[8 + i];
            }
            i += 1;
        }
        x += 1;
    }
    maps
}

/// Cantor-basis preimages of the polynomial unit vectors, by Gauss-Jordan
/// elimination over GF(2).
const fn inverse_basis() -> [u16; 16] {
    let mut poly = CANTOR_BASIS;
    let mut cantor = [0u16; 16];
    let mut i = 0;
    while i < 16 {
        cantor[i] = 1 << i;
        i += 1;
    }
    let mut bit = 0;
    while bit < 16 {
        let mut pivot = bit;
        while poly[pivot] >> bit & 1 == 0 {
            pivot += 1;
        }
        let (p, c) = (poly[pivot], cantor[pivot]);
        poly[pivot] = poly[bit];
        cantor[pivot] = cantor[bit];
        poly[bit] = p;
        cantor[bit] = c;
        let mut row = 0;
        while row < 16 {
            if row != bit && poly[row] >> bit & 1 == 1 {
                poly[row] ^= p;
                cantor[row] ^= c;
            }
            row += 1;
        }
        bit += 1;
    }
    cantor
}

const fn nibble_tables() -> [[u8; 16]; 8] {
    let mut tables = [[0; 16]; 8];
    let mut n = 0;
    while n < 4 {
        let mut x = 0;
        while x < 16 {
            let v = x << (4 * n);
            let p = TO_POLY[0][v & 0xff] ^ TO_POLY[1][v >> 8];
            tables[n][x] = p as u8;
            tables[4 + n][x] = (p >> 8) as u8;
            x += 1;
        }
        n += 1;
    }
    tables
}

fn to_poly(v: u16) -> u16 {
    TO_POLY[0][usize::from(v as u8)] ^ TO_POLY[1][usize::from(v >> 8)]
}

fn from_poly(p: u16) -> u16 {
    FROM_POLY[0][usize::from(p as u8)] ^ FROM_POLY[1][usize::from(p >> 8)]
}

/// Reduces a carry-less product of two 16-bit polynomials.
fn reduce(mut x: u32) -> u16 {
    while x >> 16 != 0 {
        x ^= POLY_MODULUS << (15 - x.leading_zeros());
    }
    x as u16
}

/// Reports whether the CPU supports every instruction the kernels use:
/// 8-bit PMULL and TBL (base ASIMD) and EOR3 (SHA3).
pub(super) fn available() -> bool {
    std::arch::is_aarch64_feature_detected!("neon")
        && std::arch::is_aarch64_feature_detected!("sha3")
}

/// Computes the RLC of every row. Callers must check [`available`].
pub(super) fn compute_rlcs(data: &[u8], row_size: usize, coefficients: &[GF128]) -> Vec<GF128> {
    let k = data.len() / row_size;
    let blocks = row_size / (2 * BLOCK_SYMBOLS);
    let mut tables = [
        vec![0u8; blocks * COEFF_BYTES],
        vec![0u8; blocks * COEFF_BYTES],
    ];
    let [lo, hi] = &mut tables;
    lo.par_chunks_exact_mut(COEFF_BYTES)
        .zip(hi.par_chunks_exact_mut(COEFF_BYTES))
        .zip(coefficients[..blocks * BLOCK_SYMBOLS].par_chunks_exact(BLOCK_SYMBOLS))
        .for_each(|((lo, hi), coefficients)| fill_coefficients([lo, hi], coefficients));

    let mut out = vec![GF128::zero(); k];
    out.par_iter_mut()
        .zip(data[..k * row_size].par_chunks_exact(row_size))
        // Per-worker scratch for the converted row.
        .for_each_init(
            || vec![0u8; blocks * OPERAND_BYTES],
            |operands, (out, row)| {
                // SAFETY: callers check `available()`, which detects neon and sha3.
                *out = unsafe { row_rlc(row, operands, &tables) };
            },
        );
    out
}

/// Writes one table block: symbol `j` of the block sits in lane `j`, matching
/// [`convert_row`]'s output; limbs 0-3 go to `tables[0]`, 4-7 to `tables[1]`.
fn fill_coefficients(tables: [&mut [u8]; 2], coefficients: &[GF128]) {
    for (j, coefficient) in coefficients.iter().enumerate() {
        for (limb, &v) in coefficient.limbs.iter().enumerate() {
            let p = to_poly(v);
            let off = (limb % 4) * OPERAND_BYTES + j;
            let table = &mut *tables[limb / 4];
            table[off] = p as u8;
            table[off + BLOCK_SYMBOLS] = (p >> 8) as u8;
            table[off + 2 * BLOCK_SYMBOLS] = p as u8 ^ (p >> 8) as u8;
        }
    }
}

#[target_feature(enable = "neon,sha3")]
fn row_rlc(row: &[u8], operands: &mut [u8], tables: &[Vec<u8>; 2]) -> GF128 {
    convert_row(row, operands);
    let acc = [
        accumulate(operands, &tables[0]),
        accumulate(operands, &tables[1]),
    ];
    GF128 {
        limbs: std::array::from_fn(|limb| {
            let parts = &acc[limb / 4][3 * (limb % 4)..];
            let lo = xor_lanes(&parts[0]);
            let hi = xor_lanes(&parts[1]);
            let mid = xor_lanes(&parts[2]) ^ lo ^ hi;
            from_poly(reduce(
                u32::from(lo) ^ u32::from(mid) << 8 ^ u32::from(hi) << 16,
            ))
        }),
    }
}

fn xor_lanes(v: &[u8; 16]) -> u16 {
    v.as_chunks::<2>()
        .0
        .iter()
        .fold(0, |x, lane| x ^ u16::from_le_bytes(*lane))
}

#[inline]
#[target_feature(enable = "neon")]
fn load(bytes: &[u8; 16]) -> uint8x16_t {
    // SAFETY: reads exactly the 16 bytes of `bytes`; unaligned loads are allowed.
    unsafe { vld1q_u8(bytes.as_ptr()) }
}

#[inline]
#[target_feature(enable = "neon")]
fn store(bytes: &mut [u8; 16], v: uint8x16_t) {
    // SAFETY: writes exactly the 16 bytes of `bytes`; unaligned stores are allowed.
    unsafe { vst1q_u8(bytes.as_mut_ptr(), v) }
}

/// Maps 16 symbols, given as low and high byte vectors, to polynomial form.
#[inline]
#[target_feature(enable = "neon")]
fn map16(lo: uint8x16_t, hi: uint8x16_t, t: &[uint8x16_t; 8]) -> (uint8x16_t, uint8x16_t) {
    let mask = vdupq_n_u8(0x0f);
    let nibbles = [
        vandq_u8(lo, mask),
        vshrq_n_u8::<4>(lo),
        vandq_u8(hi, mask),
        vshrq_n_u8::<4>(hi),
    ];
    let mut out = (vdupq_n_u8(0), vdupq_n_u8(0));
    for (n, &idx) in nibbles.iter().enumerate() {
        out.0 = veorq_u8(out.0, vqtbl1q_u8(t[n], idx));
        out.1 = veorq_u8(out.1, vqtbl1q_u8(t[4 + n], idx));
    }
    out
}

/// Converts Leopard-layout `row` to polynomial operand blocks in `dst`.
#[inline]
#[target_feature(enable = "neon")]
fn convert_row(row: &[u8], dst: &mut [u8]) {
    let t = TO_POLY_NIBBLES.map(|table| load(&table));
    let (chunks, _) = row.as_chunks::<64>();
    let (blocks, _) = dst.as_chunks_mut::<OPERAND_BYTES>();
    let (pairs, _) = blocks.as_chunks_mut::<2>();
    for (chunk, pair) in chunks.iter().zip(pairs) {
        let (v, _) = chunk.as_chunks::<16>();
        for (block, out) in pair.iter_mut().enumerate() {
            let (out, _) = out.as_chunks_mut::<16>();
            let (lo, hi) = map16(load(&v[block]), load(&v[2 + block]), &t);
            store(&mut out[0], lo);
            store(&mut out[1], hi);
            store(&mut out[2], veorq_u8(lo, hi));
        }
    }
}

/// XORs, for four limbs, the Karatsuba parts of the carry-less products of
/// `operands` with `table` into 12 accumulators of eight 16-bit lanes. Lane
/// `i` folds symbols `i` and `i + 8` of each block.
#[inline]
#[target_feature(enable = "neon,sha3")]
fn accumulate(operands: &[u8], table: &[u8]) -> [[u8; 16]; 12] {
    let mut acc = [vdupq_n_u8(0); 12];
    let (operands, _) = operands.as_chunks::<OPERAND_BYTES>();
    let (table, _) = table.as_chunks::<COEFF_BYTES>();
    for (a, b) in operands.iter().zip(table) {
        let (a, _) = a.as_chunks::<16>();
        let a = [load(&a[0]), load(&a[1]), load(&a[2])];
        let (b, _) = b.as_chunks::<16>();
        for (idx, acc) in acc.iter_mut().enumerate() {
            let a = vreinterpretq_p8_u8(a[idx % 3]);
            let b = vreinterpretq_p8_u8(load(&b[idx]));
            let low = vmull_p8(vget_low_p8(b), vget_low_p8(a));
            let high = vmull_high_p8(b, a);
            *acc = veor3q_u8(vreinterpretq_u8_p16(low), vreinterpretq_u8_p16(high), *acc);
        }
    }
    let mut out = [[0; 16]; 12];
    for (out, &acc) in out.iter_mut().zip(&acc) {
        store(out, acc);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::symbols::compute_rlc;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn clmul16(a: u16, b: u16) -> u32 {
        (0..16)
            .filter(|i| b >> i & 1 == 1)
            .fold(0, |p, i| p ^ u32::from(a) << i)
    }

    #[test]
    fn basis_roundtrip_all_elements() {
        for v in 0..=u16::MAX {
            assert_eq!(from_poly(to_poly(v)), v);
        }
    }

    #[test]
    fn reduced_products_match_leopard_mul() {
        let mut rng = ChaCha8Rng::seed_from_u64(3);
        let fixed = [0, 1, u16::MAX].into_iter().chain(CANTOR_BASIS);
        let fixed: Vec<u16> = fixed.chain((0..16).map(|i| 1 << i)).collect();
        for a in 0..=u16::MAX {
            let seeded = (0..4).map(|_| rng.gen::<u16>());
            for b in fixed.iter().copied().chain(seeded) {
                let expected = GF128 { limbs: [b; 8] }.scalar_mul(a).limbs[0];
                let actual = from_poly(reduce(clmul16(to_poly(a), to_poly(b))));
                assert_eq!(actual, expected, "a={a:#06x}, b={b:#06x}");
            }
        }
    }

    #[test]
    fn matches_scalar_reference() {
        if !available() {
            eprintln!("skipping: neon+sha3 unavailable");
            return;
        }
        let mut rng = ChaCha8Rng::seed_from_u64(11);
        for (k, row_size) in [
            (1, 64),
            (3, 128),
            (17, 2112),
            (33, 4096),
            (100, 64),
            (2, 131072),
        ] {
            let mut data = vec![0u8; k * row_size + 1];
            rng.fill(&mut data[..]);
            let coefficients: Vec<GF128> = (0..row_size / 2)
                .map(|i| GF128 {
                    limbs: std::array::from_fn(|l| if (i + l) % 5 == 0 { 0 } else { rng.gen() }),
                })
                .collect();
            // Unaligned subslice.
            let data = &data[1..];
            let expected: Vec<_> = data
                .chunks_exact(row_size)
                .map(|row| compute_rlc(row, &coefficients))
                .collect();
            assert_eq!(
                compute_rlcs(data, row_size, &coefficients),
                expected,
                "k={k}, row_size={row_size}"
            );
        }
    }
}
