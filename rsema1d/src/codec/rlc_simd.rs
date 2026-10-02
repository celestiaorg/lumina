#[cfg(target_arch = "x86")]
use core::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;
use reed_solomon_simd::engine::tables::{get_exp_log, Mul128};

pub(super) struct Avx2Rlc {
    tables: &'static Mul128,
}

impl Avx2Rlc {
    pub(super) fn new() -> Option<Self> {
        std::is_x86_feature_detected!("avx2").then(|| Self {
            tables: reed_solomon_simd::engine::tables::get_mul128(),
        })
    }

    pub(super) fn mul_add8(
        &self,
        input: &[[u8; 64]],
        outputs: &mut [&mut [[u8; 64]]; 8],
        coefficients: &[u16; 8],
    ) {
        // SAFETY: construction requires AVX2, and the kernel checks output lengths.
        unsafe { mul_add8(self.tables, input, outputs, coefficients) }
    }
}

#[target_feature(enable = "avx2")]
unsafe fn mul_add8(
    tables: &Mul128,
    input: &[[u8; 64]],
    outputs: &mut [&mut [[u8; 64]]; 8],
    coefficients: &[u16; 8],
) {
    for output in outputs.iter() {
        assert_eq!(input.len(), output.len());
    }
    let log = &get_exp_log().log;
    let luts = coefficients.map(|c| (c != 0).then(|| &tables[log[c as usize] as usize]));
    let mask = _mm256_set1_epi8(15);
    for (block, source) in input.iter().enumerate() {
        // SAFETY: AVX2 is required by this function. Each source and checked output
        // block contains 64 bytes; all vector accesses are unaligned and in bounds.
        unsafe {
            let lo = _mm256_loadu_si256(source.as_ptr().cast());
            let hi = _mm256_loadu_si256(source.as_ptr().add(32).cast());
            let nibbles = [
                _mm256_and_si256(lo, mask),
                _mm256_and_si256(_mm256_srli_epi64(lo, 4), mask),
                _mm256_and_si256(hi, mask),
                _mm256_and_si256(_mm256_srli_epi64(hi, 4), mask),
            ];
            for (output, lut) in outputs.iter_mut().zip(luts) {
                let Some(lut) = lut else { continue };
                let dst = output[block].as_mut_ptr();
                let mut lo = _mm256_loadu_si256(dst.cast());
                let mut hi = _mm256_loadu_si256(dst.add(32).cast());
                for (i, nibble) in nibbles.into_iter().enumerate() {
                    let table_lo = _mm256_broadcastsi128_si256(_mm_loadu_si128(
                        core::ptr::from_ref(&lut.lo[i]).cast(),
                    ));
                    let table_hi = _mm256_broadcastsi128_si256(_mm_loadu_si128(
                        core::ptr::from_ref(&lut.hi[i]).cast(),
                    ));
                    lo = _mm256_xor_si256(lo, _mm256_shuffle_epi8(table_lo, nibble));
                    hi = _mm256_xor_si256(hi, _mm256_shuffle_epi8(table_hi, nibble));
                }
                _mm256_storeu_si256(dst.cast(), lo);
                _mm256_storeu_si256(dst.add(32).cast(), hi);
            }
        }
    }
}
