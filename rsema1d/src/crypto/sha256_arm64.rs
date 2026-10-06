use core::arch::aarch64::*;

pub(super) fn hash_leaf_pair(a: &[u8], b: &[u8]) -> Option<[[u8; 32]; 2]> {
    if a.len() != b.len() || a.len() < 63 || !std::arch::is_aarch64_feature_detected!("sha2") {
        return None;
    }
    // SAFETY: SHA-2 support and the input requirements were checked above.
    Some(unsafe { hash_pair(a, b) })
}

const IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// # Safety
/// Requires SHA-2 support and equal input lengths of at least 63 bytes.
#[target_feature(enable = "sha2")]
unsafe fn hash_pair(a: &[u8], b: &[u8]) -> [[u8; 32]; 2] {
    let mut states = [IV; 2];
    let mut blocks = [[0u8; 128]; 2];
    blocks[0][1..64].copy_from_slice(&a[..63]);
    blocks[1][1..64].copy_from_slice(&b[..63]);
    // SAFETY: This function requires SHA-2 support; all compression inputs are full blocks.
    unsafe { compress(&mut states, &blocks[0][..64], &blocks[1][..64]) };

    let full = (a.len() - 63) / 64 * 64;
    unsafe { compress(&mut states, &a[63..63 + full], &b[63..63 + full]) };

    blocks = [[0; 128]; 2];
    let tail_len = a.len() - 63 - full;
    let padded_len = if tail_len < 56 { 64 } else { 128 };
    let bits = ((a.len() as u64 + 1) * 8).to_be_bytes();
    for (block, data) in blocks.iter_mut().zip([a, b]) {
        block[..tail_len].copy_from_slice(&data[63 + full..]);
        block[tail_len] = 0x80;
        block[padded_len - 8..padded_len].copy_from_slice(&bits);
    }
    unsafe {
        compress(
            &mut states,
            &blocks[0][..padded_len],
            &blocks[1][..padded_len],
        )
    };
    states.map(|state| {
        let mut digest = [0; 32];
        for (bytes, word) in digest.as_chunks_mut::<4>().0.iter_mut().zip(state) {
            bytes.copy_from_slice(&word.to_be_bytes());
        }
        digest
    })
}

/// # Safety
/// Requires SHA-2 support. Inputs contain equal numbers of complete 64-byte blocks.
#[target_feature(enable = "sha2")]
unsafe fn compress(states: &mut [[u32; 8]; 2], a: &[u8], b: &[u8]) {
    // SAFETY: Each state contains eight words, each chunk contains 64 bytes, and every
    // constant load stays within K. NEON loads accept unaligned input addresses.
    unsafe {
        let mut abcd_a = vld1q_u32(states[0].as_ptr());
        let mut efgh_a = vld1q_u32(states[0][4..].as_ptr());
        let mut abcd_b = vld1q_u32(states[1].as_ptr());
        let mut efgh_b = vld1q_u32(states[1][4..].as_ptr());

        for (a, b) in a.as_chunks::<64>().0.iter().zip(b.as_chunks::<64>().0) {
            let saved = [abcd_a, efgh_a, abcd_b, efgh_b];
            let mut wa = [0, 16, 32, 48]
                .map(|i| vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(a[i..].as_ptr()))));
            let mut wb = [0, 16, 32, 48]
                .map(|i| vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(b[i..].as_ptr()))));

            // Unroll four rounds at a time to keep both schedules in registers.
            macro_rules! rounds {
                ($r:literal) => {{
                    let i = $r % 4;
                    if $r >= 4 {
                        wa[i] = vsha256su1q_u32(
                            vsha256su0q_u32(wa[i], wa[(i + 1) % 4]),
                            wa[(i + 2) % 4],
                            wa[(i + 3) % 4],
                        );
                        wb[i] = vsha256su1q_u32(
                            vsha256su0q_u32(wb[i], wb[(i + 1) % 4]),
                            wb[(i + 2) % 4],
                            wb[(i + 3) % 4],
                        );
                    }
                    let k = vld1q_u32(K[$r * 4..].as_ptr());
                    let ta = vaddq_u32(wa[i], k);
                    let tb = vaddq_u32(wb[i], k);
                    let prev_a = abcd_a;
                    let prev_b = abcd_b;
                    abcd_a = vsha256hq_u32(abcd_a, efgh_a, ta);
                    abcd_b = vsha256hq_u32(abcd_b, efgh_b, tb);
                    efgh_a = vsha256h2q_u32(efgh_a, prev_a, ta);
                    efgh_b = vsha256h2q_u32(efgh_b, prev_b, tb);
                }};
            }
            rounds!(0);
            rounds!(1);
            rounds!(2);
            rounds!(3);
            rounds!(4);
            rounds!(5);
            rounds!(6);
            rounds!(7);
            rounds!(8);
            rounds!(9);
            rounds!(10);
            rounds!(11);
            rounds!(12);
            rounds!(13);
            rounds!(14);
            rounds!(15);

            abcd_a = vaddq_u32(abcd_a, saved[0]);
            efgh_a = vaddq_u32(efgh_a, saved[1]);
            abcd_b = vaddq_u32(abcd_b, saved[2]);
            efgh_b = vaddq_u32(efgh_b, saved[3]);
        }
        vst1q_u32(states[0].as_mut_ptr(), abcd_a);
        vst1q_u32(states[0][4..].as_mut_ptr(), efgh_a);
        vst1q_u32(states[1].as_mut_ptr(), abcd_b);
        vst1q_u32(states[1][4..].as_mut_ptr(), efgh_b);
    }
}
