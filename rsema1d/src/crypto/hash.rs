use crate::field::GF128;
use sha2::{Digest, Sha256};

/// Hash data with SHA-256
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Hash two 32-byte values without allocating a temporary concatenation.
pub(crate) fn sha256_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Convert 32-byte hash to GF128 by XORing first and second halves
pub fn hash_to_gf128(hash: &[u8; 32]) -> GF128 {
    let mut limbs = [0u16; 8];

    for i in 0..8 {
        let low = u16::from_le_bytes([hash[i * 2], hash[i * 2 + 1]]);
        let high = u16::from_le_bytes([hash[16 + i * 2], hash[16 + i * 2 + 1]]);
        limbs[i] = low ^ high;
    }

    GF128 { limbs }
}

/// Derive RLC coefficients bound to `(row_root, k, n, row_size)`.
///
/// The seed is `sha256(row_root || k || n || row_size)` with the parameters
/// as little-endian u32, matching `rlc.DeriveCoefficients` in celestia-app.
pub fn derive_coefficients(row_root: &[u8; 32], k: usize, n: usize, row_size: usize) -> Vec<GF128> {
    let num_symbols = row_size / 2;

    let mut hasher = Sha256::new();
    hasher.update(row_root);
    let mut params = [0u8; 12];
    params[0..4].copy_from_slice(&(k as u32).to_le_bytes());
    params[4..8].copy_from_slice(&(n as u32).to_le_bytes());
    params[8..12].copy_from_slice(&(row_size as u32).to_le_bytes());
    hasher.update(params);
    let seed: [u8; 32] = hasher.finalize().into();
    let mut coeffs = Vec::with_capacity(num_symbols);
    let mut buf = [0u8; 32 + 4];
    buf[..32].copy_from_slice(&seed);

    for i in 0..num_symbols {
        buf[32..].copy_from_slice(&(i as u32).to_le_bytes());
        let hash: [u8; 32] = Sha256::digest(buf).into();
        coeffs.push(hash_to_gf128(&hash));
    }
    coeffs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256() {
        let data = b"hello world";
        let hash = sha256(data);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn sha256_pair_matches_concatenation() {
        let left = [1u8; 32];
        let right = [2u8; 32];
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&left);
        combined[32..].copy_from_slice(&right);
        assert_eq!(sha256_pair(&left, &right), sha256(&combined));
    }

    #[test]
    fn test_hash_to_gf128() {
        let hash = [0u8; 32];
        let gf = hash_to_gf128(&hash);
        assert_eq!(gf, GF128::zero());
    }

    #[test]
    fn test_derive_coefficients() {
        let row_root = [0u8; 32];
        let coeffs = derive_coefficients(&row_root, 4, 4, 64);
        assert_eq!(coeffs.len(), 32);
        // Coefficients should be deterministic
        let coeffs2 = derive_coefficients(&row_root, 4, 4, 64);
        assert_eq!(coeffs, coeffs2);
        // ...and domain-separated by (k, n, row_size)
        let coeffs3 = derive_coefficients(&row_root, 8, 8, 64);
        assert_ne!(coeffs, coeffs3);
    }
}
