use crate::field::GF128;
use sha2::{Digest, Sha256};

/// Hash data with SHA-256
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
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

/// Derive RLC coefficients from row root
pub fn derive_coefficients(row_root: &[u8; 32], num_symbols: usize) -> Vec<GF128> {
    let seed = sha256(row_root);

    let mut coeffs = Vec::with_capacity(num_symbols);
    for i in 0..num_symbols {
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&(i as u32).to_le_bytes());
        let hash: [u8; 32] = hasher.finalize().into();
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
    fn test_hash_to_gf128() {
        let hash = [0u8; 32];
        let gf = hash_to_gf128(&hash);
        assert_eq!(gf, GF128::zero());
    }

    #[test]
    fn test_derive_coefficients() {
        let row_root = [0u8; 32];
        let coeffs = derive_coefficients(&row_root, 10);
        assert_eq!(coeffs.len(), 10);
        // Coefficients should be deterministic
        let coeffs2 = derive_coefficients(&row_root, 10);
        assert_eq!(coeffs, coeffs2);
    }
}
