//! Cryptographic utilities for hashing and Merkle trees

mod hash;
mod merkle;
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[allow(unsafe_code)]
mod sha256_arm64;

pub(crate) use hash::sha256_pair;
pub use hash::{derive_coefficients, hash_to_gf128, sha256};
pub(crate) use merkle::hash_leaf_pair;
pub use merkle::{hash_internal, hash_leaf, verify_proof, MerkleTree};

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_hash_leaf_pair(a: &[u8], b: &[u8]) -> [[u8; 32]; 2] {
    merkle::hash_leaf_pair(a, b)
}
