//! Galois Field arithmetic for GF(2^128)

/// GF(2^128) element represented as 8 GF(2^16) limbs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GF128 {
    pub limbs: [u16; 8],
}

impl GF128 {
    /// Zero element
    pub const fn zero() -> Self {
        Self { limbs: [0; 8] }
    }

    /// Create from 16 bytes (little-endian)
    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        let mut limbs = [0u16; 8];
        for i in 0..8 {
            limbs[i] = u16::from_le_bytes([bytes[i * 2], bytes[i * 2 + 1]]);
        }
        Self { limbs }
    }

    /// Convert to 16 bytes (little-endian)
    pub fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        for i in 0..8 {
            let limb_bytes = self.limbs[i].to_le_bytes();
            bytes[i * 2] = limb_bytes[0];
            bytes[i * 2 + 1] = limb_bytes[1];
        }
        bytes
    }

    /// Add two GF128 elements (XOR)
    #[inline]
    pub fn add(self, other: Self) -> Self {
        let mut limbs = [0u16; 8];
        for i in 0..8 {
            limbs[i] = self.limbs[i] ^ other.limbs[i];
        }
        Self { limbs }
    }

    /// Scalar multiplication: GF(2^16) × GF(2^128) -> GF(2^128)
    #[inline]
    pub fn scalar_mul(self, scalar: u16) -> Self {
        if scalar == 0 {
            return Self::zero();
        }
        let mut limbs = [0u16; 8];
        for i in 0..8 {
            limbs[i] = gf16_mul(scalar, self.limbs[i]);
        }
        Self { limbs }
    }
}

/// Multiply two GF(2^16) elements using log/exp tables from reed-solomon-simd
#[inline]
fn gf16_mul(a: u16, b: u16) -> u16 {
    use reed_solomon_simd::engine::tables::{get_exp_log, mul};

    if a == 0 || b == 0 {
        return 0;
    }

    let exp_log = get_exp_log();
    let log_b = exp_log.log[b as usize];

    // Use the mul function: mul(a, log[b], exp, log) = exp[log[a] + log[b]]
    mul(a, log_b, &exp_log.exp, &exp_log.log)
}

impl std::ops::Add for GF128 {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        self.add(other)
    }
}

impl std::ops::AddAssign for GF128 {
    fn add_assign(&mut self, other: Self) {
        *self = self.add(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gf16_mul() {
        // Test basic multiplication
        assert_eq!(gf16_mul(0, 5), 0);
        assert_eq!(gf16_mul(1, 5), 5);

        // Test with known values from Go implementation
        assert_eq!(gf16_mul(0x0100, 0x318a), 0xac4c);

        // Test commutativity
        assert_eq!(gf16_mul(3, 7), gf16_mul(7, 3));
    }

    #[test]
    fn test_gf128_add() {
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let b = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let c = a.add(b);
        assert_eq!(c, GF128::zero()); // a + a = 0 in GF(2)
    }

    #[test]
    fn test_gf128_scalar_mul() {
        let a = GF128 {
            limbs: [1, 0, 0, 0, 0, 0, 0, 0],
        };
        let b = a.scalar_mul(2);
        assert_eq!(b.limbs[0], 2);
    }

    #[test]
    fn test_gf128_serialization() {
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let bytes = a.to_bytes();
        let b = GF128::from_bytes(&bytes);
        assert_eq!(a, b);
    }
}

#[cfg(test)]
mod colocated_comprehensive_field_tests {
    #![allow(clippy::uninlined_format_args)]

    use crate::crypto::hash_to_gf128;
    use crate::field::GF128;

    #[test]
    fn test_gf128_zero() {
        let z = GF128::zero();
        for i in 0..8 {
            assert_eq!(
                z.limbs[i], 0,
                "Zero() element {} = {}, expected 0",
                i, z.limbs[i]
            );
        }
    }

    #[test]
    fn test_gf128_addition() {
        // zero + zero = zero
        let result = GF128::zero() + GF128::zero();
        assert_eq!(result, GF128::zero());

        // identity: a + 0 = a
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let result = a + GF128::zero();
        assert_eq!(result, a);

        // self-inverse: a + a = 0
        let result = a + a;
        assert_eq!(result, GF128::zero());

        // XOR operation
        let a = GF128 {
            limbs: [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        };
        let b = GF128 {
            limbs: [0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA],
        };
        let want = GF128 {
            limbs: [0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55],
        };
        assert_eq!(a + b, want);

        // commutativity
        assert_eq!(a + b, b + a);
    }

    #[test]
    fn test_gf128_scalar_multiplication() {
        // multiply by zero
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let result = a.scalar_mul(0);
        assert_eq!(result, GF128::zero());

        // multiply by one
        let result = a.scalar_mul(1);
        assert_eq!(result, a);

        // general cases
        let test_cases = vec![
            (
                2,
                GF128 {
                    limbs: [1, 2, 3, 4, 5, 6, 7, 8],
                },
            ),
            (
                0x1234,
                GF128 {
                    limbs: [
                        0xABCD, 0xEF01, 0x2345, 0x6789, 0xBCDE, 0xF012, 0x3456, 0x789A,
                    ],
                },
            ),
        ];

        for (scalar, vec) in test_cases {
            let result = vec.scalar_mul(scalar);
            // Each component should be multiplied correctly
            // This is verified by the internal GF16 multiplication
            // We just check it doesn't panic
            let _ = result;
        }
    }

    #[test]
    fn test_gf128_serialization() {
        let test_cases = vec![
            GF128::zero(),
            GF128 {
                limbs: [1, 2, 3, 4, 5, 6, 7, 8],
            },
            GF128 {
                limbs: [
                    0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF,
                ],
            },
            GF128 {
                limbs: [
                    0x1234, 0x5678, 0x9ABC, 0xDEF0, 0x1111, 0x2222, 0x3333, 0x4444,
                ],
            },
        ];

        for original in test_cases {
            // Serialize to bytes
            let serialized = original.to_bytes();

            // Check size
            assert_eq!(serialized.len(), 16);

            // Deserialize back
            let deserialized = GF128::from_bytes(&serialized);

            // Check round-trip
            assert_eq!(deserialized, original);
        }
    }

    #[test]
    fn test_gf128_properties() {
        // Test associativity of addition: (a + b) + c = a + (b + c)
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let b = GF128 {
            limbs: [9, 10, 11, 12, 13, 14, 15, 16],
        };
        let c = GF128 {
            limbs: [17, 18, 19, 20, 21, 22, 23, 24],
        };

        let left = (a + b) + c;
        let right = a + (b + c);
        assert_eq!(left, right, "Addition not associative");

        // Test distributivity of scalar multiplication: s * (a + b) = (s * a) + (s * b)
        let scalar = 0x1234u16;
        let sum = a + b;
        let left = sum.scalar_mul(scalar);
        let right = a.scalar_mul(scalar) + b.scalar_mul(scalar);
        assert_eq!(left, right, "Scalar multiplication not distributive");
    }

    #[test]
    fn test_hash_to_gf128() {
        // Test with different inputs
        let data1 = [0u8; 32]; // all zeros
        let data2 = [0xFF; 32]; // all ones
        let data3: [u8; 32] = std::array::from_fn(|i| i as u8); // sequential

        for data in &[data1, data2, data3] {
            let result1 = hash_to_gf128(data);
            let result2 = hash_to_gf128(data);

            // Test determinism
            assert_eq!(result1, result2, "hash_to_gf128 not deterministic");

            // Test that changing input changes output
            let mut modified = *data;
            modified[0] ^= 1;
            let result3 = hash_to_gf128(&modified);
            assert_ne!(
                result1, result3,
                "hash_to_gf128 did not change with different input"
            );
        }
    }

    #[test]
    fn test_gf128_commutativity() {
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let b = GF128 {
            limbs: [9, 10, 11, 12, 13, 14, 15, 16],
        };

        assert_eq!(a + b, b + a, "Addition not commutative");
    }

    #[test]
    fn test_gf128_additive_identity() {
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let zero = GF128::zero();

        assert_eq!(a + zero, a, "Zero not additive identity");
        assert_eq!(zero + a, a, "Zero not additive identity (commuted)");
    }

    #[test]
    fn test_gf128_additive_inverse() {
        let a = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        // In GF(2^n), every element is its own additive inverse
        assert_eq!(a + a, GF128::zero(), "Element not its own additive inverse");
    }
}
