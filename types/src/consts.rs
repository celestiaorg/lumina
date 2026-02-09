//! Constants used within celestia ecosystem.

/// The size of the SHA256 hash.
pub const HASH_SIZE: usize = tendermint::hash::SHA256_HASH_SIZE;

// celestia-core/types/genesis
/// Constants related to genesis definition.
pub mod genesis {
    /// Max length of the chain ID.
    pub const MAX_CHAIN_ID_LEN: usize = 50;
}

// celestia-core/version/version
/// Constants related to the protocol versions.
pub mod version {
    /// Version of all the block data structures and processing.
    ///
    /// This includes validity of blocks and state updates.
    pub const BLOCK_PROTOCOL: u64 = 11;
}

/// Constants defined in [`celestia-app`] consensus nodes.
///
/// [`celestia-app`]: https://github.com/celestiaorg/celestia-app
pub mod appconsts {
    #[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
    use wasm_bindgen::prelude::*;

    pub use global_consts::*;

    // https://github.com/celestiaorg/celestia-app/blob/v1.28.0/pkg/appconsts/v1/app_consts.go
    /// Consts of App v1.
    pub mod v1 {
        /// App version.
        pub const VERSION: u64 = 1;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 128;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
    }

    // https://github.com/celestiaorg/celestia-app/blob/v2.3.1/pkg/appconsts/v2/app_consts.go
    /// Consts of App v2.
    pub mod v2 {
        /// App version.
        pub const VERSION: u64 = 2;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 128;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
    }

    // https://github.com/celestiaorg/celestia-app/blob/v3.10.6/pkg/appconsts/v3/app_consts.go
    /// Consts of App v3.
    pub mod v3 {
        /// App version.
        pub const VERSION: u64 = 3;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 128;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
        /// Cost of each byte in a transaction (in units of gas).
        pub const TX_SIZE_COST_PER_BYTE: u64 = 10;
        /// Cost of each byte in blob (in units of gas).
        pub const GAS_PER_BLOB_BYTE: u64 = 8;
        /// Maximum size of the transaction (in bytes).
        pub const MAX_TX_SIZE: u64 = 2_097_152; // 2MB
    }

    // https://github.com/celestiaorg/celestia-app/blob/v4.1.0/pkg/appconsts/v4/app_consts.go
    /// Consts of App v4.
    pub mod v4 {
        /// App version.
        pub const VERSION: u64 = 4;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 128;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
        /// Cost of each byte in a transaction (in units of gas).
        pub const TX_SIZE_COST_PER_BYTE: u64 = 10;
        /// Cost of each byte in blob (in units of gas).
        pub const GAS_PER_BLOB_BYTE: u64 = 8;
        /// Maximum size of the transaction (in bytes).
        pub const MAX_TX_SIZE: u64 = 2_097_152; // 2MB
    }

    // https://github.com/celestiaorg/celestia-app/blob/v5.0.2/pkg/appconsts/app_consts.go
    /// Consts of App v5.
    pub mod v5 {
        /// App version.
        pub const VERSION: u64 = 5;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 128;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
        /// Cost of each byte in a transaction (in units of gas).
        pub const TX_SIZE_COST_PER_BYTE: u64 = 10;
        /// Cost of each byte in blob (in units of gas).
        pub const GAS_PER_BLOB_BYTE: u64 = 8;
        /// Maximum size of the transaction (in bytes).
        pub const MAX_TX_SIZE: u64 = 2_097_152; // 2MB
    }

    // https://github.com/celestiaorg/celestia-app/blob/v6.0.0-rc0/pkg/appconsts/app_consts.go
    /// Consts of App v6.
    pub mod v6 {
        /// App version.
        pub const VERSION: u64 = 6;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 512;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
        /// Cost of each byte in a transaction (in units of gas).
        pub const TX_SIZE_COST_PER_BYTE: u64 = 10;
        /// Cost of each byte in blob (in units of gas).
        pub const GAS_PER_BLOB_BYTE: u64 = 8;
        /// Maximum size of the transaction (in bytes).
        pub const MAX_TX_SIZE: u64 = 8_388_608; // 8MB
    }

    // https://github.com/celestiaorg/celestia-app/blob/v7.0.0-rc0/pkg/appconsts/app_consts.go
    /// Consts of App v7.
    pub mod v7 {
        /// App version.
        pub const VERSION: u64 = 7;
        /// Maximum width of the original data square.
        pub const SQUARE_SIZE_UPPER_BOUND: usize = 512;
        /// Maximum width of a single subtree root when generating blob's commitment.
        pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;
        /// Cost of each byte in a transaction (in units of gas).
        pub const TX_SIZE_COST_PER_BYTE: u64 = 10;
        /// Cost of each byte in blob (in units of gas).
        pub const GAS_PER_BLOB_BYTE: u64 = 8;
        /// Maximum size of the transaction (in bytes).
        pub const MAX_TX_SIZE: u64 = 8_388_608; // 8MB
    }

    // celestia-app/pkg/appconsts/versioned_consts.go
    /// Latest App version.
    pub const LATEST_VERSION: u64 = v7::VERSION;

    /// Enum with all valid App versions.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    #[repr(u64)]
    #[cfg(not(feature = "uniffi"))]
    pub enum AppVersion {
        /// App v1
        V1 = v1::VERSION,
        /// App v2
        V2 = v2::VERSION,
        /// App v3
        V3 = v3::VERSION,
        /// App v4
        V4 = v4::VERSION,
        /// App v5
        V5 = v5::VERSION,
        /// App v6
        V6 = v6::VERSION,
        /// App v7
        V7 = v7::VERSION,
    }

    /// Enum with all valid App versions.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, uniffi::Enum)]
    #[repr(u64)]
    #[cfg(feature = "uniffi")] // uniffi supports only literal initialisers
    pub enum AppVersion {
        /// App v1
        V1 = 1,
        /// App v2
        V2 = 2,
        /// App v3
        V3 = 3,
        /// App v4
        V4 = 4,
        /// App v5
        V5 = 5,
        /// App v6
        V6 = 6,
        /// App v7
        V7 = 7,
    }

    impl AppVersion {
        /// Latest App version variant.
        pub fn latest() -> AppVersion {
            AppVersion::from_u64(LATEST_VERSION).expect("Unknown app version")
        }

        /// Creates `AppVersion` from a numeric value.
        pub fn from_u64(version: u64) -> Option<AppVersion> {
            match version {
                v1::VERSION => Some(AppVersion::V1),
                v2::VERSION => Some(AppVersion::V2),
                v3::VERSION => Some(AppVersion::V3),
                v4::VERSION => Some(AppVersion::V4),
                v5::VERSION => Some(AppVersion::V5),
                v6::VERSION => Some(AppVersion::V6),
                v7::VERSION => Some(AppVersion::V7),
                _ => None,
            }
        }

        /// Returns the numeric value of App version.
        pub fn as_u64(&self) -> u64 {
            *self as u64
        }
    }

    // wasm-bindgen duplicates classes for `impl` blocks for enums
    // so we can't export enum with additional methods
    // https://github.com/rustwasm/wasm-bindgen/issues/1715
    /// Version of the App
    #[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    #[wasm_bindgen(js_name = AppVersion)]
    pub struct JsAppVersion(AppVersion);

    #[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
    #[wasm_bindgen(js_class = AppVersion)]
    impl JsAppVersion {
        /// App v1
        #[wasm_bindgen(js_name = V1, getter)]
        pub fn v1() -> JsAppVersion {
            JsAppVersion(AppVersion::V1)
        }

        /// App v2
        #[wasm_bindgen(js_name = V2, getter)]
        pub fn v2() -> JsAppVersion {
            JsAppVersion(AppVersion::V2)
        }

        /// App v3
        #[wasm_bindgen(js_name = V3, getter)]
        pub fn v3() -> JsAppVersion {
            JsAppVersion(AppVersion::V3)
        }

        /// App v4
        #[wasm_bindgen(js_name = V4, getter)]
        pub fn v4() -> JsAppVersion {
            JsAppVersion(AppVersion::V4)
        }

        /// App v5
        #[wasm_bindgen(js_name = V5, getter)]
        pub fn v5() -> JsAppVersion {
            JsAppVersion(AppVersion::V5)
        }

        /// App v6
        #[wasm_bindgen(js_name = V6, getter)]
        pub fn v6() -> JsAppVersion {
            JsAppVersion(AppVersion::V6)
        }

        /// App v7
        #[wasm_bindgen(js_name = V7, getter)]
        pub fn v7() -> JsAppVersion {
            JsAppVersion(AppVersion::V7)
        }

        /// Latest App version variant.
        pub fn latest() -> JsAppVersion {
            AppVersion::latest().into()
        }
    }

    #[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
    impl From<JsAppVersion> for AppVersion {
        fn from(value: JsAppVersion) -> AppVersion {
            value.0
        }
    }

    #[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
    impl From<AppVersion> for JsAppVersion {
        fn from(value: AppVersion) -> JsAppVersion {
            JsAppVersion(value)
        }
    }

    /// Maximum width of the original data square.
    pub const fn square_size_upper_bound(app_version: AppVersion) -> usize {
        match app_version {
            AppVersion::V1 => v1::SQUARE_SIZE_UPPER_BOUND,
            AppVersion::V2 => v2::SQUARE_SIZE_UPPER_BOUND,
            AppVersion::V3 => v3::SQUARE_SIZE_UPPER_BOUND,
            AppVersion::V4 => v4::SQUARE_SIZE_UPPER_BOUND,
            AppVersion::V5 => v5::SQUARE_SIZE_UPPER_BOUND,
            AppVersion::V6 => v6::SQUARE_SIZE_UPPER_BOUND,
            AppVersion::V7 => v7::SQUARE_SIZE_UPPER_BOUND,
        }
    }

    /// Maximum width of a single subtree root when generating blob's commitment.
    pub const fn subtree_root_threshold(app_version: AppVersion) -> u64 {
        match app_version {
            AppVersion::V1 => v1::SUBTREE_ROOT_THRESHOLD,
            AppVersion::V2 => v2::SUBTREE_ROOT_THRESHOLD,
            AppVersion::V3 => v3::SUBTREE_ROOT_THRESHOLD,
            AppVersion::V4 => v4::SUBTREE_ROOT_THRESHOLD,
            AppVersion::V5 => v5::SUBTREE_ROOT_THRESHOLD,
            AppVersion::V6 => v6::SUBTREE_ROOT_THRESHOLD,
            AppVersion::V7 => v7::SUBTREE_ROOT_THRESHOLD,
        }
    }

    /// Cost of each byte in a transaction (in units of gas).
    pub const fn tx_size_cost_per_byte(app_version: AppVersion) -> u64 {
        // v1 and v2 don't have this constant because it was taken from cosmos-sdk before.
        // The value was the same as in v3 tho, so fall back to it.
        match app_version {
            AppVersion::V1 | AppVersion::V2 | AppVersion::V3 => v3::TX_SIZE_COST_PER_BYTE,
            AppVersion::V4 => v4::TX_SIZE_COST_PER_BYTE,
            AppVersion::V5 => v5::TX_SIZE_COST_PER_BYTE,
            AppVersion::V6 => v6::TX_SIZE_COST_PER_BYTE,
            AppVersion::V7 => v7::TX_SIZE_COST_PER_BYTE,
        }
    }

    /// Cost of each byte in blob (in units of gas).
    pub const fn gas_per_blob_byte(app_version: AppVersion) -> u64 {
        // In v1 and v2 this const was in appconsts/initial_consts.go rather than being versioned.
        // The value was the same as in v3 tho, so fall back to it.
        match app_version {
            AppVersion::V1 | AppVersion::V2 | AppVersion::V3 => v3::GAS_PER_BLOB_BYTE,
            AppVersion::V4 => v4::GAS_PER_BLOB_BYTE,
            AppVersion::V5 => v5::GAS_PER_BLOB_BYTE,
            AppVersion::V6 => v6::GAS_PER_BLOB_BYTE,
            AppVersion::V7 => v7::GAS_PER_BLOB_BYTE,
        }
    }

    /// Maximum size of the transaction (in bytes).
    pub const fn max_tx_size(app_version: AppVersion) -> u64 {
        match app_version {
            // In v1 and v2 the limit for transaction was the size of the ODS.
            // See: https://github.com/celestiaorg/celestia-app/issues/3686
            AppVersion::V1 | AppVersion::V2 => {
                square_size_upper_bound(app_version) as u64 * SHARE_SIZE as u64
            }
            AppVersion::V3 => v3::MAX_TX_SIZE,
            AppVersion::V4 => v4::MAX_TX_SIZE,
            AppVersion::V5 => v5::MAX_TX_SIZE,
            AppVersion::V6 => v6::MAX_TX_SIZE,
            AppVersion::V7 => v7::MAX_TX_SIZE,
        }
    }

    // celestia-app/pkg/appconsts/global_consts
    mod global_consts {
        use crate::nmt::NS_SIZE;

        /// The size of the namespace.
        pub const NAMESPACE_SIZE: usize = NS_SIZE;

        /// The size of a share in bytes.
        pub const SHARE_SIZE: usize = 512;

        /// The number of bytes reserved for the share metadata.
        ///
        /// The info byte contains the share version and a sequence start indicator.
        pub const SHARE_INFO_BYTES: usize = 1;

        /// The number of bytes reserved for the sequence length in a share.
        /// It is present only in the first share of a sequence.
        pub const SEQUENCE_LEN_BYTES: usize = 4;

        /// The first share version format.
        pub const SHARE_VERSION_ZERO: u8 = 0;

        /// The second share version format.
        pub const SHARE_VERSION_ONE: u8 = 1;

        /// The number of bytes reserved for the location of the first unit (transaction, ISR) in a compact share.
        pub const COMPACT_SHARE_RESERVED_BYTES: usize = 4;

        /// The number of bytes usable for data in the first compact share of a sequence.
        pub const FIRST_COMPACT_SHARE_CONTENT_SIZE: usize = SHARE_SIZE
            - NAMESPACE_SIZE
            - SHARE_INFO_BYTES
            - SEQUENCE_LEN_BYTES
            - COMPACT_SHARE_RESERVED_BYTES;

        /// The number of bytes usable for data in a continuation compact share of a sequence.
        pub const CONTINUATION_COMPACT_SHARE_CONTENT_SIZE: usize =
            SHARE_SIZE - NAMESPACE_SIZE - SHARE_INFO_BYTES - COMPACT_SHARE_RESERVED_BYTES;

        /// The number of bytes usable for data in the first sparse share of a sequence.
        pub const FIRST_SPARSE_SHARE_CONTENT_SIZE: usize =
            SHARE_SIZE - NAMESPACE_SIZE - SHARE_INFO_BYTES - SEQUENCE_LEN_BYTES;

        /// The number of bytes usable for data in a continuation sparse share of a sequence.
        pub const CONTINUATION_SPARSE_SHARE_CONTENT_SIZE: usize =
            SHARE_SIZE - NAMESPACE_SIZE - SHARE_INFO_BYTES;

        /// The smallest original square width.
        pub const MIN_SQUARE_SIZE: usize = 1;

        /// The minimum number of shares allowed in the original data square.
        pub const MIN_SHARE_COUNT: usize = MIN_SQUARE_SIZE * MIN_SQUARE_SIZE;

        /// The maximum value a share version can be.
        pub const MAX_SHARE_VERSION: u8 = 127;

        /// The size of a signer field in share, in bytes
        pub const SIGNER_SIZE: usize = 20;
    }
}

// celestia-app/pkg/da/data_availability_header
/// Constants related to the [`DataAvailabilityHeader`].
///
/// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
pub mod data_availability_header {
    /// A maximum width of the [`ExtendedDataSquare`].
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub const fn max_extended_square_width(app_version: super::appconsts::AppVersion) -> usize {
        super::appconsts::square_size_upper_bound(app_version) * 2
    }
    /// A minimum width of the [`ExtendedDataSquare`].
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub const MIN_EXTENDED_SQUARE_WIDTH: usize = super::appconsts::MIN_SQUARE_SIZE * 2;
}

/// Constants related to the underlying cosmos sdk.
pub mod cosmos {
    use const_format::concatcp;

    const PREFIX_ACCOUNT: &str = "celestia";
    const PREFIX_PUBLIC: &str = "pub";
    const PREFIX_VALIDATOR: &str = "val";
    const PREFIX_OPERATOR: &str = "oper";
    const PREFIX_CONSENSUS: &str = "cons";

    /// Bech32PrefixAccAddr defines the Bech32 prefix of an account's address.
    pub const BECH32_PREFIX_ACC_ADDR: &str = PREFIX_ACCOUNT;

    /// Bech32PrefixAccPub defines the Bech32 prefix of an account's public key.
    pub const BECH32_PREFIX_ACC_PUB: &str = concatcp!(BECH32_PREFIX_ACC_ADDR, PREFIX_PUBLIC);

    /// Bech32PrefixValAddr defines the Bech32 prefix of a validator's operator address.
    pub const BECH32_PREFIX_VAL_ADDR: &str =
        concatcp!(PREFIX_ACCOUNT, PREFIX_VALIDATOR, PREFIX_OPERATOR);

    /// Bech32PrefixValPub defines the Bech32 prefix of a validator's operator public key.
    pub const BECH32_PREFIX_VAL_PUB: &str = concatcp!(BECH32_PREFIX_VAL_ADDR, PREFIX_PUBLIC);

    /// Bech32PrefixConsAddr defines the Bech32 prefix of a consensus node address.
    pub const BECH32_PREFIX_CONS_ADDR: &str =
        concatcp!(PREFIX_ACCOUNT, PREFIX_VALIDATOR, PREFIX_CONSENSUS);

    /// Bech32PrefixConsPub defines the Bech32 prefix of a consensus node public key.
    pub const BECH32_PREFIX_CONS_PUB: &str = concatcp!(BECH32_PREFIX_CONS_ADDR, PREFIX_PUBLIC);
}
