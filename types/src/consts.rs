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

    /// Maximum width of a single subtree root when generating blob's commitment.
    ///
    /// This value is the same for all app versions (V1-V7).
    pub const SUBTREE_ROOT_THRESHOLD: u64 = 64;

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
