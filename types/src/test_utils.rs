//! Utilities for writing tests.
use std::iter;
use std::time::Duration;

use ed25519_consensus::SigningKey;
use rand::RngCore;
use tendermint::block::header::{Header, Version};
use tendermint::block::{Commit, CommitSig, parts};
use tendermint::public_key::PublicKey;
use tendermint::{Signature, Time, chain};

use crate::block::{CommitExt, GENESIS_HEIGHT};
pub use crate::byzantine::test_utils::corrupt_eds;
use crate::consts::appconsts::{
    AppVersion, CONTINUATION_SPARSE_SHARE_CONTENT_SIZE, FIRST_SPARSE_SHARE_CONTENT_SIZE,
    SHARE_INFO_BYTES, SHARE_SIZE,
};
use crate::consts::version;
use crate::hash::{Hash, HashExt};
use crate::nmt::{NS_SIZE, Namespace, NamespacedHash, NamespacedHashExt};
use crate::{
    Blob, DataAvailabilityHeader, ExtendedDataSquare, ExtendedHeader, Share, ValidatorSet,
};
#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
use wasm_bindgen::prelude::*;

/// [`ExtendedHeader`] generator for testing purposes.
///
/// **WARNING: ALL METHODS PANIC! DO NOT USE IT IN PRODUCTION!**
#[derive(Debug, Clone)]
#[cfg_attr(all(feature = "wasm-bindgen", target_arch = "wasm32"), wasm_bindgen)]
pub struct ExtendedHeaderGenerator {
    chain_id: chain::Id,
    key: SigningKey,
    current_header: Option<ExtendedHeader>,
    spoofed_block_time: Option<(Time, Duration)>,
}

impl ExtendedHeaderGenerator {
    /// Creates new `ExtendedHeaderGenerator`.
    pub fn new() -> ExtendedHeaderGenerator {
        let chain_id: chain::Id = "private".try_into().unwrap();
        let key = SigningKey::new(rand::thread_rng());

        ExtendedHeaderGenerator {
            chain_id,
            key,
            current_header: None,
            spoofed_block_time: None,
        }
    }

    /// Creates new `ExtendedHeaderGenerator` starting from specified height.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut generator = ExtendedHeaderGenerator::new_from_height(5);
    /// let header5 = generator.next();
    /// ```
    pub fn new_from_height(height: u64) -> ExtendedHeaderGenerator {
        let prev_height = height.saturating_sub(1);
        let mut generator = ExtendedHeaderGenerator::new();

        generator.current_header = if prev_height == 0 {
            None
        } else {
            Some(generate_new(
                prev_height,
                &generator.chain_id,
                Time::now(),
                &generator.key,
                None,
            ))
        };

        generator
    }

    /// Generates the next header for a random non-empty data square.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next();
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> ExtendedHeader {
        self.next_impl(Some(generate_dah(8)))
    }

    /// Generates the next header for an empty data square.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next_empty();
    /// ```
    pub fn next_empty(&mut self) -> ExtendedHeader {
        self.next_impl(None)
    }

    /// Generates the next header with the given [`DataAvailabilityHeader`]
    ///
    /// ```no_run
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    /// # fn generate_dah() -> celestia_types::DataAvailabilityHeader {
    /// #    unimplemented!();
    /// # }
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next_with_dah(generate_dah());
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn next_with_dah(&mut self, dah: DataAvailabilityHeader) -> ExtendedHeader {
        self.next_impl(Some(dah))
    }

    fn next_impl(&mut self, maybe_dah: Option<DataAvailabilityHeader>) -> ExtendedHeader {
        let time = self.get_and_increment_time(1);
        let header = match self.current_header {
            Some(ref header) => generate_next(1, header, time, &self.key, maybe_dah),
            None => generate_new(GENESIS_HEIGHT, &self.chain_id, time, &self.key, maybe_dah),
        };

        self.current_header = Some(header.clone());
        header
    }

    /// Generate the `amount` of subsequent non-empty headers.
    pub fn next_many(&mut self, amount: u64) -> Vec<ExtendedHeader> {
        let mut headers = Vec::with_capacity(amount as usize);

        for _ in 0..amount {
            headers.push(self.next());
        }

        headers
    }

    /// Generate the `amount` of subsequent empty headers.
    pub fn next_many_empty(&mut self, amount: u64) -> Vec<ExtendedHeader> {
        let mut headers = Vec::with_capacity(amount as usize);
        for _ in 0..amount {
            headers.push(self.next_empty());
        }
        headers
    }

    /// Generates the next header of the provided header.
    ///
    /// This can be used to create two headers of same height but different hash.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next();
    /// let header2 = generator.next();
    /// let another_header2 = generator.next_of(&header1);
    /// ```
    ///
    /// # Note
    ///
    /// This method does not change the state of `ExtendedHeaderGenerator`.
    pub fn next_of(&self, header: &ExtendedHeader) -> ExtendedHeader {
        let time = self
            .spoofed_block_time
            .map(|t| t.0)
            .unwrap_or_else(Time::now);
        generate_next(1, header, time, &self.key, None)
    }

    /// Generates the next header of the provided header with the given [`DataAvailabilityHeader`].
    ///
    /// This can be used to create two headers of same height but different hash.
    ///
    /// ```no_run
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    /// # fn generate_dah() -> celestia_types::DataAvailabilityHeader {
    /// #    unimplemented!();
    /// # }
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next();
    /// let header2 = generator.next();
    /// let another_header2 = generator.next_of_with_dah(&header1, generate_dah());
    /// ```
    ///
    /// # Note
    ///
    /// This method does not change the state of `ExtendedHeaderGenerator`.
    pub fn next_of_with_dah(
        &self,
        header: &ExtendedHeader,
        dah: DataAvailabilityHeader,
    ) -> ExtendedHeader {
        let time = self
            .spoofed_block_time
            .map(|t| t.0)
            .unwrap_or_else(Time::now);
        generate_next(1, header, time, &self.key, Some(dah))
    }

    /// Generates the next amount of headers of the provided header.
    ///
    /// This can be used to create two chains of headers of same
    /// heights but different hashes.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next();
    /// let headers_2_to_12 = generator.next_many(10);
    /// let another_headers_2_to_12 = generator.next_many_of(&header1, 10);
    /// ```
    ///
    /// # Note
    ///
    /// This method does not change the state of `ExtendedHeaderGenerator`.
    pub fn next_many_of(&self, header: &ExtendedHeader, amount: u64) -> Vec<ExtendedHeader> {
        let mut headers = Vec::with_capacity(amount as usize);

        for _ in 0..amount {
            let current_header = headers.last().unwrap_or(header);
            let header = self.next_of(current_header);
            headers.push(header);
        }

        headers
    }

    /// Generates the another header of the same height but different hash.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut generator = ExtendedHeaderGenerator::new();
    /// let header1 = generator.next();
    /// let header2 = generator.next();
    /// let another_header2 = generator.another_of(&header2);
    /// ```
    ///
    /// # Note
    ///
    /// This method does not change the state of `ExtendedHeaderGenerator`.
    pub fn another_of(&self, header: &ExtendedHeader) -> ExtendedHeader {
        let mut header = header.to_owned();

        header.header.consensus_hash = Hash::Sha256(rand::random());
        header.commit.block_id.part_set_header =
            parts::Header::new(1, Hash::Sha256(rand::random())).expect("invalid PartSetHeader");

        hash_and_sign(&mut header, &self.key);
        header.validate().expect("invalid header generated");

        header
    }

    /// Skips an amount of headers.
    pub fn skip(&mut self, amount: u64) {
        if amount == 0 {
            return;
        }

        let time = self.get_and_increment_time(amount);

        let header = match self.current_header {
            Some(ref header) => generate_next(amount, header, time, &self.key, None),
            None => generate_new(amount, &self.chain_id, time, &self.key, None),
        };

        self.current_header = Some(header.clone());
    }

    /// Create a "forked" generator for "forking" the chain.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut gen_chain1 = ExtendedHeaderGenerator::new();
    ///
    /// let header1 = gen_chain1.next();
    /// let header2 = gen_chain1.next();
    ///
    /// let mut gen_chain2 = gen_chain1.fork();
    ///
    /// let header3_chain1 = gen_chain1.next();
    /// let header3_chain2 = gen_chain2.next();
    /// ```
    ///
    /// # Note
    ///
    /// This is the same as clone, but the name describes the intention.
    pub fn fork(&self) -> ExtendedHeaderGenerator {
        self.clone()
    }

    /// Change header generator time. Headers generated from now on will have `time` as creation
    /// time.
    pub fn set_time(&mut self, time: Time, block_time: Duration) {
        self.spoofed_block_time = Some((time, block_time));
    }

    /// Reset header generator time, so that it produces headers with current timestamp
    pub fn reset_time(&mut self) {
        self.spoofed_block_time = None;
    }

    // private function which gets and increments generator time, since we cannot have multiple headers on the
    // exact same timestamp
    fn get_and_increment_time(&mut self, amount: u64) -> Time {
        let Some((spoofed_time, block_time)) = self.spoofed_block_time.take() else {
            return Time::now();
        };

        let block_time_ms: u64 = block_time.as_millis().try_into().expect("u64 overflow");

        let timestamp = (spoofed_time + Duration::from_millis(block_time_ms * amount))
            .expect("not to overflow");
        self.spoofed_block_time = Some((timestamp, block_time));

        timestamp
    }
}

impl Default for ExtendedHeaderGenerator {
    fn default() -> Self {
        ExtendedHeaderGenerator::new()
    }
}

#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
#[wasm_bindgen]
impl ExtendedHeaderGenerator {
    /// Creates new `ExtendedHeaderGenerator` starting from specified height.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut gen = ExtendedHeaderGenerator::new_from_height(5);
    /// let header5 = gen.next();
    /// ```
    #[wasm_bindgen(constructor, js_name = new)]
    pub fn js_new(from_height: Option<u64>) -> ExtendedHeaderGenerator {
        if let Some(height) = from_height {
            ExtendedHeaderGenerator::new_from_height(height)
        } else {
            ExtendedHeaderGenerator::new()
        }
    }

    /// Generates the next header.
    ///
    /// ```
    /// use celestia_types::test_utils::ExtendedHeaderGenerator;
    ///
    /// let mut gen = ExtendedHeaderGenerator::new();
    /// let header1 = gen.next();
    /// ```
    #[wasm_bindgen(js_name = next)]
    pub fn js_next(&mut self) -> ExtendedHeader {
        let time = self.get_and_increment_time(1);
        let header = match self.current_header {
            Some(ref header) => generate_next(1, header, time, &self.key, None),
            None => generate_new(GENESIS_HEIGHT, &self.chain_id, time, &self.key, None),
        };

        self.current_header = Some(header.clone());
        header
    }
}

/// Invalidate the provided header.
///
/// This can be combined with [`unverify`]
pub fn invalidate(header: &mut ExtendedHeader) {
    // One way of invalidate `ExtendedHeader` but still passes the
    // verification check, is to clear `dah` but keep `data_hash` unchanged.
    header.dah = DataAvailabilityHeader::new_unchecked(Vec::new(), Vec::new());

    header.validate().unwrap_err();
}

/// Unverify the provided header.
///
/// This can be combined with [`invalidate`].
pub fn unverify(header: &mut ExtendedHeader) {
    let was_invalidated = header.validate().is_err();

    // One way to unverify `ExtendedHeader` but still passes the
    // validation check, is to sign it with a new key.
    let key = SigningKey::new(rand::thread_rng());
    let pub_key_bytes = key.verification_key().to_bytes();
    let pub_key = PublicKey::from_raw_ed25519(&pub_key_bytes).unwrap();
    let validator_address = tendermint::account::Id::from(pub_key);

    header.header.proposer_address = validator_address;

    header.validator_set = ValidatorSet::new(
        vec![tendermint::validator::Info {
            address: validator_address,
            pub_key,
            power: 5000_u32.into(),
            name: None,
            proposer_priority: 0_i64.into(),
        }],
        Some(tendermint::validator::Info {
            address: validator_address,
            pub_key,
            power: 5000_u32.into(),
            name: None,
            proposer_priority: 0_i64.into(),
        }),
    );

    hash_and_sign(header, &key);

    if was_invalidated {
        invalidate(header);
    } else {
        header.validate().expect("invalid header generated");
    }
}

/// Generate a dummy DAH, with semi-real namespace structure
///
/// Computes fake roots of an EDS where:
/// - first share is PFB
/// - second is primary padding
/// - last is tail padding
/// - each share between is a blob within unique namespace
fn generate_dah(square_width: usize) -> DataAvailabilityHeader {
    // allow minimum ODS to be 2x2, to not handle case with single
    // share in ODS
    assert!(square_width >= 4);

    let ods_width = square_width / 2;

    // toss random namespaces for our blobs. we do it for the whole ODS even tho
    // 3 of those will be unused, but it makes operating on indexes simpler
    let blob_namespaces: Vec<_> = (0..ods_width * ods_width)
        .map(|n| {
            let ns = [
                // ensure ordering
                (n as u32).to_be_bytes().as_slice(),
                random_bytes(6).as_slice(),
            ]
            .concat();
            Namespace::new_v0(&ns).unwrap()
        })
        .collect();

    let random_namespaced_hash = |(min_ns, max_ns): (Namespace, Namespace)| {
        let hash = [
            min_ns.as_bytes(),
            max_ns.as_bytes(),
            random_bytes(32).as_slice(),
        ]
        .concat();
        NamespacedHash::from_raw(&hash).unwrap()
    };

    // within ODS, create hashes with proper min and max namespaces
    // based on blobs namespaces we created earlier. Outside ODS
    // everything is parity
    let row_roots: Vec<_> = (0..ods_width)
        .map(|n| {
            let min_ns = match n {
                0 => Namespace::PAY_FOR_BLOB,
                _ => {
                    // take blobs from first column
                    let first_blob_in_row = ods_width * n;
                    blob_namespaces[first_blob_in_row]
                }
            };
            let max_ns = if n == ods_width - 1 {
                Namespace::TAIL_PADDING
            } else {
                // take blobs from last column
                let last_blob_in_row = ods_width * (n + 1) - 1;
                blob_namespaces[last_blob_in_row]
            };
            (min_ns, max_ns)
        })
        .chain(iter::repeat_n(
            (Namespace::PARITY_SHARE, Namespace::PARITY_SHARE),
            ods_width,
        ))
        .map(random_namespaced_hash)
        .collect();

    let col_roots: Vec<_> = (0..ods_width)
        .map(|n| {
            let min_ns = match n {
                0 => Namespace::PAY_FOR_BLOB,
                1 => Namespace::PRIMARY_RESERVED_PADDING,
                // take blobs from first row
                _ => blob_namespaces[n],
            };
            let max_ns = if n == ods_width - 1 {
                Namespace::TAIL_PADDING
            } else {
                // take blobs from last row
                let last_blob_in_col = ods_width * (ods_width - 1) + n;
                blob_namespaces[last_blob_in_col]
            };
            (min_ns, max_ns)
        })
        .chain(iter::repeat_n(
            (Namespace::PARITY_SHARE, Namespace::PARITY_SHARE),
            ods_width,
        ))
        .map(random_namespaced_hash)
        .collect();

    DataAvailabilityHeader::new_unchecked(row_roots, col_roots)
}

/// Generate a properly encoded [`ExtendedDataSquare`] with random data.
pub fn generate_dummy_eds(square_width: usize, app_version: AppVersion) -> ExtendedDataSquare {
    let ns = Namespace::const_v0(rand::random());
    let ods_width = square_width / 2;

    let shares: Vec<_> = (0..ods_width * ods_width)
        .map(|_| {
            [
                ns.as_bytes(),
                &[0; SHARE_INFO_BYTES][..],
                &random_bytes(SHARE_SIZE - NS_SIZE - SHARE_INFO_BYTES)[..],
            ]
            .concat()
        })
        .collect();

    ExtendedDataSquare::from_ods(shares, app_version).unwrap()
}

/// Generate a properly encoded [`ExtendedDataSquare`] with random data.
///
/// The generated EDS will try to mimic structure of the real blocks,
/// having some shares from primary reserved namespace and padding shares.
///
/// The generated square will have PFB shares and primary reserved namespace padding
/// in first ODS row. Then it will have a blob that spans 2 rows with padding.
/// 4th row will have a blob with padding in the same namespace as the previous blob.
/// Each next row have a blob in random namespace followed by padding, and last one
/// is padded with tail padding namespace.
///
/// Minimum supported square_width is 8.
pub fn generate_eds(square_width: usize, app_version: AppVersion) -> ExtendedDataSquare {
    assert!(square_width >= 8);

    let ods_width = square_width / 2;
    let mut shares = Vec::with_capacity(ods_width * ods_width);

    // pay for blob shares, only in first row
    let pfb_shares = (rand::random::<usize>() % (ods_width - 1)) + 1;
    shares.extend((0..pfb_shares).map(|n| {
        let info_byte = (n == 0) as u8; // first has sequence_start
        [
            Namespace::PAY_FOR_BLOB.as_bytes(),
            &[info_byte][..],
            &random_bytes(SHARE_SIZE - NS_SIZE - SHARE_INFO_BYTES)[..],
        ]
        .concat()
    }));
    // primary namespace padding
    shares.extend((pfb_shares..ods_width).map(|_| {
        [
            Namespace::PRIMARY_RESERVED_PADDING.as_bytes(),
            &[0; SHARE_SIZE - NS_SIZE][..],
        ]
        .concat()
    }));

    // fill rest of rows with user blobs
    let mut namespaces: Vec<_> = (3..ods_width)
        .map(|_| Namespace::const_v0(rand::random()))
        .collect();
    namespaces.sort();

    // first blob is bigger so that it spans over 2 rows
    let blob_shares = (rand::random::<usize>() % (ods_width - 1)) + ods_width + 1;
    let data = random_bytes(blob_len(blob_shares));
    let blob = Blob::new(namespaces[0], data, None, app_version).unwrap();
    shares.extend(blob.to_shares().unwrap().iter().map(Share::to_vec));

    // namespace padding
    shares.extend(
        (blob_shares - ods_width..ods_width)
            .map(|_| [namespaces[0].as_bytes(), &[0; SHARE_SIZE - NS_SIZE][..]].concat()),
    );

    // rest of the blobs, starting with one in the same namespace as the big one before
    for ns in &namespaces {
        let blob_shares = (rand::random::<usize>() % (ods_width - 1)) + 1;
        let data = random_bytes(blob_len(blob_shares));
        let blob = Blob::new(*ns, data, None, app_version).unwrap();
        shares.extend(blob.to_shares().unwrap().iter().map(Share::to_vec));

        let padding_ns = if ns != namespaces.last().unwrap() {
            *ns
        } else {
            Namespace::TAIL_PADDING
        };
        shares.extend(
            (blob_shares..ods_width)
                .map(|_| [padding_ns.as_bytes(), &[0; SHARE_SIZE - NS_SIZE][..]].concat()),
        );
    }

    ExtendedDataSquare::from_ods(shares, app_version).unwrap()
}

fn blob_len(shares: usize) -> usize {
    assert_ne!(shares, 0);
    FIRST_SPARSE_SHARE_CONTENT_SIZE + (shares - 1) * CONTINUATION_SPARSE_SHARE_CONTENT_SIZE
}

pub(crate) fn random_bytes(len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut buf);
    buf
}

fn generate_new(
    height: u64,
    chain_id: &chain::Id,
    time: Time,
    signing_key: &SigningKey,
    dah: Option<DataAvailabilityHeader>,
) -> ExtendedHeader {
    assert!(height >= GENESIS_HEIGHT);

    let pub_key_bytes = signing_key.verification_key().to_bytes();
    let pub_key = PublicKey::from_raw_ed25519(&pub_key_bytes).unwrap();
    let validator_address = tendermint::account::Id::from(pub_key);

    let last_block_id = if height == GENESIS_HEIGHT {
        None
    } else {
        Some(tendermint::block::Id {
            hash: Hash::Sha256(rand::random()),
            part_set_header: parts::Header::new(1, Hash::Sha256(rand::random()))
                .expect("invalid PartSetHeader"),
        })
    };

    let mut header = ExtendedHeader {
        header: Header {
            version: Version {
                block: version::BLOCK_PROTOCOL,
                app: 1,
            },
            chain_id: chain_id.clone(),
            height: height.try_into().unwrap(),
            time,
            last_block_id,
            last_commit_hash: Some(Hash::default_sha256()),
            data_hash: Some(Hash::None),
            validators_hash: Hash::None,
            next_validators_hash: Hash::None,
            consensus_hash: Hash::Sha256(rand::random()),
            app_hash: Hash::default_sha256()
                .as_bytes()
                .to_vec()
                .try_into()
                .unwrap(),
            last_results_hash: Some(Hash::default_sha256()),
            evidence_hash: Some(Hash::default_sha256()),
            proposer_address: validator_address,
        },
        commit: Commit {
            height: height.try_into().unwrap(),
            round: 0_u16.into(),
            block_id: tendermint::block::Id {
                hash: Hash::None,
                part_set_header: parts::Header::new(1, Hash::Sha256(rand::random()))
                    .expect("invalid PartSetHeader"),
            },
            signatures: vec![CommitSig::BlockIdFlagCommit {
                validator_address,
                timestamp: time,
                signature: None,
            }],
        },
        validator_set: ValidatorSet::new(
            vec![tendermint::validator::Info {
                address: validator_address,
                pub_key,
                power: 5000_u32.into(),
                name: None,
                proposer_priority: 0_i64.into(),
            }],
            Some(tendermint::validator::Info {
                address: validator_address,
                pub_key,
                power: 5000_u32.into(),
                name: None,
                proposer_priority: 0_i64.into(),
            }),
        ),
        dah: dah.unwrap_or_else(|| DataAvailabilityHeader::from_eds(&ExtendedDataSquare::empty())),
    };

    hash_and_sign(&mut header, signing_key);
    header.validate().expect("invalid header generated");

    header
}

fn generate_next(
    increment: u64,
    current: &ExtendedHeader,
    time: Time,
    signing_key: &SigningKey,
    dah: Option<DataAvailabilityHeader>,
) -> ExtendedHeader {
    assert!(increment > 0);

    let validator_address = current.validator_set.validators()[0].address;

    let height = (current.header.height.value() + increment)
        .try_into()
        .unwrap();

    let last_block_id = if increment == 1 {
        Some(current.commit.block_id)
    } else {
        Some(tendermint::block::Id {
            hash: Hash::Sha256(rand::random()),
            part_set_header: parts::Header::new(1, Hash::Sha256(rand::random()))
                .expect("invalid PartSetHeader"),
        })
    };

    let mut header = ExtendedHeader {
        header: Header {
            version: current.header.version,
            chain_id: current.header.chain_id.clone(),
            height,
            time,
            last_block_id,
            last_commit_hash: Some(Hash::default_sha256()),
            data_hash: Some(Hash::None),
            validators_hash: Hash::None,
            next_validators_hash: Hash::None,
            consensus_hash: Hash::Sha256(rand::random()),
            app_hash: Hash::default_sha256()
                .as_bytes()
                .to_vec()
                .try_into()
                .unwrap(),
            last_results_hash: Some(Hash::default_sha256()),
            evidence_hash: Some(Hash::default_sha256()),
            proposer_address: validator_address,
        },
        commit: Commit {
            height,
            round: 0_u16.into(),
            block_id: tendermint::block::Id {
                hash: Hash::None,
                part_set_header: parts::Header::new(1, Hash::Sha256(rand::random()))
                    .expect("invalid PartSetHeader"),
            },
            signatures: vec![CommitSig::BlockIdFlagCommit {
                validator_address,
                timestamp: time,
                signature: None,
            }],
        },
        validator_set: current.validator_set.clone(),
        dah: dah.unwrap_or_else(|| DataAvailabilityHeader::from_eds(&ExtendedDataSquare::empty())),
    };

    hash_and_sign(&mut header, signing_key);
    header.validate().expect("invalid header generated");
    current.verify(&header).expect("invalid header generated");

    header
}

fn hash_and_sign(header: &mut ExtendedHeader, signing_key: &SigningKey) {
    header.header.validators_hash = header.validator_set.hash();
    header.header.next_validators_hash = header.validator_set.hash();
    header.header.data_hash = Some(header.dah.hash());
    header.commit.block_id.hash = header.header.hash();

    let vote_sign = header
        .commit
        .vote_sign_bytes(&header.header.chain_id, 0)
        .unwrap();
    let sig = signing_key.sign(&vote_sign).to_bytes();

    match header.commit.signatures[0] {
        CommitSig::BlockIdFlagAbsent => {}
        CommitSig::BlockIdFlagNil {
            ref mut signature, ..
        }
        | CommitSig::BlockIdFlagCommit {
            ref mut signature, ..
        } => {
            *signature = Some(Signature::new(sig).unwrap().unwrap());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[test]
    fn generate_blocks() {
        let mut generator = ExtendedHeaderGenerator::new();

        let genesis = generator.next();
        assert_eq!(genesis.height(), 1);

        let height2 = generator.next();
        assert_eq!(height2.height(), 2);

        let another_height2 = generator.next_of(&genesis);
        assert_eq!(another_height2.height(), 2);

        genesis.verify(&height2).unwrap();
        genesis.verify(&another_height2).unwrap();

        assert_ne!(height2.hash(), another_height2.hash());
    }

    #[test]
    fn generate_and_verify_range() {
        let mut generator = ExtendedHeaderGenerator::new();

        let genesis = generator.next();
        assert_eq!(genesis.height(), 1);

        let headers = generator.next_many(256);
        assert_eq!(headers.last().unwrap().height(), 257);

        genesis.verify_adjacent_range(&headers).unwrap();
        genesis.verify_range(&headers[10..]).unwrap();

        headers[0].verify_adjacent_range(&headers[1..]).unwrap();
        headers[0].verify_range(&headers[10..]).unwrap();

        headers[5].verify_adjacent_range(&headers[6..]).unwrap();
        headers[5].verify_range(&headers[10..]).unwrap();
    }

    #[test]
    fn generate_and_skip() {
        let mut generator = ExtendedHeaderGenerator::new();

        let genesis = generator.next();
        generator.skip(3);
        let header5 = generator.next();

        assert_eq!(genesis.height(), 1);
        assert_eq!(header5.height(), 5);
        genesis.verify(&header5).unwrap();
    }

    #[test]
    fn new_and_skip() {
        let mut generator = ExtendedHeaderGenerator::new();

        generator.skip(3);
        let header4 = generator.next();
        let header5 = generator.next();

        assert_eq!(header4.height(), 4);
        assert_eq!(header5.height(), 5);
        header4.verify(&header5).unwrap();

        let mut generator = ExtendedHeaderGenerator::new();

        generator.skip(1);
        let header2 = generator.next();
        let header3 = generator.next();

        assert_eq!(header2.height(), 2);
        assert_eq!(header3.height(), 3);
        header2.verify(&header3).unwrap();

        let mut generator = ExtendedHeaderGenerator::new();

        generator.skip(0);
        let genesis = generator.next();
        let header2 = generator.next();

        assert_eq!(genesis.height(), 1);
        assert_eq!(header2.height(), 2);
        genesis.verify(&header2).unwrap();
    }

    #[test]
    fn new_from_height() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);
        let header5 = generator.next();
        assert_eq!(header5.height(), 5);

        let mut generator = ExtendedHeaderGenerator::new_from_height(1);
        let header1 = generator.next();
        assert_eq!(header1.height(), 1);

        let mut generator = ExtendedHeaderGenerator::new_from_height(0);
        let header1 = generator.next();
        assert_eq!(header1.height(), 1);
    }

    #[test]
    fn generate_next_of() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let header6 = generator.next();
        let _header7 = generator.next();
        let another_header6 = generator.next_of(&header5);

        header5.verify(&header6).unwrap();
        header5.verify(&another_header6).unwrap();

        assert_eq!(header6.height(), 6);
        assert_eq!(another_header6.height(), 6);
        assert_ne!(header6.hash(), another_header6.hash());
    }

    #[test]
    fn generate_next_many_of() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let header6 = generator.next();
        let _header7 = generator.next();
        let another_header_6_to_10 = generator.next_many_of(&header5, 5);

        header5.verify(&header6).unwrap();
        header5
            .verify_adjacent_range(&another_header_6_to_10)
            .unwrap();

        assert_eq!(another_header_6_to_10.len(), 5);
        assert_eq!(header6.height(), 6);
        assert_eq!(another_header_6_to_10[0].height(), 6);
        assert_ne!(header6.hash(), another_header_6_to_10[0].hash());
    }

    #[test]
    fn gen_next_after_next_many_of() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let another_header_6_to_10 = generator.next_many_of(&header5, 5);
        // `next_of` and `next_many_of` does not change the state of the
        // generator, so `next` must return height 6 header.
        let header6 = generator.next();

        header5.verify(&header6).unwrap();
        header5
            .verify_adjacent_range(&another_header_6_to_10)
            .unwrap();

        assert_eq!(another_header_6_to_10.len(), 5);
        assert_eq!(header6.height(), 6);
        assert_eq!(another_header_6_to_10[0].height(), 6);
        assert_ne!(header6.hash(), another_header_6_to_10[0].hash());
    }

    #[test]
    fn generate_another_of() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let header6 = generator.next();

        let another_header6 = generator.another_of(&header6);

        header5.verify(&header6).unwrap();
        header5.verify(&another_header6).unwrap();
    }

    #[test]
    fn invalidate_header() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let mut header6 = generator.next();
        let mut header7 = generator.next();

        invalidate(&mut header6);

        // Check that can be called multiple times
        invalidate(&mut header7);
        invalidate(&mut header7);
        invalidate(&mut header7);

        header6.validate().unwrap_err();
        header5.verify(&header6).unwrap();

        header7.validate().unwrap_err();
        header5.verify(&header7).unwrap();
    }

    #[test]
    fn unverify_header() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let mut header6 = generator.next();
        let mut header7 = generator.next();

        unverify(&mut header6);

        // Check that can be called multiple times
        unverify(&mut header7);
        unverify(&mut header7);
        unverify(&mut header7);

        header6.validate().unwrap();
        header5.verify(&header6).unwrap_err();

        header7.validate().unwrap();
        header5.verify(&header7).unwrap_err();
    }

    #[test]
    fn invalidate_and_unverify_header() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let mut header6 = generator.next();

        invalidate(&mut header6);
        unverify(&mut header6);

        header6.validate().unwrap_err();
        header5.verify(&header6).unwrap_err();

        let mut generator = ExtendedHeaderGenerator::new_from_height(5);

        let header5 = generator.next();
        let mut header6 = generator.next();

        // check different order too
        unverify(&mut header6);
        invalidate(&mut header6);

        header6.validate().unwrap_err();
        header5.verify(&header6).unwrap_err();
    }
}
