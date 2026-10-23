#![no_main]

use std::iter;
use std::time::Duration;

use arbitrary::Arbitrary;
use celestia_types::nmt::{NamespacedHash, NamespacedHashExt};
use celestia_types::{DataAvailabilityHeader, ExtendedHeader};
use celestia_types_fuzz::header::{
    Params, Signed, Validator, Vote, build, own_power, total_power, trusted_power, validator_set,
};
use celestia_types_fuzz::mutate::Ns;
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;
use tendermint::block::{self, parts};
use tendermint::{Hash, Time};
use tendermint_proto::Protobuf;

const TRUSTED_HEIGHT: u64 = 100;
const TRUSTED_TIME: u64 = 1_735_689_600; // 2025-01-01
/// Upper bound on the length of the untrusted chain.
const MAX_CHAIN: usize = 3;

#[derive(Arbitrary, Debug)]
enum Dah {
    Square(u8),
    Roots {
        rows: Vec<(Ns, Ns, [u8; 32])>,
        cols: Vec<(Ns, Ns, [u8; 32])>,
    },
}

/// A header built on top of the previous one in the chain.
#[derive(Arbitrary, Debug)]
struct Untrusted {
    /// Offset from the previous height.
    height: i16,
    /// Seconds after the previous time.
    time: i32,
    other_chain: bool,
    /// Validators, the previous ones if `None`.
    validators: Option<Vec<Validator>>,
    proposer: Option<u8>,
    /// Next validators, the same as `validators` if `None`.
    next_validators: Option<Vec<Validator>>,
    /// Hash of the previous block, the one of the previous header if `None`.
    last_block_hash: Option<[u8; 32]>,
    dah: Dah,
    votes: Vec<Vote>,
}

#[derive(Arbitrary, Debug)]
struct Input {
    trusted: Vec<Validator>,
    trusted_proposer: u8,
    untrusted: Vec<Untrusted>,
}

fn dah(dah: &Dah) -> Option<DataAvailabilityHeader> {
    let roots = |roots: &[(Ns, Ns, [u8; 32])]| {
        roots
            .iter()
            .take(64)
            .map(|(min, max, hash)| {
                NamespacedHash::from_raw(&[&min.bytes()[..], &max.bytes(), hash].concat())
            })
            .collect::<Result<Vec<_>, _>>()
            .ok()
    };
    match dah {
        Dah::Square(selector) => Some(square(*selector).dah.clone()),
        Dah::Roots { rows, cols } => Some(DataAvailabilityHeader::new_unchecked(
            roots(rows)?,
            roots(cols)?,
        )),
    }
}

// Chains of headers signed with fuzzer-chosen validators and votes, checked like in
// header-sub and header-ex: `decode_and_validate`, and then `verify`, `verify_range` and
// `verify_adjacent_range` against a trusted header.
// Panics if a header is accepted without enough of the required voting power.
fuzz_target!(|input: Input| {
    let trusted_time = Time::from_unix_timestamp(TRUSTED_TIME as i64, 0).unwrap();
    let Some(trusted_set) = validator_set(&input.trusted, Some(input.trusted_proposer)) else {
        return;
    };
    let Some((trusted, _)) = build(Params {
        chain_id: "private".try_into().unwrap(),
        height: TRUSTED_HEIGHT,
        time: trusted_time,
        last_block_id: Some(block::Id {
            hash: Hash::Sha256([1; 32]),
            part_set_header: parts::Header::new(1, Hash::Sha256([1; 32])).unwrap(),
        }),
        next_validators_hash: trusted_set.hash(),
        validator_set: trusted_set,
        dah: square(0).dah.clone(),
        votes: &[],
    }) else {
        return;
    };
    if trusted.validate().is_err() {
        return;
    }

    let mut untrusted = Vec::new();
    for input in input.untrusted.iter().take(MAX_CHAIN) {
        let Some((header, signed)) =
            build_next(untrusted.last().map_or(&trusted, |(h, _)| h), input)
        else {
            break;
        };
        let Ok(decoded) = ExtendedHeader::decode_and_validate(&header.clone().encode_vec()) else {
            break;
        };
        assert!(
            3 * own_power(&header.validator_set, &signed) > 2 * total_power(&header.validator_set)
        );
        untrusted.push((decoded, signed));
    }
    let headers: Vec<_> = untrusted.iter().map(|(header, _)| header.clone()).collect();

    if let Some((first, signed)) = untrusted.first()
        && trusted.verify(first).is_ok()
    {
        assert_verified(&trusted, first, signed);
    }

    if trusted.verify_range(&headers).is_ok() {
        if let Some((first, signed)) = untrusted.first() {
            assert_verified(&trusted, first, signed);
        }
        for pair in untrusted.windows(2) {
            assert_verified(&pair[0].0, &pair[1].0, &pair[1].1);
        }
    }

    if trusted.verify_adjacent_range(&headers).is_ok() {
        for (prev, (header, signed)) in iter::once(&trusted).chain(&headers).zip(&untrusted) {
            assert_eq!(header.height(), prev.height() + 1);
            assert_verified(prev, header, signed);
        }
    }
});

/// Builds and signs a header on top of `prev`.
fn build_next(prev: &ExtendedHeader, input: &Untrusted) -> Option<(ExtendedHeader, Vec<Signed>)> {
    let validators = match &input.validators {
        Some(validators) => validator_set(validators, input.proposer)?,
        None => prev.validator_set.clone(),
    };
    let next_validators_hash = match &input.next_validators {
        Some(validators) => validator_set(validators, None)?.hash(),
        None => validators.hash(),
    };
    let offset = Duration::from_secs(input.time.unsigned_abs().into());
    let time = if input.time < 0 {
        prev.time() - offset
    } else {
        prev.time() + offset
    };

    build(Params {
        chain_id: if input.other_chain {
            "other"
        } else {
            "private"
        }
        .try_into()
        .unwrap(),
        height: prev.height().checked_add_signed(input.height.into())?,
        time: time.ok()?,
        last_block_id: Some(match input.last_block_hash {
            Some(hash) => block::Id {
                hash: Hash::Sha256(hash),
                part_set_header: prev.commit.block_id.part_set_header,
            },
            None => prev.commit.block_id,
        }),
        validator_set: validators,
        next_validators_hash,
        dah: dah(&input.dah)?,
        votes: &input.votes,
    })
}

/// Panics if `untrusted`, verified against `trusted`, isn't adjacent to it nor signed by
/// enough of the trusted voting power.
fn assert_verified(trusted: &ExtendedHeader, untrusted: &ExtendedHeader, signed: &[Signed]) {
    assert!(untrusted.height() > trusted.height());
    assert_eq!(untrusted.chain_id(), trusted.chain_id());
    if untrusted.height() == trusted.height() + 1 {
        assert_eq!(
            untrusted.validator_set.hash(),
            trusted.header.next_validators_hash
        );
        assert_eq!(untrusted.last_header_hash(), trusted.hash());
    } else {
        assert!(
            3 * trusted_power(&trusted.validator_set, signed) > total_power(&trusted.validator_set)
        );
    }
}
