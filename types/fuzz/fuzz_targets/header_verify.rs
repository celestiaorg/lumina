#![no_main]

use std::slice;
use std::time::Duration;

use arbitrary::Arbitrary;
use celestia_types::nmt::{NamespacedHash, NamespacedHashExt};
use celestia_types::{DataAvailabilityHeader, ExtendedHeader};
use celestia_types_fuzz::header::{
    Params, Validator, Vote, build, own_power, total_power, trusted_power, validator_set,
};
use celestia_types_fuzz::mutate::Ns;
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;
use tendermint::block::{self, parts};
use tendermint::{Hash, Time};
use tendermint_proto::Protobuf;

const TRUSTED_HEIGHT: u64 = 100;
const TRUSTED_TIME: u64 = 1_735_689_600; // 2025-01-01

#[derive(Arbitrary, Debug)]
enum Dah {
    Square(u8),
    Roots {
        rows: Vec<(Ns, Ns, [u8; 32])>,
        cols: Vec<(Ns, Ns, [u8; 32])>,
    },
}

#[derive(Arbitrary, Debug)]
struct Untrusted {
    /// Offset from the trusted height.
    height: i16,
    /// Seconds after the trusted time.
    time: i32,
    other_chain: bool,
    /// Validators, the trusted ones if `None`.
    validators: Option<Vec<Validator>>,
    proposer: Option<u8>,
    /// Next validators, the same as `validators` if `None`.
    next_validators: Option<Vec<Validator>>,
    /// Hash of the previous block, the trusted one if `None`.
    last_block_hash: Option<[u8; 32]>,
    dah: Dah,
    votes: Vec<Vote>,
}

#[derive(Arbitrary, Debug)]
struct Input {
    trusted: Vec<Validator>,
    trusted_proposer: u8,
    untrusted: Untrusted,
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

// Headers signed with fuzzer-chosen validators and votes, checked like in header-sub and
// header-ex: `decode_and_validate` and then `verify` against a trusted header.
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

    let input = input.untrusted;
    let Some(validators) = (match &input.validators {
        Some(validators) => validator_set(validators, input.proposer),
        None => Some(trusted.validator_set.clone()),
    }) else {
        return;
    };
    let Some(next_validators_hash) = (match &input.next_validators {
        Some(validators) => validator_set(validators, None).map(|set| set.hash()),
        None => Some(validators.hash()),
    }) else {
        return;
    };
    let Some(dah) = dah(&input.dah) else {
        return;
    };
    let Some(height) = TRUSTED_HEIGHT.checked_add_signed(input.height.into()) else {
        return;
    };
    let offset = Duration::from_secs(input.time.unsigned_abs().into());
    let Ok(time) = (if input.time < 0 {
        trusted_time - offset
    } else {
        trusted_time + offset
    }) else {
        return;
    };

    let Some((untrusted, signed)) = build(Params {
        chain_id: if input.other_chain {
            "other"
        } else {
            "private"
        }
        .try_into()
        .unwrap(),
        height,
        time,
        last_block_id: Some(match input.last_block_hash {
            Some(hash) => block::Id {
                hash: Hash::Sha256(hash),
                part_set_header: trusted.commit.block_id.part_set_header,
            },
            None => trusted.commit.block_id,
        }),
        validator_set: validators,
        next_validators_hash,
        dah,
        votes: &input.votes,
    }) else {
        return;
    };

    let Ok(decoded) = ExtendedHeader::decode_and_validate(&untrusted.clone().encode_vec()) else {
        return;
    };
    assert!(
        3 * own_power(&untrusted.validator_set, &signed)
            > 2 * total_power(&untrusted.validator_set)
    );

    if trusted.verify(&decoded).is_ok() {
        assert!(decoded.height() > trusted.height());
        assert_eq!(decoded.chain_id(), trusted.chain_id());
        if decoded.height() == trusted.height() + 1 {
            assert_eq!(
                decoded.validator_set.hash(),
                trusted.header.next_validators_hash
            );
            assert_eq!(decoded.last_header_hash(), trusted.hash());
        } else {
            assert!(
                3 * trusted_power(&trusted.validator_set, &signed)
                    > total_power(&trusted.validator_set)
            );
        }
    }

    let _ = trusted.verify_adjacent_range(slice::from_ref(&decoded));
});
