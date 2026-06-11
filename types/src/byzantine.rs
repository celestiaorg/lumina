use celestia_proto::share::eds::byzantine::pb::BadEncoding as RawBadEncodingFraudProof;
use celestia_proto::share::eds::byzantine::pb::Share as RawShareWithProof;
use tendermint::block::Height;
use tendermint_proto::Protobuf;

use crate::bail_validation;
use crate::consts::appconsts;
use crate::eds::AxisType;
use crate::fraud_proof::FraudProof;
use crate::hash::Hash;
use crate::nmt::{NS_SIZE, Namespace, NamespaceProof, Nmt, NmtExt};
use crate::{Error, ExtendedHeader, Result};

/// A proof that the block producer incorrectly encoded [`ExtendedDataSquare`].
///
/// A malicious actor may incorrectly extend the original data with the
/// recovery data, thus making it impossible to reconstruct the original
/// information.
///
/// Light node cannot detect such behaviour with [`Data Availability Sampling`].
/// When the full node collects all the [`Share`]s of a row or column of the [`ExtendedDataSquare`]
/// and detects that it was incorrectly encoded, it should create and announce
/// [`BadEncodingFraudProof`] so that light nodes can reject this block.
///
/// [`Data Availability Sampling`]: https://docs.celestia.org/learn/how-celestia-works/data-availability-layer
/// [`ExtendedDataSquare`]: crate::ExtendedDataSquare
/// [`Share`]: crate::share::Share
#[derive(Debug, Clone, PartialEq)]
pub struct BadEncodingFraudProof {
    header_hash: Hash,
    block_height: u64,
    // ShareWithProof contains all shares from row or col.
    // Shares that did not pass verification in rsmt2d will be nil (None).
    // For non-nil shares MerkleProofs are computed.
    shares: Vec<Option<ShareWithProof>>,
    // Index represents the row/col index where ErrByzantineRow/ErrByzantineColl occurred.
    index: u16,
    // Axis represents the axis that verification failed on.
    axis: AxisType,
}

impl FraudProof for BadEncodingFraudProof {
    const TYPE: &'static str = "badencoding";

    fn header_hash(&self) -> Hash {
        self.header_hash
    }

    fn height(&self) -> u64 {
        self.block_height
    }

    fn validate(&self, header: &ExtendedHeader) -> Result<()> {
        if header.height() != self.height() {
            bail_validation!(
                "header height ({}) != fraud proof height ({})",
                header.height(),
                self.height()
            );
        }

        // NOTE: shouldn't ever happen as header should be validated before
        if header.dah.row_roots().len() != header.dah.column_roots().len() {
            bail_validation!(
                "dah rows len ({}) != dah columns len ({})",
                header.dah.row_roots().len(),
                header.dah.column_roots().len(),
            );
        }

        let square_width = usize::from(header.dah.square_width());
        let ods_width = square_width / 2;

        if usize::from(self.index) >= square_width {
            bail_validation!(
                "fraud proof index ({}) >= dah square width ({})",
                self.index,
                square_width,
            );
        }

        if self.shares.len() != square_width {
            bail_validation!(
                "fraud proof shares len ({}) != dah square width ({})",
                self.shares.len(),
                square_width,
            );
        }

        let shares_count = self.shares.iter().filter(|s| s.is_some()).count();
        if shares_count < ods_width {
            bail_validation!(
                "fraud proof present shares count ({shares_count}) < dah ods width ({ods_width})"
            );
        }

        // verify that Merkle proofs correspond to particular shares.
        for (share_idx, maybe_share) in self.shares.iter().enumerate() {
            let Some(share_with_proof) = maybe_share else {
                continue;
            };

            let ShareWithProof {
                leaf: NmtLeaf { namespace, share },
                proof,
                proof_axis,
            } = share_with_proof;

            // unwraps are safe because we validated that index is in range
            let root = match (self.axis, proof_axis) {
                (AxisType::Row, AxisType::Row) => header.dah.row_root(self.index).unwrap(),
                (AxisType::Row, AxisType::Col) => header.dah.column_root(share_idx as u16).unwrap(),
                (AxisType::Col, AxisType::Row) => header.dah.row_root(share_idx as u16).unwrap(),
                (AxisType::Col, AxisType::Col) => header.dah.column_root(self.index).unwrap(),
            };

            proof
                .verify_range(&root, &[&share], **namespace)
                .map_err(Error::RangeProofError)?;
        }

        // rebuild the whole axis
        let mut rebuilt_shares: Vec<_> = self
            .shares
            .iter()
            .map(|maybe_share| {
                maybe_share
                    .clone()
                    .map(|share_with_proof| share_with_proof.leaf.share)
                    .unwrap_or_default()
            })
            .collect();
        if leopard_codec::reconstruct(&mut rebuilt_shares, ods_width).is_err() {
            // we couldn't reconstruct the data even tho we had enough *proven* shares
            // befp is legit
            return Ok(());
        }

        // re-encode the parity data
        if leopard_codec::encode(&mut rebuilt_shares, ods_width).is_err() {
            // NOTE: this is unreachable for current implementation of encode, esp. since
            // reconstruct succeeded, however leaving that as a future-proofing
            //
            // we couldn't encode parity data after reconstruction from proven shares
            // befp is legit
            return Ok(());
        }

        let mut nmt = Nmt::default();

        for (n, share) in rebuilt_shares.iter().enumerate() {
            let ns = if n < ods_width {
                // safety: length must be correct
                Namespace::from_raw(&share[..NS_SIZE]).unwrap()
            } else {
                Namespace::PARITY_SHARE
            };
            if nmt.push_leaf(share, *ns).map_err(Error::Nmt).is_err() {
                // we couldn't rebuild the nmt from reconstructed data
                // befp is legit
                return Ok(());
            }
        }

        // unwraps are safe because we validated that index is in range
        let expected_root = match self.axis {
            AxisType::Row => header.dah.row_root(self.index).unwrap(),
            AxisType::Col => header.dah.column_root(self.index).unwrap(),
        };
        let root = nmt.root();

        if root == expected_root {
            bail_validation!("recomputed root hash matches the one in header");
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ShareWithProof {
    leaf: NmtLeaf,
    proof: NamespaceProof,
    proof_axis: AxisType,
}

#[derive(Debug, Clone, PartialEq)]
struct NmtLeaf {
    namespace: Namespace,
    share: Vec<u8>,
}

impl NmtLeaf {
    fn new(mut bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() != appconsts::SHARE_SIZE + NS_SIZE {
            return Err(Error::InvalidNmtLeafSize(bytes.len()));
        }

        let share = bytes.split_off(NS_SIZE);

        Ok(Self {
            namespace: Namespace::from_raw(&bytes)?,
            share,
        })
    }

    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = self.namespace.as_bytes().to_vec();
        bytes.extend_from_slice(self.share.as_ref());
        bytes
    }
}

impl TryFrom<RawShareWithProof> for ShareWithProof {
    type Error = Error;

    fn try_from(value: RawShareWithProof) -> Result<Self, Self::Error> {
        let leaf = NmtLeaf::new(value.data)?;

        let proof = value.proof.ok_or(Error::MissingProof)?;
        let proof = NamespaceProof::try_from(proof)?;

        if proof.is_of_absence() {
            return Err(Error::WrongProofType);
        }

        let proof_axis = value.proof_axis.try_into()?;

        Ok(Self {
            leaf,
            proof,
            proof_axis,
        })
    }
}

impl From<ShareWithProof> for RawShareWithProof {
    fn from(value: ShareWithProof) -> Self {
        RawShareWithProof {
            data: value.leaf.to_vec(),
            proof: Some(value.proof.into()),
            proof_axis: value.proof_axis as i32,
        }
    }
}

impl Protobuf<RawBadEncodingFraudProof> for BadEncodingFraudProof {}

impl TryFrom<RawBadEncodingFraudProof> for BadEncodingFraudProof {
    type Error = Error;

    fn try_from(value: RawBadEncodingFraudProof) -> Result<Self, Self::Error> {
        let axis = value.axis.try_into()?;

        let index = u16::try_from(value.index).map_err(|_| Error::EdsInvalidDimentions)?;

        let shares = value
            .shares
            .into_iter()
            .map(|share| {
                if share.proof.is_some() {
                    share.try_into().map(Some)
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<_, _>>()?;

        Height::try_from(value.height)?;

        Ok(Self {
            header_hash: value.header_hash.try_into()?,
            block_height: value.height,
            shares,
            index,
            axis,
        })
    }
}

impl From<BadEncodingFraudProof> for RawBadEncodingFraudProof {
    fn from(value: BadEncodingFraudProof) -> Self {
        let shares = value
            .shares
            .into_iter()
            .map(|share| share.map(Into::into).unwrap_or_default())
            .collect();
        RawBadEncodingFraudProof {
            header_hash: value.header_hash.into(),
            height: value.block_height,
            shares,
            index: value.index as u32,
            axis: value.axis as i32,
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) mod test_utils {
    use nmt_rs::NamespaceProof;
    use rand::Rng;
    use rand::seq::index;

    use crate::consts::appconsts::{FIRST_SPARSE_SHARE_CONTENT_SIZE, SHARE_SIZE};
    use crate::eds::is_ods_square;
    use crate::test_utils::{ExtendedHeaderGenerator, random_bytes};
    use crate::{DataAvailabilityHeader, ExtendedDataSquare};

    use super::*;

    /// Corrupts the [`ExtendedDataSquare`] making some row or column unrecoverable.
    /// Returns the [`ExtendedHeader`] with merkle proofs for incorrect data and [`BadEncodingFraudProof`].
    pub fn corrupt_eds(
        generator: &mut ExtendedHeaderGenerator,
        eds: &mut ExtendedDataSquare,
    ) -> (ExtendedHeader, BadEncodingFraudProof) {
        let mut rng = rand::thread_rng();

        let square_width = eds.square_width();
        let axis = rng.gen_range(0..1).try_into().unwrap();
        let axis_idx = rng.gen_range(0..square_width);

        // invalidate more than a half shares in axis
        let shares_to_break = square_width / 2 + 1;
        for share_idx in index::sample(&mut rng, square_width.into(), shares_to_break.into()) {
            let share_idx = u16::try_from(share_idx).unwrap();

            let share = match axis {
                AxisType::Row => eds.share_mut(axis_idx, share_idx).unwrap(),
                AxisType::Col => eds.share_mut(share_idx, axis_idx).unwrap(),
            };
            // only trash data after the namespace, info byte and seq length so that we don't
            // need to care whether the share is original or parity
            let offset = SHARE_SIZE - FIRST_SPARSE_SHARE_CONTENT_SIZE;
            share.as_mut()[offset..].copy_from_slice(&random_bytes(SHARE_SIZE - offset));
        }

        // create extended header with proof
        let dah = DataAvailabilityHeader::from_eds(eds);
        let eh = generator.next_with_dah(dah);

        let befp = befp_from_header_and_eds(&eh, eds, axis_idx, axis);

        (eh, befp)
    }

    pub(crate) fn befp_from_header_and_eds(
        eh: &ExtendedHeader,
        eds: &ExtendedDataSquare,
        axis_idx: u16,
        axis: AxisType,
    ) -> BadEncodingFraudProof {
        let square_width = eds.square_width();
        let mut shares_with_proof: Vec<_> = Vec::with_capacity(square_width.into());

        // collect the shares for fraud proof
        for share_idx in 0..square_width {
            let proof_axis = if rand::random() {
                AxisType::Row
            } else {
                AxisType::Col
            };

            let mut nmt = match (axis, proof_axis) {
                (AxisType::Row, AxisType::Row) => eds.row_nmt(axis_idx).unwrap(),
                (AxisType::Row, AxisType::Col) => eds.column_nmt(share_idx).unwrap(),
                (AxisType::Col, AxisType::Row) => eds.row_nmt(share_idx).unwrap(),
                (AxisType::Col, AxisType::Col) => eds.column_nmt(axis_idx).unwrap(),
            };

            // The index of the share in the `nmt`.
            let idx = match (axis, proof_axis) {
                (AxisType::Row, AxisType::Row) => share_idx,
                (AxisType::Row, AxisType::Col) => axis_idx,
                (AxisType::Col, AxisType::Row) => axis_idx,
                (AxisType::Col, AxisType::Col) => share_idx,
            };

            let (share, proof) = nmt.get_index_with_proof(idx.into());

            // it doesn't matter which is row and which is column as ods is first quadrant
            let ns = if is_ods_square(axis_idx, share_idx, square_width) {
                Namespace::from_raw(&share[..NS_SIZE]).unwrap()
            } else {
                Namespace::PARITY_SHARE
            };

            let share_with_proof = ShareWithProof {
                leaf: NmtLeaf {
                    namespace: ns,
                    share,
                },
                proof: NamespaceProof::PresenceProof {
                    proof,
                    ignore_max_ns: true,
                }
                .into(),
                proof_axis,
            };

            shares_with_proof.push(Some(share_with_proof));
        }

        BadEncodingFraudProof {
            header_hash: eh.hash(),
            block_height: eh.height(),
            shares: shares_with_proof,
            index: axis_idx,
            axis,
        }
    }
}

#[cfg(test)]
mod tests {
    use self::test_utils::befp_from_header_and_eds;
    use super::*;
    use crate::DataAvailabilityHeader;
    use crate::test_utils::{ExtendedHeaderGenerator, corrupt_eds, generate_dummy_eds};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[test]
    fn validate_honest_befp_with_incorrectly_encoded_full_row() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (eh, proof) = corrupt_eds(&mut generator, &mut eds);

        proof.validate(&eh).unwrap();
    }

    #[test]
    fn validate_honest_befp_with_shares_to_rebuild() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (eh, mut proof) = corrupt_eds(&mut generator, &mut eds);

        // remove some shares from the proof so they need to be reconstructed
        for share in proof.shares.iter_mut().step_by(2) {
            *share = None
        }

        proof.validate(&eh).unwrap();
    }

    #[test]
    fn validate_fake_befp() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let real_dah = DataAvailabilityHeader::from_eds(&eds);

        let prev_eh = generator.next();
        let (_fake_eh, proof) = corrupt_eds(&mut generator, &mut eds);

        let real_eh = generator.next_of_with_dah(&prev_eh, real_dah);

        proof.validate(&real_eh).unwrap_err();
    }

    #[test]
    fn validate_befp_over_correct_data() {
        let mut generator = ExtendedHeaderGenerator::new();
        let eds = generate_dummy_eds(8);
        let dah = DataAvailabilityHeader::from_eds(&eds);
        let eh = generator.next_with_dah(dah);

        let proof = befp_from_header_and_eds(&eh, &eds, 2, AxisType::Row);
        proof.validate(&eh).unwrap_err();

        let proof = befp_from_header_and_eds(&eh, &eds, 3, AxisType::Col);
        proof.validate(&eh).unwrap_err();
    }

    #[test]
    fn validate_befp_wrong_height() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (mut eh, proof) = corrupt_eds(&mut generator, &mut eds);

        eh.header.height = 999u32.into();

        proof.validate(&eh).unwrap_err();
    }

    #[test]
    fn validate_befp_wrong_roots_square() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (mut eh, proof) = corrupt_eds(&mut generator, &mut eds);

        eh.dah = DataAvailabilityHeader::new_unchecked(Vec::new(), eh.dah.column_roots().to_vec());

        proof.validate(&eh).unwrap_err();
    }

    #[test]
    fn validate_befp_wrong_index() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (eh, mut proof) = corrupt_eds(&mut generator, &mut eds);

        proof.index = 999;

        proof.validate(&eh).unwrap_err();
    }

    #[test]
    fn validate_befp_wrong_shares() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (eh, mut proof) = corrupt_eds(&mut generator, &mut eds);

        proof.shares = vec![];

        proof.validate(&eh).unwrap_err();
    }
}
