use celestia_rpc::share::GetRangeResponse;
use celestia_types::nmt::Namespace;
use celestia_types::{Blob, DataAvailabilityHeader, ExtendedDataSquare, ShareProof};
use nmt_rs::NamespaceProof as NmtProof;

#[test]
fn range_response_binds_returned_shares_to_proof() {
    let namespace = Namespace::new_v0(&[1, 2, 3]).unwrap();
    let blob = Blob::new(namespace, vec![7; 1500], None).unwrap();
    let ods = blob
        .to_shares()
        .unwrap()
        .into_iter()
        .map(|s| s.to_vec())
        .collect();
    let eds = ExtendedDataSquare::from_ods(ods).unwrap();
    let dah = DataAvailabilityHeader::from_eds(&eds);
    let root = dah.hash();
    let proof = ShareProof {
        data: vec![*eds.share(0, 0).unwrap().data()],
        namespace_id: namespace,
        share_proofs: vec![
            NmtProof::PresenceProof {
                proof: eds.row_nmt(0).unwrap().build_range_proof(0..1),
                ignore_max_ns: true,
            }
            .into(),
        ],
        row_proof: dah.row_proof(0..=0).unwrap(),
    };
    let valid = GetRangeResponse {
        shares: vec![eds.share(0, 0).unwrap().clone()],
        proof,
    };
    valid.verify_range(root, 0..1).unwrap();
    assert!(valid.verify_range(root, 1..2).is_err());

    let mut wrong = valid.clone();
    wrong.shares[0] = eds.share(0, 1).unwrap().clone();
    assert!(wrong.proof.verify_range(root, 0..1).is_ok());
    assert!(wrong.verify_range(root, 0..1).is_err());

    wrong.shares.clear();
    assert!(wrong.verify_range(root, 0..1).is_err());
}
