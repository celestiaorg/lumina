use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rsema1d::{
    create_verification_context, encode, reconstruct, verify_row_inclusion_proof,
    verify_standalone, verify_with_context, Parameters, RowInclusionProof, RowMatrix, RowProof,
    StandaloneProof, GF128,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestVectorFile {
    version: String,
    test_cases: Vec<TestVector>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestVector {
    name: String,
    params: TestParams,
    seed: i64,
    original_data: Vec<String>,
    rlc_orig: Vec<String>,
    commitment: String,
    proof_tests: Vec<ProofTest>,
    row_inclusion_tests: Vec<RowInclusionTest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestParams {
    k: usize,
    n: usize,
    row_size: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProofTest {
    index: usize,
    is_original: bool,
    row_hash: String,
    row_proof_hash: String,
    rlc_proof_hash: String,
    row_data: String,
    row_proof: Vec<String>,
    rlc_proof: Vec<String>,
    standalone_proof: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RowInclusionTest {
    index: usize,
    is_original: bool,
    row_hash: String,
    row_proof_hash: String,
    row_data: String,
    row_proof: Vec<String>,
    rlc_root: String,
}

fn load_vectors(path: PathBuf) -> TestVectorFile {
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read vectors at {}: {}", path.display(), e));
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("failed to parse vectors at {}: {}", path.display(), e))
}

fn decode_hex_row(hex_row: &str, row_size: usize) -> Vec<u8> {
    let row = hex::decode(hex_row).unwrap_or_else(|e| panic!("invalid hex row: {}", e));
    assert_eq!(
        row.len(),
        row_size,
        "row size mismatch in vector: expected {}, got {}",
        row_size,
        row.len()
    );
    row
}

fn decode_proof_nodes(nodes_hex: &[String]) -> Vec<[u8; 32]> {
    nodes_hex
        .iter()
        .map(|s| {
            let node = hex::decode(s).unwrap_or_else(|e| panic!("invalid proof node hex: {}", e));
            assert_eq!(
                node.len(),
                32,
                "proof node size mismatch: expected 32, got {}",
                node.len()
            );
            let mut out = [0u8; 32];
            out.copy_from_slice(&node);
            out
        })
        .collect()
}

fn decode_gf128(hex_value: &str) -> GF128 {
    let bytes = hex::decode(hex_value).unwrap_or_else(|e| panic!("invalid RLC hex: {}", e));
    assert_eq!(
        bytes.len(),
        16,
        "RLC size mismatch: expected 16 bytes, got {}",
        bytes.len()
    );
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    GF128::from_bytes(&out)
}

fn decode_hash32(hex_value: &str) -> [u8; 32] {
    let bytes = hex::decode(hex_value).unwrap_or_else(|e| panic!("invalid hash hex: {}", e));
    assert_eq!(
        bytes.len(),
        32,
        "hash size mismatch: expected 32 bytes, got {}",
        bytes.len()
    );
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

fn hash_bytes(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn hash_merkle_nodes(nodes: &[[u8; 32]]) -> String {
    let mut h = Sha256::new();
    for node in nodes {
        h.update(node);
    }
    hex::encode(h.finalize())
}

fn row_at(row_major: &[u8], row_size: usize, index: usize) -> &[u8] {
    let start = index * row_size;
    let end = start + row_size;
    &row_major[start..end]
}

#[test]
#[ignore = "requires go/fuzzy_test_vectors.json generated via go/cmd/generate_fuzzy_vectors"]
fn go_fuzzy_vectors_match_rust() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("go/fuzzy_test_vectors.json");
    let vectors = load_vectors(path);
    assert!(
        !vectors.version.is_empty(),
        "vector file version must be present"
    );

    for case in vectors.test_cases {
        let params = Parameters::new(case.params.k, case.params.n, case.params.row_size)
            .unwrap_or_else(|e| panic!("{}: invalid params: {}", case.name, e));

        assert_eq!(
            case.original_data.len(),
            params.k,
            "{}: expected {} original rows in vectors, got {}",
            case.name,
            params.k,
            case.original_data.len()
        );
        assert_eq!(
            case.rlc_orig.len(),
            params.k,
            "{}: expected {} original RLC values in vectors, got {}",
            case.name,
            params.k,
            case.rlc_orig.len()
        );
        assert!(
            !case.row_inclusion_tests.is_empty(),
            "{}: expected non-empty row_inclusion_tests",
            case.name
        );

        let mut original = Vec::with_capacity(params.k * params.row_size);
        for row_hex in &case.original_data {
            original.extend_from_slice(&decode_hex_row(row_hex, params.row_size));
        }
        let go_rlc_orig: Vec<GF128> = case.rlc_orig.iter().map(|s| decode_gf128(s)).collect();

        let original_rows = RowMatrix::with_shape(original.clone(), params.k, params.row_size)
            .unwrap_or_else(|e| panic!("{}: original rows shape failed: {}", case.name, e));

        let (ext_data, commitment, rust_rlc_orig) = encode(&original_rows, &params)
            .unwrap_or_else(|e| panic!("{}: encode failed: {}", case.name, e));

        assert_eq!(
            hex::encode(commitment),
            case.commitment,
            "{}: commitment mismatch vs Go",
            case.name
        );
        assert_eq!(
            rust_rlc_orig, go_rlc_orig,
            "{}: original RLC mismatch vs Go",
            case.name
        );

        let (context, go_rlc_root) = create_verification_context(&go_rlc_orig, &params)
            .unwrap_or_else(|e| panic!("{}: context failed: {}", case.name, e));
        assert_eq!(
            go_rlc_root,
            ext_data.rlc_root(),
            "{}: RLC root mismatch vs Go context",
            case.name
        );

        for proof in &case.proof_tests {
            let row = decode_hex_row(&proof.row_data, params.row_size);
            let row_proof = decode_proof_nodes(&proof.row_proof);
            assert_eq!(
                proof.is_original,
                proof.index < params.k,
                "{}: is_original flag mismatch for index {}",
                case.name,
                proof.index
            );
            assert_eq!(
                proof.standalone_proof, proof.is_original,
                "{}: standalone/is_original mismatch for index {}",
                case.name, proof.index
            );
            assert_eq!(
                proof.row_hash,
                hash_bytes(&row),
                "{}: row hash mismatch for index {}",
                case.name,
                proof.index
            );
            assert_eq!(
                proof.row_proof_hash,
                hash_merkle_nodes(&row_proof),
                "{}: row proof hash mismatch for index {}",
                case.name,
                proof.index
            );

            let expected_row = ext_data
                .row(proof.index)
                .unwrap_or_else(|e| panic!("{}: row({}) failed: {}", case.name, proof.index, e));
            assert_eq!(
                expected_row,
                row.as_slice(),
                "{}: row data mismatch for index {}",
                case.name,
                proof.index
            );

            if proof.standalone_proof {
                let rlc_proof = decode_proof_nodes(&proof.rlc_proof);
                assert!(
                    proof.is_original,
                    "{}: standalone proof marked for parity row index {}",
                    case.name, proof.index
                );
                assert_eq!(
                    proof.rlc_proof_hash,
                    hash_merkle_nodes(&rlc_proof),
                    "{}: RLC proof hash mismatch for index {}",
                    case.name,
                    proof.index
                );

                let rust_standalone = ext_data
                    .generate_standalone_proof(proof.index)
                    .unwrap_or_else(|e| {
                        panic!("{}: rust standalone generation failed: {}", case.name, e)
                    });
                assert_eq!(
                    rust_standalone.row.as_slice(),
                    row.as_slice(),
                    "{}: standalone row mismatch vs Rust-generated proof for index {}",
                    case.name,
                    proof.index
                );
                assert_eq!(
                    rust_standalone.row_proof.as_slice(),
                    row_proof.as_slice(),
                    "{}: standalone row proof nodes mismatch vs Rust-generated proof for index {}",
                    case.name,
                    proof.index
                );
                assert_eq!(
                    rust_standalone.rlc_proof.as_slice(),
                    rlc_proof.as_slice(),
                    "{}: standalone RLC proof nodes mismatch vs Rust-generated proof for index {}",
                    case.name,
                    proof.index
                );

                let standalone = StandaloneProof {
                    index: proof.index,
                    row: row.clone(),
                    row_proof: row_proof.clone(),
                    rlc_proof: rlc_proof.clone(),
                };
                verify_standalone(&standalone, &commitment, &params).unwrap_or_else(|e| {
                    panic!(
                        "{}: standalone verify failed for index {}: {}",
                        case.name, proof.index, e
                    )
                });
            } else {
                assert!(
                    !proof.is_original,
                    "{}: non-standalone proof marked for original row index {}",
                    case.name, proof.index
                );
                assert!(
                    proof.index >= params.k,
                    "{}: non-standalone proof must be parity index, got {} (< k={})",
                    case.name,
                    proof.index,
                    params.k
                );
                assert!(
                    proof.rlc_proof.is_empty(),
                    "{}: unexpected RLC proof for non-standalone index {}",
                    case.name,
                    proof.index
                );
                assert!(
                    proof.rlc_proof_hash.is_empty(),
                    "{}: unexpected RLC proof hash for non-standalone index {}",
                    case.name,
                    proof.index
                );

                let rust_row_proof = ext_data
                    .generate_row_proof(proof.index)
                    .unwrap_or_else(|e| {
                        panic!("{}: rust row proof generation failed: {}", case.name, e)
                    });
                assert_eq!(
                    rust_row_proof.row.as_ref(),
                    row.as_slice(),
                    "{}: row mismatch vs Rust-generated proof for index {}",
                    case.name,
                    proof.index
                );
                assert_eq!(
                    rust_row_proof.row_proof.as_slice(),
                    row_proof.as_slice(),
                    "{}: row proof nodes mismatch vs Rust-generated proof for index {}",
                    case.name,
                    proof.index
                );

                let row_proof = RowProof {
                    index: proof.index,
                    row: Cow::Owned(row),
                    row_proof,
                };
                verify_with_context(&row_proof, &commitment, &context).unwrap_or_else(|e| {
                    panic!(
                        "{}: context verify failed for index {}: {}",
                        case.name, proof.index, e
                    )
                });
            }
        }

        // Row inclusion proofs (separate section):
        // compare Go-emitted inclusion vectors against Rust-generated inclusion proofs and
        // verify inclusion for both original and parity indices.
        for inclusion_test in &case.row_inclusion_tests {
            let row = decode_hex_row(&inclusion_test.row_data, params.row_size);
            let row_proof = decode_proof_nodes(&inclusion_test.row_proof);
            let inclusion_rlc_root = decode_hash32(&inclusion_test.rlc_root);
            assert_eq!(
                inclusion_test.is_original,
                inclusion_test.index < params.k,
                "{}: row inclusion is_original mismatch for index {}",
                case.name,
                inclusion_test.index
            );
            assert_eq!(
                inclusion_test.row_hash,
                hash_bytes(&row),
                "{}: row inclusion row hash mismatch for index {}",
                case.name,
                inclusion_test.index
            );
            assert_eq!(
                inclusion_test.row_proof_hash,
                hash_merkle_nodes(&row_proof),
                "{}: row inclusion proof hash mismatch for index {}",
                case.name,
                inclusion_test.index
            );
            assert_eq!(
                inclusion_rlc_root, go_rlc_root,
                "{}: row inclusion rlc_root mismatch vs Go context root for index {}",
                case.name, inclusion_test.index
            );

            let inclusion = RowInclusionProof {
                index: inclusion_test.index,
                row: row.clone(),
                row_proof: row_proof.clone(),
                rlc_root: inclusion_rlc_root,
            };
            verify_row_inclusion_proof(&inclusion, &commitment, &params).unwrap_or_else(|e| {
                panic!(
                    "{}: row inclusion verify failed for index {}: {}",
                    case.name, inclusion_test.index, e
                )
            });

            let rust_inclusion = ext_data
                .generate_row_inclusion_proof(inclusion_test.index)
                .unwrap_or_else(|e| {
                    panic!(
                        "{}: rust row inclusion generation failed for index {}: {}",
                        case.name, inclusion_test.index, e
                    )
                });
            assert_eq!(
                rust_inclusion.index, inclusion_test.index,
                "{}: row inclusion index mismatch for index {}",
                case.name, inclusion_test.index
            );
            assert_eq!(
                rust_inclusion.row.as_slice(),
                row.as_slice(),
                "{}: row inclusion row mismatch for index {}",
                case.name,
                inclusion_test.index
            );
            assert_eq!(
                rust_inclusion.row_proof.as_slice(),
                row_proof.as_slice(),
                "{}: row inclusion proof nodes mismatch for index {}",
                case.name,
                inclusion_test.index
            );
            assert_eq!(
                rust_inclusion.rlc_root, inclusion_rlc_root,
                "{}: row inclusion rlc_root mismatch for index {}",
                case.name, inclusion_test.index
            );
        }

        // Decode path check (non-trivial): remove a random number of originals and
        // replace them with random parity rows from Go vectors. Optionally add extra
        // parity rows so total sampled rows can be > K.
        let mut parity_proofs: Vec<&ProofTest> =
            case.proof_tests.iter().filter(|p| !p.is_original).collect();
        assert!(
            !parity_proofs.is_empty(),
            "{}: no parity proof vector found",
            case.name
        );

        let mut rng = ChaCha8Rng::seed_from_u64(
            (case.seed as u64) ^ ((params.k as u64) << 32) ^ (params.n as u64),
        );
        parity_proofs.shuffle(&mut rng);

        let max_replace = parity_proofs.len().min(params.k);
        let replace_count = if max_replace == 1 {
            1
        } else {
            rng.gen_range(1..=max_replace)
        };
        let selected_parity: Vec<&ProofTest> = parity_proofs[..replace_count].to_vec();
        let remaining_pool = &parity_proofs[replace_count..];

        let mut removed_originals: Vec<usize> = (0..params.k).collect();
        removed_originals.shuffle(&mut rng);
        removed_originals.truncate(replace_count);
        removed_originals.sort_unstable();

        let remaining_parity = remaining_pool.len();
        let extra_parity = if remaining_parity == 0 {
            0
        } else {
            rng.gen_range(0..=remaining_parity)
        };
        let mut additional_parity: Vec<&ProofTest> = remaining_pool.to_vec();
        additional_parity.shuffle(&mut rng);
        additional_parity.truncate(extra_parity);

        let mut indices = Vec::with_capacity(params.k + extra_parity);
        let mut sampled = Vec::with_capacity((params.k + extra_parity) * params.row_size);
        for idx in 0..params.k {
            if removed_originals.binary_search(&idx).is_ok() {
                continue;
            }
            indices.push(idx);
            sampled.extend_from_slice(row_at(&original, params.row_size, idx));
        }
        for parity in selected_parity.iter().chain(additional_parity.iter()) {
            indices.push(parity.index);
            sampled.extend_from_slice(&decode_hex_row(&parity.row_data, params.row_size));
        }
        assert!(
            indices.len() >= params.k,
            "{}: reconstruct sample size mismatch: got {}, need at least {}",
            case.name,
            indices.len(),
            params.k
        );
        assert!(
            indices.iter().any(|&i| i >= params.k),
            "{}: reconstruct sample does not include parity rows",
            case.name
        );

        let rows = RowMatrix::with_shape(sampled, indices.len(), params.row_size)
            .unwrap_or_else(|e| panic!("{}: sampled rows shape failed: {}", case.name, e));
        let reconstructed = reconstruct(&rows, &indices, &params)
            .unwrap_or_else(|e| panic!("{}: reconstruct failed: {}", case.name, e));
        assert_eq!(
            reconstructed.as_row_major(),
            original.as_slice(),
            "{}: reconstructed original mismatch",
            case.name
        );
    }
}
