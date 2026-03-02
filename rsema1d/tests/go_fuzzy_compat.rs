use rsema1d::{
    encode, reconstruct, verify_standalone, verify_with_context, OriginalRows, Parameters,
    RowProof, StandaloneProof, VerificationContext,
};
use serde::Deserialize;
use std::borrow::Cow;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct TestVectorFile {
    test_cases: Vec<TestVector>,
}

#[derive(Debug, Deserialize)]
struct TestVector {
    name: String,
    params: TestParams,
    original_data: Vec<String>,
    commitment: String,
    proof_tests: Vec<ProofTest>,
}

#[derive(Debug, Deserialize)]
struct TestParams {
    k: usize,
    n: usize,
    row_size: usize,
}

#[derive(Debug, Deserialize)]
struct ProofTest {
    index: usize,
    row_data: String,
    row_proof: Vec<String>,
    rlc_proof: Vec<String>,
    standalone_proof: bool,
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

#[test]
#[ignore = "requires go/fuzzy_test_vectors.json generated via go/cmd/generate_fuzzy_vectors"]
fn go_fuzzy_vectors_match_rust() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("go/fuzzy_test_vectors.json");
    let vectors = load_vectors(path);

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

        let mut original = Vec::with_capacity(params.k * params.row_size);
        for row_hex in &case.original_data {
            original.extend_from_slice(&decode_hex_row(row_hex, params.row_size));
        }

        let original_rows = OriginalRows::new(original.clone(), &params)
            .unwrap_or_else(|e| panic!("{}: original rows shape failed: {}", case.name, e));

        let (ext_data, commitment, rlc_orig) = encode(&original_rows, &params)
            .unwrap_or_else(|e| panic!("{}: encode failed: {}", case.name, e));

        assert_eq!(
            hex::encode(commitment),
            case.commitment,
            "{}: commitment mismatch vs Go",
            case.name
        );

        let context = VerificationContext::new(&rlc_orig, &params)
            .unwrap_or_else(|e| panic!("{}: context failed: {}", case.name, e));

        for proof in &case.proof_tests {
            let row = decode_hex_row(&proof.row_data, params.row_size);
            let row_proof = decode_proof_nodes(&proof.row_proof);

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
                let standalone = StandaloneProof {
                    index: proof.index,
                    row,
                    row_proof,
                    rlc_proof,
                };
                verify_standalone(&standalone, &commitment, &params).unwrap_or_else(|e| {
                    panic!(
                        "{}: standalone verify failed for index {}: {}",
                        case.name, proof.index, e
                    )
                });
            } else {
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

        // Decode path check: reconstruct from the first K rows must match original.
        let indices: Vec<usize> = (0..params.k).collect();
        let rows = ext_data
            .rows()
            .sample(&indices)
            .unwrap_or_else(|e| panic!("{}: sampled rows shape failed: {}", case.name, e));
        let reconstructed = reconstruct(&rows, &indices, &params)
            .unwrap_or_else(|e| panic!("{}: reconstruct failed: {}", case.name, e));
        assert_eq!(
            reconstructed.as_bytes(),
            original.as_slice(),
            "{}: reconstructed original mismatch",
            case.name
        );
    }
}
