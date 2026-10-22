//! Checks `commitment_from_original_rows` against blob IDs from celestia-app's Go `fibre.NewBlob`.

use rsema1d::{commitment_from_original_rows, Parameters};

const K: usize = 4096;
const N: usize = 12288;
const HEADER_LEN: usize = 5;

struct Vector {
    size: usize,
    blob_id: &'static str,
    row_root_siblings: [&'static str; 2],
}

const VECTORS: &[Vector] = &[
    Vector {
        size: 1,
        blob_id: "00a5aa0ec592fe0e8bdd97ad1c303d5ff25baec8f520dfc61c3c351896450a43e5",
        row_root_siblings: [
            "63cd88b132fa7e1fb30bd6c7a1b3c02a5047027efa460522d19a3d1a32d9c497",
            "8430b688fa97dc9aa23ca83e043cbc126a63e984f0b47501f815f61ca7b0503b",
        ],
    },
    Vector {
        size: 5000,
        blob_id: "00aa5172a5328bfef46c21e81507dae952484b2238c4c65db1f26813a56c9e1a8d",
        row_root_siblings: [
            "db3f9c4a58dec38bc70f99bde77b95f6671e2c0aa4ab1ac8fe8a5e348f1f266e",
            "fb0a4a39a826eaac878bba06884297cc2ed65d4d222715b7eebafc4d476b04c3",
        ],
    },
    Vector {
        size: 262139,
        blob_id: "00cdedc4ef940d403be62f020ca2fc4ab3a8c480f2916943902abaa514f18963df",
        row_root_siblings: [
            "67337f8a5bda84ef51b09dd5dd4f4d6d22a6442102b4c94ae59c5dc096522e19",
            "954159e22ed1402c178dd7209c5f65820311108808f2ce09cb7bcbecd667ef46",
        ],
    },
    Vector {
        size: 262140,
        blob_id: "002e7cf475cb1bfe3f443919a0f131940a8c5625c0a2f0970a610801d9268708d6",
        row_root_siblings: [
            "0ec524ed42ec84a89a0c84c1e8882e665e46181fc9a887c8d09ee0fffa9f420d",
            "9e59d42d1beac17ac546d313d410eb81705d79df6c7d263f539bc3a7ad08896c",
        ],
    },
    Vector {
        size: 1048576,
        blob_id: "006be5fc1a521d478e66488235b65bbae7ff140d27f88f0de39f4c20ebee1eb2d2",
        row_root_siblings: [
            "bd2e74455ac55f92aa30f33a0e260c9797dfc5c8f550e78834ddc407111084dd",
            "3285f512e6f225a6c2806334cea6698e1e32f14d4074668c0c8e86561ac6c614",
        ],
    },
    Vector {
        size: 33554427,
        blob_id: "00cfc476ac39a6becb1c748901b0754c21ac93b8fc99f9844f02fcdf645c8b247b",
        row_root_siblings: [
            "5b47dc78c2b5481bbb6e71b6b6be67e5d9a3f91e8142db3a53dd26aab963cb78",
            "bddfceb85274020ea0b618056b6abea45fb4fc04a3d9ada3820e59ea7652fe25",
        ],
    },
    Vector {
        size: 134217723,
        blob_id: "0036af67d7d2270c70c7ef8142d3c2f2e5f0a062416334e700f621aeb95dc2344a",
        row_root_siblings: [
            "539e96c9b6886f9bfae5f89978f63c7433f291dfba196e58baf4a270b4ba050b",
            "240c9148d4ff4620f4652c340011b8531a57d38ea8b78d7dbb145a42984d6b02",
        ],
    },
];

/// Fibre blob v0 rows: version byte, big-endian u32 data length, data, zero padding.
fn original_rows(size: usize) -> (Vec<u8>, usize) {
    let row_size = (size + HEADER_LEN).div_ceil(K).next_multiple_of(64);
    let mut rows = vec![0u8; K * row_size];
    rows[1..HEADER_LEN].copy_from_slice(&(size as u32).to_be_bytes());
    // Byte i is the top byte of the wrapping u32 product i * 2654435761.
    for (i, byte) in rows[HEADER_LEN..HEADER_LEN + size].iter_mut().enumerate() {
        *byte = ((i as u32).wrapping_mul(2654435761) >> 24) as u8;
    }
    (rows, row_size)
}

#[test]
fn fibre_vectors() {
    for v in VECTORS {
        let (data, row_size) = original_rows(v.size);
        let rows: Vec<&[u8]> = data.chunks_exact(row_size).collect();
        let params = Parameters::new(K, N, row_size).unwrap();
        let siblings = v
            .row_root_siblings
            .map(|s| hex::decode(s).unwrap().try_into().unwrap());

        let commitment = commitment_from_original_rows(&rows, &params, &siblings).unwrap();
        assert_eq!(hex::encode(commitment), v.blob_id[2..], "size {}", v.size);
    }
}
