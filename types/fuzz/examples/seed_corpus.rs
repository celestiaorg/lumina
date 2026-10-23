//! Writes valid encodings into `corpus/<target>/` so fuzzing starts past the decoders.
//!
//! Only the byte-level targets need seeds, structured ones start from valid responses.

use std::fs;
use std::path::Path;

use bytes::BytesMut;
use celestia_proto::shwap::row::HalfSide;
use celestia_proto::shwap::{
    Row as RawRow, RowNamespaceData as RawRowNamespaceData, Sample as RawSample, Share as RawShare,
};
use celestia_types::eds::EdsId;
use celestia_types::namespace_data::NamespaceDataId;
use celestia_types::nmt::Namespace;
use celestia_types::row::RowId;
use celestia_types::row_namespace_data::RowNamespaceDataId;
use celestia_types::sample::{Sample, SampleId};
use celestia_types::{AxisType, ExtendedHeader};
use celestia_types_fuzz::{HEIGHT, SQUARES, namespace};
use prost::Message;
use tendermint_proto::Protobuf;

fn write(target: &str, name: &str, data: &[u8]) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join(target);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(name), data).unwrap();
}

fn encode_id(encode: impl FnOnce(&mut BytesMut)) -> BytesMut {
    let mut bytes = BytesMut::new();
    encode(&mut bytes);
    bytes
}

fn main() {
    for file in [
        "extended_header_block_1.json",
        "extended_header_block_27.json",
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data/chain1")
            .join(file);
        let header: ExtendedHeader =
            serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        let name = file.trim_end_matches(".json");
        write("extended_header", name, &header.encode_vec());
    }

    let ids = [
        encode_id(|b| EdsId::new(HEIGHT).unwrap().encode(b)),
        encode_id(|b| RowId::new(1, HEIGHT).unwrap().encode(b)),
        encode_id(|b| SampleId::new(1, 2, HEIGHT).unwrap().encode(b)),
        encode_id(|b| {
            NamespaceDataId::new(namespace(1), HEIGHT)
                .unwrap()
                .encode(b)
        }),
        encode_id(|b| {
            RowNamespaceDataId::new(namespace(1), 1, HEIGHT)
                .unwrap()
                .encode(b)
        }),
    ];
    for (selector, id) in ids.iter().enumerate() {
        write(
            "shwap_ids",
            &format!("id_{selector}"),
            &[&[selector as u8][..], id].concat(),
        );
    }

    for (selector, square) in SQUARES.iter().enumerate() {
        let eds = &square.eds;
        let width = eds.square_width();
        let prefix = [selector as u8];

        for (row, col) in [(0, 0), (1, width / 2), (width - 1, width - 1)] {
            for axis in [AxisType::Row, AxisType::Col] {
                let id = encode_id(|b| SampleId::new(row, col, HEIGHT).unwrap().encode(b));
                let sample = Sample::new(row, col, axis, eds).unwrap();
                let raw = RawSample::from(sample).encode_length_delimited_to_vec();
                write(
                    "sample",
                    &format!("w{width}_{row}_{col}_{axis:?}"),
                    &[&prefix[..], &id, &raw].concat(),
                );
            }
        }

        for index in [0, width - 1] {
            let id = encode_id(|b| RowId::new(index, HEIGHT).unwrap().encode(b));
            let shares = eds.row(index).unwrap();
            let (left, right) = shares.split_at(shares.len() / 2);
            for (half, half_side) in [(left, HalfSide::Left), (right, HalfSide::Right)] {
                let raw = RawRow {
                    shares_half: half
                        .iter()
                        .map(|share| RawShare {
                            data: share.to_vec(),
                        })
                        .collect(),
                    half_side: half_side.into(),
                };
                write(
                    "row",
                    &format!("w{width}_{index}_{half_side:?}"),
                    &[&prefix[..], &id, &raw.encode_length_delimited_to_vec()].concat(),
                );
            }
        }

        // present, in gaps between present ones, around the data and padding
        let namespaces = [
            ("present_1", namespace(1)),
            ("present_3", namespace(3)),
            ("present_5", namespace(5)),
            ("absent_0", namespace(0)),
            ("absent_2", namespace(2)),
            ("absent_4", namespace(4)),
            ("absent_max", namespace(u16::MAX)),
            ("tail_padding", Namespace::TAIL_PADDING),
        ];
        for (name, ns) in namespaces {
            let mut data = encode_id(|b| NamespaceDataId::new(ns, HEIGHT).unwrap().encode(b));
            for (_, row) in eds.get_namespace_data(ns, &square.dah, HEIGHT).unwrap() {
                RawRowNamespaceData::from(row)
                    .encode_length_delimited(&mut data)
                    .unwrap();
            }
            write(
                "namespace_data",
                &format!("w{width}_{name}"),
                &[&prefix[..], &data].concat(),
            );
        }

        let ods_width = width / 2;
        let ods: Vec<u8> = (0..ods_width)
            .flat_map(|row| (0..ods_width).map(move |col| (row, col)))
            .flat_map(|(row, col)| eds.share(row, col).unwrap().data().to_vec())
            .collect();
        write("eds", &format!("w{width}"), &ods);
    }
}
