//! Writes valid encodings into `corpus/<target>/` so fuzzing starts past the decoders.

use std::fs;
use std::path::Path;

use bytes::BytesMut;
use celestia_proto::shwap::{
    Row as RawRow, RowNamespaceData as RawRowNamespaceData, Sample as RawSample,
};
use celestia_types::eds::EdsId;
use celestia_types::namespace_data::NamespaceDataId;
use celestia_types::nmt::Namespace;
use celestia_types::row::{Row, RowId};
use celestia_types::row_namespace_data::RowNamespaceDataId;
use celestia_types::sample::{Sample, SampleId};
use celestia_types::{AxisType, ExtendedHeader};
use celestia_types_fuzz::{DAH, EDS, NAMESPACE, ODS_WIDTH};
use prost::Message;
use tendermint_proto::Protobuf;

const HEIGHT: u64 = 1;

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
        encode_id(|b| NamespaceDataId::new(NAMESPACE, HEIGHT).unwrap().encode(b)),
        encode_id(|b| {
            RowNamespaceDataId::new(NAMESPACE, 1, HEIGHT)
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

    let width = EDS.square_width();
    for (row, col) in [(0, 0), (1, 2), (width - 1, width - 1)] {
        for axis in [AxisType::Row, AxisType::Col] {
            let id = encode_id(|b| SampleId::new(row, col, HEIGHT).unwrap().encode(b));
            let sample = Sample::new(row, col, axis, &EDS).unwrap();
            let raw = RawSample::from(sample).encode_length_delimited_to_vec();
            write(
                "sample",
                &format!("sample_{row}_{col}_{axis:?}"),
                &[&id[..], &raw].concat(),
            );
        }
    }

    for index in [0, width - 1] {
        let id = encode_id(|b| RowId::new(index, HEIGHT).unwrap().encode(b));
        let row = Row::new(index, &EDS).unwrap();
        let raw = RawRow::from(row).encode_length_delimited_to_vec();
        write("row", &format!("row_{index}"), &[&id[..], &raw].concat());
    }

    for (name, ns) in [
        ("present", NAMESPACE),
        ("absent", Namespace::const_v0([2; 10])),
    ] {
        let mut data = encode_id(|b| NamespaceDataId::new(ns, HEIGHT).unwrap().encode(b));
        for (_, row) in EDS.get_namespace_data(ns, &DAH, HEIGHT).unwrap() {
            RawRowNamespaceData::from(row)
                .encode_length_delimited(&mut data)
                .unwrap();
        }
        write("namespace_data", name, &data);
    }

    let ods: Vec<u8> = (0..ODS_WIDTH as u16)
        .flat_map(|row| (0..ODS_WIDTH as u16).map(move |col| (row, col)))
        .flat_map(|(row, col)| EDS.share(row, col).unwrap().data().to_vec())
        .collect();
    write("eds", "ods", &ods);
}
