//! A build script generating rust types from protobuf definitions.

#[cfg(not(feature = "tonic"))]
use prost_types::FileDescriptorSet;
use std::collections::HashSet;
#[cfg(feature = "tonic")]
use tonic_build::FileDescriptorSet;

const DEFAULT: &str = r#"#[serde(default)]"#;
const HEXSTRING: &str = r#"#[serde(with = "crate::serializers::bytes::hexstring")]"#;
const VEC_HEXSTRING: &str = r#"#[serde(with = "crate::serializers::bytes::vec_hexstring")]"#;
const SERIALIZED: &str = r#"#[derive(::serde::Deserialize, ::serde::Serialize)]"#;
const SERIALIZED_DEFAULT: &str =
    r#"#[derive(::serde::Deserialize, ::serde::Serialize)] #[serde(default)]"#;
const TRANSPARENT: &str = r#"#[serde(transparent)]"#;
const BASE64STRING: &str = r#"#[serde(with = "crate::serializers::bytes::base64string")]"#;
const QUOTED_WITH_DEFAULT: &str = r#"#[serde(with = "crate::serializers::from_str", default)]"#;
const MAYBE_QUOTED: &str = r#"#[serde(with = "crate::serializers::maybe_quoted")]"#;
const VEC_BASE64STRING: &str = r#"#[serde(with = "crate::serializers::bytes::vec_base64string")]"#;
const OPTION_TIMESTAMP: &str = r#"#[serde(with = "crate::serializers::option_timestamp")]"#;
const OPTION_PROTOBUF_DURATION: &str =
    r#"#[serde(with = "crate::serializers::option_protobuf_duration")]"#;
const NULL_DEFAULT: &str = r#"#[serde(with = "crate::serializers::null_default")]"#;
const VEC_SKIP_IF_EMPTY: &str = r#"#[serde(skip_serializing_if = "::std::vec::Vec::is_empty")]"#;
const BYTES_SKIP_IF_EMPTY: &str = r#"#[serde(skip_serializing_if = "bytes::Bytes::is_empty")]"#;
const SERIALIZE_CAMEL_CASE: &str = r#"#[serde(rename_all = "camelCase")]"#;
const UNIFFI_RECORD: &str = r#"#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]"#;
const UNIFFI_ENUM: &str = r#"#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]"#;
const WASM_BINDGEN: &str = r#"#[cfg_attr(all(target_arch = "wasm32", feature = "wasm-bindgen"), wasm_bindgen::prelude::wasm_bindgen)]"#;
const WASM_BINDGEN_WITH_CLONE: &str = r#"#[cfg_attr(all(target_arch = "wasm32", feature = "wasm-bindgen"), wasm_bindgen::prelude::wasm_bindgen(getter_with_clone))]"#;

#[rustfmt::skip]
static CUSTOM_TYPE_ATTRIBUTES: &[(&str, &str)] = &[
    (".celestia.blob.v1.MsgPayForBlobs", SERIALIZED_DEFAULT),
    (".celestia.core.v1.da.DataAvailabilityHeader", SERIALIZED_DEFAULT),
    (".celestia.core.v1.proof.NMTProof", SERIALIZED_DEFAULT),
    (".celestia.core.v1.proof.Proof", SERIALIZED_DEFAULT),
    (".celestia.core.v1.proof.RowProof", SERIALIZED_DEFAULT),
    (".celestia.core.v1.proof.ShareProof", SERIALIZED_DEFAULT),
    (".cosmos.auth.v1beta1.Params", UNIFFI_RECORD),
    (".cosmos.base.abci.v1beta1.ABCIMessageLog", SERIALIZED_DEFAULT),
    (".cosmos.base.abci.v1beta1.ABCIMessageLog", UNIFFI_RECORD),
    (".cosmos.base.abci.v1beta1.ABCIMessageLog", WASM_BINDGEN_WITH_CLONE),
    (".cosmos.base.abci.v1beta1.Attribute", SERIALIZED_DEFAULT),
    (".cosmos.base.abci.v1beta1.Attribute", UNIFFI_RECORD),
    (".cosmos.base.abci.v1beta1.Attribute", WASM_BINDGEN_WITH_CLONE),
    (".cosmos.base.abci.v1beta1.GasInfo", UNIFFI_RECORD),
    (".cosmos.base.abci.v1beta1.GasInfo", WASM_BINDGEN),
    (".cosmos.base.abci.v1beta1.StringEvent", SERIALIZED_DEFAULT),
    (".cosmos.base.abci.v1beta1.StringEvent", UNIFFI_RECORD),
    (".cosmos.base.abci.v1beta1.StringEvent", WASM_BINDGEN_WITH_CLONE),
    (".cosmos.base.abci.v1beta1.TxResponse", SERIALIZED_DEFAULT),
    (".cosmos.base.tendermint.v1beta1.ProofOp", SERIALIZED_DEFAULT),
    (".cosmos.base.tendermint.v1beta1.ProofOp", WASM_BINDGEN_WITH_CLONE),
    (".cosmos.base.tendermint.v1beta1.ProofOp", UNIFFI_RECORD),
    (".cosmos.base.tendermint.v1beta1.ProofOps", SERIALIZED_DEFAULT),
    (".cosmos.base.tendermint.v1beta1.ProofOps", WASM_BINDGEN_WITH_CLONE),
    (".cosmos.base.tendermint.v1beta1.ProofOps", UNIFFI_RECORD),
    (".cosmos.base.v1beta1.Coin", SERIALIZED_DEFAULT),
    (".cosmos.base.query.v1beta1.PageResponse", SERIALIZED_DEFAULT),
    (".cosmos.base.query.v1beta1.PageResponse", UNIFFI_RECORD),
    (".cosmos.staking.v1beta1.QueryDelegationResponse", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.DelegationResponse", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.Delegation", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.QueryRedelegationsResponse", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.RedelegationResponse", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.Redelegation", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.RedelegationEntryResponse", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.RedelegationEntry", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.QueryUnbondingDelegationResponse", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.UnbondingDelegation", SERIALIZED_DEFAULT),
    (".cosmos.staking.v1beta1.UnbondingDelegationEntry", SERIALIZED_DEFAULT),
    (".cosmos.tx.v1beta1.SignDoc", SERIALIZED_DEFAULT),
    (".cosmos.tx.v1beta1.SignDoc", UNIFFI_RECORD),
    (".cosmos.tx.v1beta1.SignDoc", SERIALIZE_CAMEL_CASE),
    (".cosmos.tx.v1beta1.BroadcastMode", UNIFFI_ENUM),
    (".header.pb.ExtendedHeader", SERIALIZED_DEFAULT),
    (".proof.pb.Proof", SERIALIZED_DEFAULT),
    (".proto.blob.v2.BlobProto", SERIALIZED),
    (".shwap.AxisType", SERIALIZED),
    (".shwap.Row", SERIALIZED),
    (".shwap.RowNamespaceData", SERIALIZED_DEFAULT),
    (".shwap.Sample", SERIALIZED_DEFAULT),
    (".shwap.Share", SERIALIZED_DEFAULT),
    (".shwap.Share", TRANSPARENT),

    // Celestia's mods
    (".tendermint_celestia_mods.types.Block", SERIALIZED),
    (".tendermint_celestia_mods.types.Data", SERIALIZED),
    (".tendermint_celestia_mods.abci.TimeoutInfo", SERIALIZED),
    (".tendermint_celestia_mods.abci.ResponseInfo", SERIALIZED),
];

#[rustfmt::skip]
static CUSTOM_FIELD_ATTRIBUTES: &[(&str, &str)] = &[
    (".celestia.core.v1.da.DataAvailabilityHeader.column_roots", VEC_BASE64STRING),
    (".celestia.core.v1.da.DataAvailabilityHeader.row_roots", VEC_BASE64STRING),
    (".celestia.core.v1.proof.NMTProof.leaf_hash", BASE64STRING),
    (".celestia.core.v1.proof.NMTProof.nodes", VEC_BASE64STRING),
    // TODO: replace with quoted in a release or two
    // https://github.com/eigerco/lumina/issues/683
    (".celestia.core.v1.proof.Proof.index", MAYBE_QUOTED),
    (".celestia.core.v1.proof.Proof.total", MAYBE_QUOTED),
    (".celestia.core.v1.proof.Proof.aunts", VEC_BASE64STRING),
    (".celestia.core.v1.proof.Proof.leaf_hash", BASE64STRING),
    (".celestia.core.v1.proof.RowProof.root", BASE64STRING),
    (".celestia.core.v1.proof.RowProof.row_roots", VEC_HEXSTRING),
    (".celestia.core.v1.proof.ShareProof.data", VEC_BASE64STRING),
    (".celestia.core.v1.proof.ShareProof.namespace_id", BASE64STRING),
    (".cosmos.base.abci.v1beta1.TxResponse.logs", NULL_DEFAULT),
    (".cosmos.base.abci.v1beta1.TxResponse.events", NULL_DEFAULT),
    (".cosmos.base.query.v1beta1.PageResponse.next_key", BASE64STRING),
    (".cosmos.staking.v1beta1.RedelegationEntry.completion_time", OPTION_TIMESTAMP),
    (".cosmos.staking.v1beta1.UnbondingDelegationEntry.completion_time", OPTION_TIMESTAMP),
    (".proof.pb.Proof.leaf_hash", BASE64STRING),
    (".proof.pb.Proof.nodes", VEC_BASE64STRING),
    (".proto.blob.v2.BlobProto.data", BASE64STRING),
    (".proto.blob.v2.BlobProto.namespace_id", BASE64STRING),
    (".proto.blob.v2.BlobProto.signer", VEC_SKIP_IF_EMPTY),
    (".proto.blob.v2.BlobProto.signer", BASE64STRING),
    (".shwap.RowNamespaceData.shares", NULL_DEFAULT),
    (".shwap.Share", BASE64STRING),

    // Celestia's mods
    (".tendermint_celestia_mods.types.Data.txs", VEC_BASE64STRING),
    (".tendermint_celestia_mods.types.Data.hash", HEXSTRING),
    (".tendermint_celestia_mods.abci.TimeoutsInfo.timeout_propose", OPTION_PROTOBUF_DURATION),
    (".tendermint_celestia_mods.abci.TimeoutsInfo.timeout_commit", OPTION_PROTOBUF_DURATION),
    (".tendermint_celestia_mods.abci.ResponseInfo.data", DEFAULT),
    (".tendermint_celestia_mods.abci.ResponseInfo.version", DEFAULT),
    (".tendermint_celestia_mods.abci.ResponseInfo.app_version", QUOTED_WITH_DEFAULT),
    (".tendermint_celestia_mods.abci.ResponseInfo.last_block_height", QUOTED_WITH_DEFAULT),
    (".tendermint_celestia_mods.abci.ResponseInfo.last_block_app_hash", DEFAULT),
    (".tendermint_celestia_mods.abci.ResponseInfo.last_block_app_hash", BYTES_SKIP_IF_EMPTY),
];

#[rustfmt::skip]
static EXTERN_PATHS: &[(&str, &str)] = &[
    (".google.protobuf.Any", "::tendermint_proto::google::protobuf::Any"),
    (".google.protobuf.Duration", "::tendermint_proto::google::protobuf::Duration"),
    (".google.protobuf.Timestamp", "::tendermint_proto::google::protobuf::Timestamp"),
    // Must be kept in sync with cometbft version
    (".tendermint", "::tendermint_proto::v0_38"),
];

const PROTO_FILES: &[&str] = &[
    "vendor/celestia/blob/v1/params.proto",
    "vendor/celestia/blob/v1/query.proto",
    "vendor/celestia/blob/v1/tx.proto",
    "vendor/celestia/core/v1/da/data_availability_header.proto",
    "vendor/celestia/core/v1/gas_estimation/gas_estimator.proto",
    "vendor/celestia/core/v1/proof/proof.proto",
    "vendor/celestia/core/v1/tx/tx.proto",
    "vendor/cosmos/auth/v1beta1/auth.proto",
    "vendor/cosmos/auth/v1beta1/query.proto",
    "vendor/cosmos/bank/v1beta1/bank.proto",
    "vendor/cosmos/bank/v1beta1/genesis.proto",
    "vendor/cosmos/bank/v1beta1/query.proto",
    "vendor/cosmos/bank/v1beta1/tx.proto",
    "vendor/cosmos/base/abci/v1beta1/abci.proto",
    "vendor/cosmos/base/node/v1beta1/query.proto",
    "vendor/cosmos/base/tendermint/v1beta1/query.proto",
    "vendor/cosmos/base/v1beta1/coin.proto",
    "vendor/cosmos/crypto/ed25519/keys.proto",
    "vendor/cosmos/crypto/multisig/v1beta1/multisig.proto",
    "vendor/cosmos/crypto/secp256k1/keys.proto",
    "vendor/cosmos/staking/v1beta1/query.proto",
    "vendor/cosmos/staking/v1beta1/tx.proto",
    "vendor/cosmos/tx/v1beta1/service.proto",
    "vendor/cosmos/tx/v1beta1/tx.proto",
    "vendor/go-header/p2p/pb/header_request.proto",
    "vendor/go-square/blob/v2/blob.proto",
    "vendor/header/pb/extended_header.proto",
    "vendor/share/eds/byzantine/pb/share.proto",
    "vendor/share/shwap/p2p/bitswap/pb/bitswap.proto",
    "vendor/share/shwap/p2p/shrex/pb/shrex.proto",
    "vendor/share/shwap/p2p/shrex/shrexsub/pb/notification.proto",
    "vendor/share/shwap/pb/shwap.proto",
    "vendor/tendermint-celestia-mods/abci/types.proto",
    "vendor/tendermint-celestia-mods/blocksync/types.proto",
    "vendor/tendermint-celestia-mods/mempool/types.proto",
    "vendor/tendermint-celestia-mods/rpc/grpc/types.proto",
    "vendor/tendermint-celestia-mods/state/types.proto",
    "vendor/tendermint-celestia-mods/store/types.proto",
    "vendor/tendermint-celestia-mods/types/block.proto",
    "vendor/tendermint-celestia-mods/types/types.proto",
    "vendor/tendermint/types/types.proto",
];

const INCLUDES: &[&str] = &["vendor", "vendor/nmt"];

fn main() {
    let mut fds = protox_compile();

    sanitise_commments(&mut fds);

    #[cfg(not(feature = "tonic"))]
    prost_build(fds);
    #[cfg(feature = "tonic")]
    tonic_build(fds)
}

fn sanitise_commments(fds: &mut FileDescriptorSet) {
    // Google protobuf types contain protobuf-style markdown links,
    // which break in rustdoc, due to different path separator (. vs :)
    let sanitise = |s: &str| {
        s.replace("[HttpRule][google.api.HttpRule]", "[HttpRule]")
            .replace(
                "[HttpRule.body][google.api.HttpRule.body]",
                "[HttpRule].body",
            )
            .replace(
                "[selector][google.api.DocumentationRule.selector]",
                "DocumentationRule.selector",
            )
    };

    for fd in fds.file.iter_mut() {
        if fd.package() == "google.api" {
            let source_code_info = fd.source_code_info.as_mut().expect("proto metdata");

            for location in &mut source_code_info.location {
                let leading_comment = location.leading_comments.as_deref().map(sanitise);
                let trailing_comment = location.trailing_comments.as_deref().map(sanitise);
                let leading_detached_comment = location
                    .leading_detached_comments
                    .iter()
                    .map(|s| sanitise(s))
                    .collect();

                location.leading_comments = leading_comment;
                location.trailing_comments = trailing_comment;
                location.leading_detached_comments = leading_detached_comment;
            }
        }
    }
}

fn protox_compile() -> FileDescriptorSet {
    protox::compile(PROTO_FILES, INCLUDES).expect("protox failed to build")
}

#[cfg(not(feature = "tonic"))]
fn prost_build(fds: FileDescriptorSet) {
    let mut config = prost_build::Config::new();

    for (type_path, attr) in CUSTOM_TYPE_ATTRIBUTES {
        config.type_attribute(type_path, attr);
    }

    for (field_path, attr) in CUSTOM_FIELD_ATTRIBUTES {
        config.field_attribute(field_path, attr);
    }

    for (proto_path, rust_path) in EXTERN_PATHS {
        config.extern_path(proto_path.to_string(), rust_path.to_string());
    }

    for (proto_path, rust_path) in tendermint_mods_extern_paths(&fds) {
        config.extern_path(proto_path, rust_path);
    }

    config
        .include_file("mod.rs")
        .enable_type_names()
        .bytes([".tendermint_celestia_mods.abci"])
        .compile_fds(fds)
        .expect("prost failed");
}

#[cfg(feature = "tonic")]
fn tonic_build(fds: FileDescriptorSet) {
    let mut prost_config = tonic_build::Config::new();
    prost_config.enable_type_names();

    let mut tonic_config = tonic_build::configure()
        .include_file("mod.rs")
        .build_client(true)
        .build_server(cfg!(feature = "tonic-server"))
        .build_transport(false)
        .use_arc_self(true)
        .compile_well_known_types(true)
        .skip_protoc_run()
        .bytes([".tendermint_celestia_mods.abci"]);

    for (type_path, attr) in CUSTOM_TYPE_ATTRIBUTES {
        tonic_config = tonic_config.type_attribute(type_path, attr);
    }

    for (field_path, attr) in CUSTOM_FIELD_ATTRIBUTES {
        tonic_config = tonic_config.field_attribute(field_path, attr);
    }

    for (proto_path, rust_path) in EXTERN_PATHS {
        tonic_config = tonic_config.extern_path(proto_path, rust_path);
    }

    for (proto_path, rust_path) in tendermint_mods_extern_paths(&fds) {
        tonic_config = tonic_config.extern_path(proto_path, rust_path);
    }

    tonic_config
        .compile_fds_with_config(prost_config, fds)
        .expect("should be able to compile protobuf using tonic");
}

/// Create a list of Tentermint messages that needs to be replaced with Celestia's modifications.
fn tendermint_mods_extern_paths(fds: &FileDescriptorSet) -> Vec<(String, String)> {
    let mut extern_paths = Vec::new();

    let tendermint_types = get_proto_types_of(fds, "tendermint");
    let celestia_mods_types = get_proto_types_of(fds, "tendermint_celestia_mods");

    for mods_type in celestia_mods_types {
        let tm_type = mods_type.replace(".tendermint_celestia_mods.", ".tendermint.");

        if tendermint_types.contains(&tm_type) {
            let new_extern_path = format!("crate{}", mods_type.replace(".", "::"));
            extern_paths.push((tm_type, new_extern_path));
        }
    }

    extern_paths
}

/// Returns the names of all the Protobuf messages of a proto package.
fn get_proto_types_of(fds: &FileDescriptorSet, proto_package: &str) -> HashSet<String> {
    let proto_package_prefix = format!("{proto_package}.");
    let mut types = HashSet::new();

    for fd_proto in &fds.file {
        let Some(ref pkg_name) = fd_proto.package else {
            continue;
        };

        if pkg_name == proto_package || pkg_name.starts_with(&proto_package_prefix) {
            for msg in &fd_proto.message_type {
                if let Some(ref name) = msg.name {
                    types.insert(format!(".{pkg_name}.{name}"));
                }
            }
        }
    }

    types
}
