#!/usr/bin/env bash

set -xeuo pipefail

# Tags
CELESTIA_APP="7.0.2-mocha"
COMETBFT="0.38.17"
COSMOS_PULSAR="1.0.0-beta.5"
COSMOS_PROTOBUF="1.7.0"
COSMOS_SDK="1.29.4-sdk-v0.50.14"
# Branches
CELESTIA_NODE="main"
NMT="main"
GO_HEADER="main"
GO_SQUARE="main"
GOOGLEAPIS="master"

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
cd ../proto

extract_urls() {
    local dir="$1"
    shift
    local urls=("$@")

    pushd "$dir" > /dev/null 2>&1

    for url in "${urls[@]}"; do
        curl -L "$url" | tar zxf -
    done

    popd > /dev/null 2>&1
}

rm -rf ../target/proto-vendor-src
mkdir -p ../target/proto-vendor-src

# when switching from tag to branch or vice versa, make sure to update the link below appropriately, note the `v' prefix for tag
extract_urls ../target/proto-vendor-src \
    "https://github.com/cometbft/cometbft/archive/refs/tags/v${COMETBFT}.tar.gz" \
    "https://github.com/cosmos/cosmos-proto/archive/refs/tags/v${COSMOS_PULSAR}.tar.gz" \
    "https://github.com/cosmos/gogoproto/archive/refs/tags/v${COSMOS_PROTOBUF}.tar.gz" \
    "https://github.com/celestiaorg/celestia-app/archive/refs/tags/v${CELESTIA_APP}.tar.gz" \
    "https://github.com/celestiaorg/cosmos-sdk/archive/refs/tags/v${COSMOS_SDK}.tar.gz" \
    "https://github.com/celestiaorg/celestia-node/archive/refs/heads/${CELESTIA_NODE}.tar.gz" \
    "https://github.com/celestiaorg/nmt/archive/refs/heads/${NMT}.tar.gz" \
    "https://github.com/celestiaorg/go-header/archive/refs/heads/${GO_HEADER}.tar.gz" \
    "https://github.com/celestiaorg/go-square/archive/refs/heads/${GO_SQUARE}.tar.gz" \
    "https://github.com/googleapis/googleapis/archive/refs/heads/${GOOGLEAPIS}.tar.gz" \

mkdir -p vendor

rm -rf vendor/celestia
cp -r "../target/proto-vendor-src/celestia-app-${CELESTIA_APP//\//-}/proto/celestia" vendor

rm -rf vendor/go-header
mkdir -p vendor/go-header/p2p
cp -r "../target/proto-vendor-src/go-header-${GO_HEADER//\//-}/p2p/pb" vendor/go-header/p2p

rm -rf vendor/go-square
cp -r "../target/proto-vendor-src/go-square-${GO_SQUARE//\/-}/proto" vendor/go-square

rm -rf vendor/amino
cp -r "../target/proto-vendor-src/cosmos-sdk-${COSMOS_SDK//\//-}/proto/amino" vendor/amino

rm -rf vendor/cosmos
mkdir -p vendor/cosmos
cp -r "../target/proto-vendor-src/cosmos-sdk-${COSMOS_SDK//\//-}/proto/cosmos/"{auth,bank,base,crypto,msg,query,staking,tx} vendor/cosmos

rm -rf vendor/cosmos_proto
cp -r "../target/proto-vendor-src/cosmos-proto-${COSMOS_PULSAR//\//-}/proto/cosmos_proto" vendor

rm -rf vendor/gogoproto
mkdir -p vendor/gogoproto
cp "../target/proto-vendor-src/gogoproto-${COSMOS_PROTOBUF//\//-}/gogoproto/gogo.proto" vendor/gogoproto

rm -rf vendor/google
mkdir -p vendor/google/api
cp "../target/proto-vendor-src/googleapis-${GOOGLEAPIS//\//-}/google/api/"{annotations.proto,http.proto} vendor/google/api

rm -rf vendor/header
mkdir -p vendor/header
cp -r "../target/proto-vendor-src/celestia-node-${CELESTIA_NODE//\//-}/header/pb" vendor/header

rm -rf vendor/share
mkdir -p vendor/share
shwap_dir=../target/proto-vendor-src/celestia-node-${CELESTIA_NODE//\//-}/share
find "$shwap_dir" -name pb -type d -print0 | while read -r -d '' pb_dir; do
  # remove prefix
  out_dir="${pb_dir#"$shwap_dir"}"
  # remove /pb suffix
  out_dir="vendor/share/${out_dir%/*}"
  mkdir -p "$out_dir"
  cp -r "$pb_dir" "$out_dir"
done

# NOTE: celestia-node doesn't set the namespace for shrex proto,
# only specifying he `go_package`. To not have it exported directly
# from the root of `celestia_proto` crate, we append a namespace to it.
# TODO: ideally fix in celestia-node
sed -i'.bak' '2 a package share.p2p.shrex;' vendor/share/shwap/p2p/shrex/pb/shrex.proto
rm vendor/share/shwap/p2p/shrex/pb/shrex.proto.bak

rm -rf vendor/tendermint
cp -r "../target/proto-vendor-src/cometbft-${COMETBFT//\//-}/proto/tendermint" vendor

rm -rf vendor/nmt
mkdir -p vendor/nmt
cp -r ../target/proto-vendor-src/nmt-${NMT//\//-}/pb vendor/nmt

find vendor -name '*.go' -delete
