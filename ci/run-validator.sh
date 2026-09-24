#!/bin/bash

set -euxo pipefail

# Amount of DA nodes to setup, taken from the first argument
# or 1 if not provided
NODE_COUNT="${NODE_COUNT:-1}"
# a private local network
P2P_NETWORK="${P2P_NETWORK:-"private"}"
# a validator node configuration directory
CONFIG_DIR="$CELESTIA_HOME/.celestia-app"
# the names of the keys
NODE_NAME="validator-0"
# amounts of the coins for the keys
NODE_COINS="200000000000000utia"
VALIDATOR_COINS="1000000000000000utia"
# Fibre escrow balance of every funded account
FIBRE_ESCROW="1000000000000"
# a directory and the files shared with the bridge nodes
CREDENTIALS_DIR="/credentials"
# directory where validator will write the genesis hash
GENESIS_DIR="/genesis"
GENESIS_HASH_FILE="$GENESIS_DIR/genesis-hash-$P2P_NETWORK"

# only let one of validators to create keys, to avoid races
if [ "$P2P_NETWORK" = "private" ]; then
  CAN_CREATE_KEYS="true"
else
  CAN_CREATE_KEYS="false"
fi

# Get the address of the node of given name
node_address() {
  local node_name="$1"
  local bech="acc"
  local node_address

  if [[ $# -ge 2 ]]; then
    bech="$2"
  fi

  node_address=$(celestia-appd keys show "$node_name" --bech "$bech" -a --keyring-backend="test")
  echo "$node_address"
}

# Waits for the given block to be created and returns it's hash
wait_for_block() {
  local block_num="$1"
  local block_hash=""

  # Wait for the block to be created
  while [[ -z "$block_hash" ]]; do
    # `|| echo` fallbacks to an empty string in case it's not ready
    # `celestia-appd` skips the block_id field so we use rest
    block_hash="$(curl -sS "http://localhost:26657/block?height=$block_num" 2>/dev/null | jq -r '.result.block_id.hash // ""' || echo)"
    sleep 0.1
  done

  echo "$block_hash"
}

# CometBFT serves block 1 before the application commits it.
wait_for_app() {
  until celestia-appd query staking params >/dev/null 2>&1; do
    sleep 0.1
  done
}

# Creates or imports key for node
create_or_import_key() {
  local node_name="$1"
  local key_file="$CREDENTIALS_DIR/$node_name.key"
  local plaintext_key_file="$CREDENTIALS_DIR/$node_name.plaintext-key"
  local acc_addr_file="$CREDENTIALS_DIR/$node_name.addr"
  local val_addr_file="$CREDENTIALS_DIR/$node_name.valaddr"

  if [ "$CAN_CREATE_KEYS" = false ]; then
    # wait for key to be created
    while [ ! -e "$key_file" ]; do
      sleep 0.1
    done
  elif [ ! -e "$key_file" ]; then
    # if key doesn't exist yet, then create and export it
    echo "Creating a new key for the $node_name"
    celestia-appd keys add "$node_name" --keyring-backend "test"
    # export it
    echo "password" | celestia-appd keys export "$node_name" --keyring-backend "test" > "$key_file.lock"
    # export also plaintext key for convenience in tests
    echo y | celestia-appd keys export "$node_name" --unsafe --unarmored-hex --keyring-backend "test" > "${plaintext_key_file}"
    # the `.lock` file and `mv` ensures that readers read file only after finished writing
    mv "$key_file.lock" "$key_file"

    # export associated account address
    node_address "$node_name" > "$acc_addr_file"

    # export validator address
    if [[ "$node_name" == validator-* ]]; then
      node_address "$node_name" "val" > "$val_addr_file"
    fi

    return 0
  fi

  # otherwise, just import it
  celestia-appd keys import-hex "$node_name" "$(cat "$plaintext_key_file")" \
    --keyring-backend "test"
}

# Announce the first block hash to the DA nodes
announce_genesis_hash() {
  local genesis_hash
  genesis_hash=$(wait_for_block 1)
  echo "$genesis_hash" > "$GENESIS_HASH_FILE"
}

# Register this validator's Fibre host on-chain
register_fibre_host() {
  celestia-appd tx valaddr set-host "$FIBRE_HOST" --from "$NODE_NAME" \
    --fees 21000utia --keyring-backend test --chain-id "$P2P_NETWORK" \
    --yes --output json | jq -e '.code == 0'
}

# Set up the validator for a private alone network.
# Based on
# https://github.com/celestiaorg/celestia-app/blob/main/scripts/single-node.sh
setup_private_validator() {
  local validator_acc_addr

  # Initialize the validator
  celestia-appd init "$P2P_NETWORK" --chain-id "$P2P_NETWORK"
  # Derive a new private key for the validator
  create_or_import_key "$NODE_NAME"
  validator_acc_addr="$(node_address "$NODE_NAME")"
  # Create a validator's genesis account for the genesis.json with an initial bag of coins
  celestia-appd genesis add-genesis-account "$validator_acc_addr" "$VALIDATOR_COINS"
  local node_idx address
  local addresses=("$validator_acc_addr")
  for ((node_idx = 0; node_idx < NODE_COUNT; node_idx++)); do
    create_or_import_key "node-$node_idx"
    address=$(node_address "node-$node_idx")
    addresses+=("$address")
    celestia-appd genesis add-genesis-account "$address" "$NODE_COINS"
  done

  if [ -n "${FIBRE_HOST:-}" ]; then
    local fibre_address genesis_file="$CONFIG_DIR/config/genesis.json"
    # module account address is sha256(module name)[:20]
    fibre_address=$(celestia-appd debug addr "$(printf fibre | sha256sum | cut -c 1-40)" | awk '/Bech32 Acc:/ {print $3}')
    # the module account backs the escrow of every funded account
    celestia-appd genesis add-genesis-account "$fibre_address" \
      "$(((NODE_COUNT + 1) * FIBRE_ESCROW))utia" --module-name fibre
    # `--module-name` grants burner+minter permissions (cosmos-sdk genutil); drop them
    jq --args --arg escrow "$FIBRE_ESCROW" '
      {denom: "utia", amount: $escrow} as $balance |
      .app_state.fibre.escrow_accounts = [
        $ARGS.positional[] | {signer: ., balance: $balance, available_balance: $balance}
      ] |
      (.app_state.auth.accounts[] | select(.name? == "fibre") | .permissions) = []
    ' "${addresses[@]}" < "$genesis_file" > "$genesis_file.tmp"
    mv "$genesis_file.tmp" "$genesis_file"
  fi

  # Generate a genesis transaction that creates a validator with a self-delegation
  celestia-appd genesis gentx "$NODE_NAME" 5000000000utia \
    --fees 500utia \
    --keyring-backend="test" \
    --chain-id "$P2P_NETWORK"
  # Collect the genesis transactions and form a genesis.json
  celestia-appd genesis collect-gentxs

  # Set proper defaults and change ports
  dasel put -f "$CONFIG_DIR/config/config.toml" -t string -v 'tcp://0.0.0.0:26657' rpc.laddr
  # enable transaction indexing
  dasel put -f "$CONFIG_DIR/config/config.toml" -t string -v 'kv' tx_index.indexer
  if [ -n "${FIBRE_HOST:-}" ]; then
    dasel put -f "$CONFIG_DIR/config/config.toml" -t string -v '127.0.0.1:26669' priv_validator_grpc_laddr
  fi

  # enable REST API
  dasel put -f "$CONFIG_DIR/config/app.toml" -t bool -v true api.enable
  dasel put -f "$CONFIG_DIR/config/app.toml" -t string -v 'tcp://0.0.0.0:1317' api.address
  # enable gRPC
  dasel put -f "$CONFIG_DIR/config/app.toml" -t bool -v true grpc.enable
  dasel put -f "$CONFIG_DIR/config/app.toml" -t string -v '0.0.0.0:9090' grpc.address

  # Optionally, configure the block size
  if [ -n "${BLOCK_SIZE:-""}" ]; then
    dasel put -f "$CONFIG_DIR/config/genesis.json" -t string -v "${BLOCK_SIZE}" consensus.params.block.max_bytes
  fi
  # Optionally, configure the square size
  if [ -n "${SQUARE_SIZE:-""}" ]; then
    dasel put -f "$CONFIG_DIR/config/genesis.json" -t string -v "${SQUARE_SIZE}" app_state.blob.params.gov_max_square_size
  fi
  # Optionally, configure the transactions ttl in mempool
  if [ -n "${MEMPOOL_TX_TTL:-""}" ]; then
    dasel put -f "$CONFIG_DIR/config/config.toml" -t int -v "${MEMPOOL_TX_TTL}" mempool.ttl-num-blocks
  fi

  # TODO: uncomment and remove grpcwebproxy, once CORS works with built-in grpc-web
  # enable grpc-web
  #dasel put -f "$CONFIG_DIR/config/app.toml" -t bool -v true grpc-web.enable
  # enable CORS as regular grpc is open
  #dasel put -f "$CONFIG_DIR/config/app.toml" -t bool -v true grpc-web.enable-unsafe-cors
  #dasel put -f "$CONFIG_DIR/config/app.toml" -t bool -v true api.enabled-unsafe-cors
  #dasel put -f "$CONFIG_DIR/config/app.toml" -t bool -v true api.enable-unsafe-cors
}

main() {
  # Configure stuff
  setup_private_validator
  # Announce genesis and register Fibre once the chain produces blocks
  (
    announce_genesis_hash
    if [ -n "${FIBRE_HOST:-}" ]; then
      wait_for_app
      register_fibre_host
    fi
  ) &
  provision_pid=$!

  # celestia-appd overrides quite a few settings if they
  # are not within a sane range for regular deployment.
  # we need to bypass that if we want e.g. crazy low ttl
  local extra_flags=()
  if [ -n "${MEMPOOL_TX_TTL:-""}" ]; then
    extra_flags+=(--bypass-config-overrides)
  fi

  # Start the celestia-app, with 500ms block time
  echo "Configuration finished. Running a validator node..."
  celestia-appd start \
    --api.enable \
    --grpc.enable \
    --force-no-bbr \
    --delayed-precommit-timeout 500ms \
    "${extra_flags[@]}" &
  service_pids=("$!")
  trap 'kill "${service_pids[@]}" "$provision_pid" 2>/dev/null || true; wait' EXIT
  trap 'exit 0' INT TERM

  if [ -n "${FIBRE_HOST:-}" ]; then
    (
      wait_for_app
      exec fibre start \
        --app-grpc-address localhost:9090 \
        --signer-grpc-address localhost:26669 \
        --server-listen-address 0.0.0.0:7980
    ) &
    service_pids+=("$!")
  fi

  wait -n "${service_pids[@]}"
  exit "$?"
}

main
