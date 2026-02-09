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

# Saves the hash of the genesis node and the keys funded with the coins
# to the directory shared with the da node
provision_da_nodes() {
  local genesis_hash
  local last_node_idx=$((NODE_COUNT - 1))
  local addresses=()

  # Save the genesis hash for the DA node
  genesis_hash=$(wait_for_block 1)
  echo "Saving a genesis hash to $GENESIS_HASH_FILE"
  echo "$genesis_hash" > "$GENESIS_HASH_FILE"

  # Import or create the keys for DA nodes and collect their addresses
  for node_idx in $(seq 0 "$last_node_idx"); do
    create_or_import_key "node-$node_idx"
    addresses+=("$(node_address "node-$node_idx")")
  done

  # Transfer the coins to DA nodes addresses
  # Coins transfer need to be after validator registers EVM address, which happens in block 2.
  # see `setup_private_validator`
  wait_for_block 3

  echo "Transfering $NODE_COINS coins to DA nodes"
  echo "y" | celestia-appd tx bank multi-send \
    "$NODE_NAME" \
    "${addresses[@]}" \
    "$NODE_COINS" \
    --fees 21000utia \
    --keyring-backend "test" \
    --chain-id "$P2P_NETWORK"

  echo "Provisioning finished."
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
  # Spawn a job to provision a bridge node later
  provision_da_nodes &

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
    "${extra_flags[@]}"
}

main
