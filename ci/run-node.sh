#!/bin/bash

set -euxo pipefail

NODE_ID="${NODE_ID:-0}"
NODE_TYPE="${NODE_TYPE:-bridge}"
SKIP_AUTH="${SKIP_AUTH:-false}"
NODE_NAME="node-$NODE_ID"
# a private local network
P2P_NETWORK="${P2P_NETWORK:-"private"}"
# a node configuration directory
CONFIG_DIR="$HOME/.celestia-$NODE_TYPE-$P2P_NETWORK"
# directory and the files shared with the validator node
CREDENTIALS_DIR="/credentials"
# node credentials
NODE_KEY_FILE="$CREDENTIALS_DIR/$NODE_NAME.key"
NODE_JWT_FILE="$CREDENTIALS_DIR/$NODE_NAME.jwt"
# directory where validator will write the genesis hash
GENESIS_DIR="/genesis"
GENESIS_HASH_FILE="$GENESIS_DIR/genesis-hash-$P2P_NETWORK"

# Wait for the validator to set up and provision us via shared dir
wait_for_provision() {
  echo "Waiting for the validator node to start"
  while [[ ! ( -e "$GENESIS_HASH_FILE" && -e "$NODE_KEY_FILE" ) ]]; do
    sleep 0.1
  done

  echo "Validator is ready"
}

# Import the test account key shared by the validator
import_shared_key() {
  echo "password" | cel-key import "$NODE_NAME" "$NODE_KEY_FILE" \
    --keyring-backend="test" \
    --p2p.network "$P2P_NETWORK" \
    --node.type "$NODE_TYPE"
}

set_custom_network() {
  local genesis_hash

  # Read the hash of the genesis block
  genesis_hash="$(cat "$GENESIS_HASH_FILE")"
  # celestia-node requires setting custom network params through env
  export CELESTIA_CUSTOM="$P2P_NETWORK:$genesis_hash"
}

whitelist_localhost_nodes() {
  # to get the list of ips:
  # cargo run -- node -n private -l 0.0.0.0
  # docker compose -f ci/docker-compose.yml exec node-0 celestia p2p peer-info $lumina_peerid
  dasel put -f "$CONFIG_DIR/config.toml" \
    -t json -v '["172.16.0.0/12", "192.168.0.0/16"]' \
    'P2P.IPColocationWhitelist'
}

write_jwt_token() {
  echo "Saving jwt token to $NODE_JWT_FILE"
  # `CELESTIA_CUSTOM` makes the cli print a warning to stdout, keep only the token
  celestia "$NODE_TYPE" auth admin --p2p.network "$P2P_NETWORK" | tail -n 1 > "$NODE_JWT_FILE"
}

# Retries the given command until it succeeds or the attempts run out
retry() {
  local attempts="$1"
  shift

  for (( i = 1; i <= attempts; i++ )); do
    if "$@"; then
      return 0
    fi
    echo "Attempt $i/$attempts of '$*' failed, retrying" >&2
    sleep 1
  done

  echo "'$*' did not succeed after $attempts attempts" >&2
  return 1
}

common_node_addr() {
  local peer_id

  # get PeerId of the common node. `depends_on` in docker-compose ensures
  # node-0 is healthy before we start, but keep retrying just in case
  peer_id="$(retry 30 celestia p2p info --token skip-auth --url 'ws://node-0:26658' | jq -r '.result.id')"
  if [[ -z "$peer_id" || "$peer_id" == null ]]; then
    echo "Could not get the peer id of node-0" >&2
    return 1
  fi

  echo "$peer_id"
}

main() {
  # Initialize data availability node
  celestia "$NODE_TYPE" init --p2p.network "$P2P_NETWORK"
  # don't allow banning nodes we create in tests by pubsub ip counting
  whitelist_localhost_nodes
  # Wait for a validator
  wait_for_provision
  # Import the key with the coins
  import_shared_key
  # Trust the private blockchain
  set_custom_network
  # Update the JWT token
  write_jwt_token
  sleep 5

  local extra_flags=()

  # all nodes but first should be connected to first one
  if [ "$NODE_ID" != 0 ]; then
    local peer_id
    peer_id="$(common_node_addr)"

    # bridge node syncs from validator, so it connects manually
    # other node types just set trusted peers to sync from
    if [ "$NODE_TYPE" == bridge ]; then
      # this should run in background after node is started
      (
        # wait for our own node to start its rpc, then connect
        sleep 4
        echo "Connecting to common $peer_id node"
        retry 30 celestia p2p connect "$peer_id" /dns/node-0/tcp/2121
      )&
    else
      extra_flags+=( --headers.trusted-peers "/dns/node-0/tcp/2121/p2p/$peer_id" )
    fi
  fi

  # Start the node
  echo "Configuration finished. Running a $NODE_TYPE node..."
  celestia "$NODE_TYPE" start \
    --rpc.skip-auth="$SKIP_AUTH" \
    --rpc.cors-enabled=true \
    --rpc.cors-allowed-origins "*" \
    --rpc.addr 0.0.0.0 \
    --core.ip validator \
    --keyring.keyname "$NODE_NAME" \
    --p2p.network "$P2P_NETWORK" \
    "${extra_flags[@]}"
}

main
