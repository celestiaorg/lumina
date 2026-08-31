#!/bin/bash
# Healthcheck for a DA node: reports healthy only once the node's RPC is up
# and it has synced at least the given number of headers (default: 3).
set -euo pipefail

target_height="${1:-3}"
# admin token written by the entrypoint before the node starts.
# nodes with skip-auth accept it as well.
token="$(tail -n 1 "/credentials/node-${NODE_ID:-0}.jwt")"
url="http://localhost:26658"

height="$(celestia header network-head --token "$token" --url "$url" 2>/dev/null \
  | jq -r '.result.header.height // empty')"

if [[ -z "$height" ]]; then
  echo "rpc not ready" >&2
  exit 1
fi

if (( height < target_height )); then
  echo "network head is $height, waiting for $target_height" >&2
  exit 1
fi

# the header at the target height must be retrievable, not just announced
celestia header get-by-height "$target_height" --token "$token" --url "$url" >/dev/null
