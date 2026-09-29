#!/bin/bash
# Healthcheck for the validator: reports healthy once a few blocks were produced
# and, when Fibre is enabled, its server accepts connections and its host is
# registered on-chain.
set -euo pipefail

celestia-appd status 2>/dev/null \
  | jq -e '(.sync_info.latest_block_height | tonumber) >= 3' >/dev/null

if [[ -n "${FIBRE_HOST:-}" ]]; then
  nc -z localhost 7980
  celestia-appd query valaddr providers --output json \
    | jq -e --arg host "$FIBRE_HOST" '.providers[] | select(.info.host == $host)' >/dev/null
fi
