#!/usr/bin/env bash
# Private Celestia services as local processes. Bash 3.2+, jq, dasel v2 and curl.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
STARTUP_TIMEOUT=180
SMOKE=false
KEEP_DATA=false
STATE=
LOGS=
ACTIVE_PID=0
SETUP_ACTIVE=false
PIDS=(0)
NAMES=(none)
TOKENS=(none none none)
RPC_PORTS=(26658 36658 46658)
P2P_PORTS=(2121 3121 4121)
REPLY=

die() { printf 'devnet: %s\n' "$*" >&2; exit 1; }
positive_integer() { [[ "$1" =~ ^[1-9][0-9]{0,5}$ ]] && ((10#$1 <= 86400)); }
alive() { kill -0 "$1" 2>/dev/null; }
signal_group() { kill -"$1" -- "-$2" 2>/dev/null || true; }

flush_setup_output() {
    [[ "$SETUP_ACTIVE" == true ]] || return 0
    cat "$STATE/stdout" "$STATE/stderr" >>"$LOGS/setup.log"
    SETUP_ACTIVE=false
}

cleanup() {
    local status=$? i deadline
    trap - EXIT INT TERM
    if [[ "$ACTIVE_PID" != 0 ]]; then signal_group TERM "$ACTIVE_PID"; fi
    for ((i=1; i<${#PIDS[@]}; i++)); do signal_group TERM "${PIDS[$i]}"; done
    deadline=$((SECONDS + 5))
    while ((SECONDS < deadline)); do
        local running=false
        if [[ "$ACTIVE_PID" != 0 ]] && alive "$ACTIVE_PID"; then running=true; fi
        for ((i=1; i<${#PIDS[@]}; i++)); do
            if alive "${PIDS[$i]}"; then running=true; fi
        done
        if [[ "$running" == false ]]; then break; fi
        sleep 0.1
    done
    # Kill groups even when their direct parent already exited, to catch leftovers.
    if [[ "$ACTIVE_PID" != 0 ]]; then
        signal_group KILL "$ACTIVE_PID"; wait "$ACTIVE_PID" 2>/dev/null || true
    fi
    for ((i=${#PIDS[@]}-1; i>0; i--)); do
        signal_group KILL "${PIDS[$i]}"; wait "${PIDS[$i]}" 2>/dev/null || true
    done
    # A service failure or signal can interrupt run before it saves its output.
    flush_setup_output || true
    if [[ -n "$STATE" ]]; then
        if [[ "$KEEP_DATA" == true ]]; then printf 'Devnet state retained: %s\n' "$STATE"
        else rm -rf "$STATE"; fi
    fi
    exit "$status"
}

init_scope() {
    # Job control gives each asynchronous command its own process group on both
    # Linux and macOS; a separate setsid executable is unnecessary.
    set -m
    STATE=$(mktemp -d "${TMPDIR:-/tmp}/lumina-devnet-XXXXXX")
    LOGS="$ROOT/target/oss-scanner/$(basename "$STATE")"
    trap cleanup EXIT
    trap 'exit 130' INT TERM
    mkdir -p "$LOGS"
    unset CELESTIA_CUSTOM CELESTIA_RPC_URL
    export GOMAXPROCS="${GOMAXPROCS:-2}"
    export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
    export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
    # Selecting the channel explicitly avoids rustup installing the root file's
    # wasm target for commands that only build native code.
    # The devnet-only image has no Rust checkout or toolchain.
    if [[ -z "${RUSTUP_TOOLCHAIN:-}" && -f "$ROOT/rust-toolchain.toml" ]]; then
        RUSTUP_TOOLCHAIN=$(awk -F '"' '/^[[:space:]]*channel[[:space:]]*=/ { print $2; exit }' "$ROOT/rust-toolchain.toml")
        [[ -n "$RUSTUP_TOOLCHAIN" ]] || die 'Missing Rust channel in rust-toolchain.toml'
        export RUSTUP_TOOLCHAIN
    fi
    printf 'Devnet logs: %s\n' "$LOGS"
}

check_services() {
    local i status
    for ((i=1; i<${#PIDS[@]}; i++)); do
        if ! alive "${PIDS[$i]}"; then
            status=0; wait "${PIDS[$i]}" || status=$?
            die "${NAMES[$i]} exited with $status; see $LOGS/${NAMES[$i]}.log"
        fi
    done
}

spawn() {
    local name=$1; shift
    check_services
    "$@" </dev/null >"$LOGS/$name.log" 2>&1 &
    PIDS+=("$!"); NAMES+=("$name")
}

run() {
    local status=0 started=$SECONDS
    check_services
    # Do not log arguments: key-import commands contain disposable private keys.
    SETUP_ACTIVE=true
    "$@" <"${RUN_INPUT:-/dev/null}" >"$STATE/stdout" 2>"$STATE/stderr" &
    ACTIVE_PID=$!
    while alive "$ACTIVE_PID"; do
        check_services
        if ((SECONDS - started >= STARTUP_TIMEOUT)); then
            signal_group KILL "$ACTIVE_PID"
            wait "$ACTIVE_PID" 2>/dev/null || true
            status=124
            break
        fi
        sleep 0.1
    done
    if [[ "$status" == 0 ]]; then wait "$ACTIVE_PID" || status=$?; fi
    signal_group KILL "$ACTIVE_PID"
    ACTIVE_PID=0
    flush_setup_output
    REPLY=$(cat "$STATE/stdout")
    if [[ "$status" != 0 ]]; then
        printf 'devnet: setup command exited with %s; see %s/setup.log\n' "$status" "$LOGS" >&2
    fi
    return "$status"
}

execute() {
    local status=0
    check_services
    "$@" &
    ACTIVE_PID=$!
    while alive "$ACTIVE_PID"; do check_services; sleep 0.1; done
    wait "$ACTIVE_PID" || status=$?
    # EXIT cleanup owns the group until every descendant has been stopped.
    return "$status"
}

app() {
    local home=$1 rpc=$2; shift 2
    local flags=()
    if [[ "$1" == query || "$1" == tx ]]; then flags=(--node "tcp://127.0.0.1:$rpc"); fi
    run celestia-appd "$@" --home "$home" ${flags[@]+"${flags[@]}"}
}

toml_set() {
    local file=$1 type=$2 key=$3 value=$4 allow_missing=${5:-false}
    if [[ "$allow_missing" != true ]]; then
        dasel -f "$file" -r toml -w json "$key" >/dev/null || die "Missing configuration key $key"
    fi
    dasel put -f "$file" -r toml -t "$type" -v "$value" "$key"
}

http_get() {
    run curl --noproxy '*' --fail --silent --show-error --max-time 2 "http://127.0.0.1:$1/$2" || return
    REPLY=$(jq -ce 'if has("result") then .result else error("Missing RPC result") end' <<<"$REPLY")
}

wait_for() {
    local description=$1 started=$SECONDS; shift
    while ((SECONDS - started < STARTUP_TIMEOUT)); do
        check_services
        if "$@" 2>>"$LOGS/readiness.log"; then return 0; fi
        sleep 0.5
    done
    die "Timed out waiting for $description; see $LOGS"
}

blocks_ready() {
    http_get "$1" status || return
    jq -e '.sync_info.latest_block_height | tonumber >= 3' <<<"$REPLY" >/dev/null
}

app_ready() { app "$1" "$2" query staking params; }

fibre_ready() {
    (exec 3<>/dev/tcp/127.0.0.1/7980) 2>/dev/null || return
    app "$1" 26657 query valaddr providers --output json || return
    jq -e 'any(.providers[]; .info.host == "localhost:7980")' <<<"$REPLY" >/dev/null
}

start_fibre() {
    local home=$1
    spawn fibre fibre start --home "$STATE/fibre" --app-grpc-address localhost:19090 \
        --signer-grpc-address 127.0.0.1:26669 --server-listen-address 127.0.0.1:7980
    app "$home" 26657 tx valaddr set-host localhost:7980 --from validator-0 \
        --fees 21000utia --keyring-backend test --chain-id private --yes --output json
    jq -e '(.code // 0) == 0' <<<"$REPLY" >/dev/null || die 'Fibre host registration failed'
    wait_for 'Fibre host registration' fibre_ready "$home"
}

create_validator() {
    local name=validator home="$STATE/validator"
    local chain=private rpc=26657 p2p=26656 grpc=19090 comet_grpc=9098
    local account key_file address coins module hash genesis
    local addresses=()
    printf 'Starting %s (%s, gRPC %s)\n' "$name" "$chain" "$grpc"
    app "$home" "$rpc" init "$chain" --chain-id "$chain"
    for account in validator-0 node-0 node-1 node-2; do
        key_file="$STATE/credentials/$account.plaintext-key"
        if [[ -f "$key_file" ]]; then
            app "$home" "$rpc" keys import-hex "$account" "$(cat "$key_file")" --keyring-backend test
        else
            app "$home" "$rpc" keys add "$account" --keyring-backend test
            printf 'y\n' >"$STATE/yes"
            RUN_INPUT="$STATE/yes" app "$home" "$rpc" keys export "$account" --unsafe --unarmored-hex --keyring-backend test
            printf '%s\n' "$REPLY" >"$key_file"
        fi
        app "$home" "$rpc" keys show "$account" -a --keyring-backend test
        address=$REPLY; addresses+=("$address")
        coins=200000000000000utia
        if [[ "$account" == validator-0 ]]; then coins=1000000000000000utia; fi
        app "$home" "$rpc" genesis add-genesis-account "$address" "$coins"
    done
    genesis="$home/config/genesis.json"
    if command -v sha256sum >/dev/null; then hash=$(printf fibre | sha256sum)
    else hash=$(printf fibre | shasum -a 256); fi
    app "$home" "$rpc" debug addr "${hash:0:40}"
    module=$(awk '/Bech32 Acc:/ {print $3}' <<<"$REPLY")
    [[ -n "$module" ]] || die 'Missing Fibre module address'
    app "$home" "$rpc" genesis add-genesis-account "$module" 4000000000000utia --module-name fibre
    jq --args '
        {denom:"utia", amount:"1000000000000"} as $balance |
        .app_state.fibre.escrow_accounts = [$ARGS.positional[] |
            {signer:., balance:$balance, available_balance:$balance}] |
        (.app_state.auth.accounts[] | select(.name? == "fibre") | .permissions) = []
    ' "${addresses[@]}" <"$genesis" >"$genesis.tmp"
    mv "$genesis.tmp" "$genesis"
    app "$home" "$rpc" genesis gentx validator-0 5000000000utia --fees 500utia --keyring-backend test --chain-id "$chain"
    app "$home" "$rpc" genesis collect-gentxs
    toml_set "$home/config/config.toml" string rpc.laddr "tcp://127.0.0.1:$rpc"
    toml_set "$home/config/config.toml" string p2p.laddr "tcp://127.0.0.1:$p2p"
    toml_set "$home/config/config.toml" string p2p.seeds ''
    toml_set "$home/config/config.toml" string p2p.persistent_peers ''
    toml_set "$home/config/config.toml" string rpc.pprof_laddr ''
    toml_set "$home/config/config.toml" string rpc.grpc_laddr "tcp://127.0.0.1:$comet_grpc"
    toml_set "$home/config/config.toml" string tx_index.indexer kv
    toml_set "$home/config/config.toml" string priv_validator_grpc_laddr 127.0.0.1:26669
    toml_set "$home/config/app.toml" bool api.enable false
    toml_set "$home/config/app.toml" bool grpc.enable true
    toml_set "$home/config/app.toml" string grpc.address "127.0.0.1:$grpc"
    spawn "$name" celestia-appd start --home "$home" --force-no-bbr \
        --delayed-precommit-timeout 500ms
    wait_for "$name blocks" blocks_ready "$rpc"
    wait_for "$name app" app_ready "$home" "$rpc"
    start_fibre "$home"
    http_get "$rpc" 'block?height=1'
    GENESIS_HASH=$(jq -er '.block_id.hash' <<<"$REPLY")
}

node_command() {
    local index=$1 kind=bridge; shift
    if [[ "$index" == 2 ]]; then kind=light; fi
    NODE_ARGS=(celestia "$kind" "$@" --node.store "$STATE/node-$index" --p2p.network private)
}

token() {
    node_command "$1" auth "$2"
    run env CELESTIA_CUSTOM="$CUSTOM_NETWORK" "${NODE_ARGS[@]}"
    REPLY=$(tail -n 1 <<<"$REPLY")
}

rpc() {
    local index=$1; shift
    run celestia "$@" --url "http://127.0.0.1:${RPC_PORTS[$index]}" --token "${TOKENS[$index]}" || return
    REPLY=$(jq -ce 'if has("result") and (.result | type) != "string" then .result else error("RPC failed") end' <<<"$REPLY")
}

node_ready() { rpc "$1" p2p info; }
headers_ready() {
    rpc "$1" header network-head || return
    jq -e '.header.height | tonumber >= 3' <<<"$REPLY" >/dev/null || return
    rpc "$1" header get-by-height 3
}

start_nodes() {
    local index home address peer_id='' level
    local flags=()
    CUSTOM_NETWORK="private:$1"
    for index in 0 1 2; do
        printf 'Starting node-%s (RPC %s, P2P %s)\n' "$index" "${RPC_PORTS[$index]}" "${P2P_PORTS[$index]}"
        node_command "$index" init
        run env CELESTIA_CUSTOM="$CUSTOM_NETWORK" "${NODE_ARGS[@]}"
        home="$STATE/node-$index"
        app "$home" 26657 keys import-hex "node-$index" "$(cat "$STATE/credentials/node-$index.plaintext-key")" \
            --keyring-backend test --keyring-dir "$home/keys"
        address="/ip4/127.0.0.1/tcp/${P2P_PORTS[$index]}"
        toml_set "$home/config.toml" json P2P.ListenAddresses "[\"$address\"]"
        toml_set "$home/config.toml" json P2P.AnnounceAddresses "[\"$address\"]"
        toml_set "$home/config.toml" json P2P.NoAnnounceAddresses '[]'
        toml_set "$home/config.toml" json P2P.IPColocationWhitelist '["127.0.0.0/8"]' true
        token "$index" admin; TOKENS[index]=$REPLY
        flags=(--rpc.skip-auth=true)
        if [[ "$index" == 1 ]]; then flags=(--rpc.skip-auth=false); fi
        if [[ "$index" == 2 ]]; then flags+=(--headers.trusted-peers "/ip4/127.0.0.1/tcp/2121/p2p/$peer_id"); fi
        node_command "$index" start --rpc.addr 127.0.0.1 --rpc.port "${RPC_PORTS[$index]}" \
            --core.ip 127.0.0.1 --core.port 19090 --keyring.keyname "node-$index" "${flags[@]}"
        spawn "node-$index" env CELESTIA_CUSTOM="$CUSTOM_NETWORK" "${NODE_ARGS[@]}"
        wait_for "node-$index RPC" node_ready "$index"
        if [[ "$index" == 0 ]]; then peer_id=$(jq -er '.id' <<<"$REPLY")
        elif [[ "$index" == 1 ]]; then rpc "$index" p2p connect "$peer_id" /ip4/127.0.0.1/tcp/2121; fi
        wait_for "node-$index headers" headers_ready "$index"
    done
    for level in read write admin; do
        token 1 "$level"
        case "$level" in
            read) export CELESTIA_NODE_AUTH_TOKEN_READ="$REPLY" ;;
            write) export CELESTIA_NODE_AUTH_TOKEN_WRITE="$REPLY" ;;
            admin) export CELESTIA_NODE_AUTH_TOKEN_ADMIN="$REPLY" ;;
        esac
    done
}

preflight() {
    local port
    local ports=(26657 26656 19090 9098 26669 7980 26658 36658 46658 2121 3121 4121)
    for port in "${ports[@]}"; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then die "Devnet port $port is already in use"; fi
    done
}

usage() {
    cat <<'EOF'
Usage: bash .oss-scanner/devnet.sh [--smoke] [--timeout SECONDS] [--keep-data]
       bash .oss-scanner/devnet.sh [options] -- COMMAND [ARGS...]

Starts a fresh private validator, Fibre, two bridges and a light node.
No command: keep services running until Ctrl-C. --smoke: exit when ready.
Commands receive bridge read/write/admin tokens and keep their exit status.
Requires Bash 3.2+, jq, dasel v2, curl and the Celestia binaries on PATH.
EOF
}

main() {
    local command_only=false dependency
    if [[ "${1:-}" == _exec ]]; then command_only=true; shift; fi
    while (($#)); do
        case "$1" in
            --smoke) SMOKE=true ;;
            --keep-data) KEEP_DATA=true ;;
            --timeout)
                [[ $# -ge 2 ]] || die '--timeout needs a value'
                STARTUP_TIMEOUT=$2; shift
                positive_integer "$STARTUP_TIMEOUT" || die '--timeout must be an integer from 1 to 86400'
                ;;
            --help|-h) usage; return ;;
            --) shift; break ;;
            *) die "Unknown option $1; use -- before a command" ;;
        esac
        shift
    done
    [[ "$SMOKE" == false || $# == 0 ]] || die '--smoke cannot be combined with a command'
    cd "$ROOT"
    init_scope
    if [[ "$command_only" == false ]]; then
        for dependency in jq dasel curl celestia-appd celestia fibre; do
            command -v "$dependency" >/dev/null || die "$dependency is missing from PATH"
        done
        [[ "$(dasel --version)" == 'dasel version v2.'* ]] || die 'Use dasel v2 (CI-compatible CLI)'
        preflight
        cp -R "$ROOT/ci/credentials" "$STATE/credentials"
        create_validator
        start_nodes "$GENESIS_HASH"
        printf 'Private native devnet ready; all services use local endpoints.\n'
    fi
    if (($#)); then execute "$@"
    elif [[ "$SMOKE" == false ]]; then
        printf 'Devnet is running. Press Ctrl-C to stop.\n'
        while :; do check_services; sleep 0.2; done
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
