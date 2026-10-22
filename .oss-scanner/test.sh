#!/usr/bin/env bash
# Lumina native tests and devnet launcher lifecycle checks.
#
# Commands (run from the repository root):
#   bash .oss-scanner/test.sh                     All selected native suites.
#   bash .oss-scanner/test.sh test celestia-fibre  One package, with a fresh devnet.
#   bash .oss-scanner/test.sh test celestia-rpc celestia-grpc  Multiple packages.
#   bash .oss-scanner/test.sh test lumina-node    Node networking and synchronization.
#   bash .oss-scanner/test.sh unit                Independent suites; no devnet.
#   bash .oss-scanner/test.sh build               Compile CLI/tests; do not run them.
#   bash .oss-scanner/test.sh smoke               Start devnet, check readiness, stop.
#   bash .oss-scanner/test.sh run COMMAND...      Run a command with a fresh devnet.
#   bash .oss-scanner/test.sh check               Test the launcher itself.
#
# Normal flow: select packages -> devnet.sh starts services -> run Cargo -> cleanup.
# Cargo runs offline; dependencies must already be cached in the local checkout.
# The optional launcher checks below are skipped during normal Rust test runs.

set -euo pipefail
cd "$(dirname "$0")/.."

# =============================================================================
# 1. Launcher self-checks (`check` mode only; no Celestia binaries or Rust needed)
# =============================================================================

# Child-shell expressions remain quoted until their child evaluates them.
# shellcheck disable=SC2016
if [[ "${1:-}" == check || "${1:-}" == _case ]]; then

    # --- Isolated scenarios (internal `_case` subprocesses) ---
    # Each scenario uses dummy processes to exercise devnet.sh's supervision.
    if [[ "${1:-}" == _case ]]; then
        # shellcheck source=.oss-scanner/devnet.sh
        source .oss-scanner/devnet.sh
        name=$2; markers=$3
        init_scope
        printf '%s\n' "$STATE" >"$markers/state"
        printf '%s\n' "$LOGS" >"$markers/logs"

        case "$name" in
            status)
                spawn sleeper sleep 60
                printf '%s\n' "${PIDS[1]}" >"$markers/service"
                execute sh -c 'exit 7'
                ;;

            service-failure)
                spawn broken sh -c 'while [ ! -f "$1" ]; do sleep 0.01; done; exit 9' sh "$markers/command"
                execute sh -c 'echo $$ > "$1"; exec sleep 60' sh "$markers/command"
                ;;

            timeout)
                STARTUP_TIMEOUT=1
                status=0
                run sh -c 'echo $$ > "$1"; echo timeout-stdout; echo timeout-stderr >&2; exec sleep 60' sh "$markers/command" || status=$?
                [[ "$status" == 124 && "$REPLY" == timeout-stdout ]]
                for message in timeout-stdout timeout-stderr; do
                    [[ "$(cat "$LOGS/setup.log")" == *"$message"* ]]
                done
                exit "$status"
                ;;

            setup-failure)
                status=0
                run sh -c 'echo setup-stderr >&2; exit 9' || status=$?
                [[ "$status" == 9 && "$(cat "$LOGS/setup.log")" == *setup-stderr* ]]
                exit "$status"
                ;;

            setup-service-failure)
                spawn broken sh -c 'while [ ! -f "$1" ]; do sleep 0.01; done; exit 9' sh "$markers/command"
                run sh -c 'echo interrupted-stdout; echo interrupted-stderr >&2; echo $$ > "$1"; exec sleep 60' sh "$markers/command"
                ;;

            signal)
                execute sh -c 'echo $$ > "$1"; exec sleep 60' sh "$markers/command"
                ;;

            stubborn)
                spawn stubborn sh -c 'trap "" TERM; echo $$ > "$1"; exec sleep 60' sh "$markers/service"
                while [[ ! -f "$markers/service" ]]; do sleep 0.01; done
                execute sh -c 'exit 7'
                ;;

            descendants)
                execute sh -c 'sleep 60 & echo $! > "$1"; exit 7' sh "$markers/command"
                ;;

            environment)
                execute sh -c 'test -z "${CELESTIA_CUSTOM+x}" && test -z "${CELESTIA_RPC_URL+x}" && test "$CELESTIA_NODE_AUTH_TOKEN_ADMIN" = test-token'
                ;;
        esac
        exit
    fi

    # --- Temporary fixtures and cleanup assertions ---
    CHECKS=$(mktemp -d "${TMPDIR:-/tmp}/lumina-shell-check-XXXXXX")
    ACTIVE_TEST=0
    # Called by the EXIT trap, including when a check fails.
    # shellcheck disable=SC2329
    finish() {
        if [[ "$ACTIVE_TEST" != 0 ]]; then kill -TERM "$ACTIVE_TEST" 2>/dev/null || true; wait "$ACTIVE_TEST" 2>/dev/null || true; fi
        rm -rf "$CHECKS"
    }
    trap finish EXIT

    assert_clean() {
        local markers=$1 file pid deadline
        [[ ! -e "$(cat "$markers/state")" ]] || { echo 'FAIL: temporary state remains' >&2; exit 1; }
        for file in service command; do
            if [[ -f "$markers/$file" ]]; then
                pid=$(cat "$markers/$file")
                deadline=$((SECONDS + 3))
                while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do sleep 0.05; done
                if kill -0 "$pid" 2>/dev/null; then echo "FAIL: child $pid remains" >&2; exit 1; fi
            fi
        done
    }

    expect() {
        local name=$1 expected=$2 status=0
        mkdir "$CHECKS/$name"
        bash "$0" _case "$name" "$CHECKS/$name" >"$CHECKS/$name.log" 2>&1 || status=$?
        if [[ "$status" != "$expected" ]]; then
            cat "$CHECKS/$name.log"; echo "FAIL: $name returned $status, expected $expected" >&2; exit 1
        fi
        assert_clean "$CHECKS/$name"
        if [[ "$name" == setup-service-failure ]]; then
            for message in interrupted-stdout interrupted-stderr; do
                [[ "$(cat "$(cat "$CHECKS/$name/logs")/setup.log")" == *"$message"* ]] || {
                    echo 'FAIL: interrupted setup diagnostics were lost' >&2; exit 1;
                }
            done
        fi
        printf 'PASS %s\n' "$name"
    }

    # --- Exit codes, failures, timeouts, descendants and environment isolation ---
    expect status 7
    expect service-failure 1
    expect timeout 124
    expect setup-failure 9
    expect setup-service-failure 1
    expect stubborn 7
    expect descendants 7
    CELESTIA_CUSTOM=mainnet CELESTIA_RPC_URL=https://example.invalid \
        CELESTIA_NODE_AUTH_TOKEN_ADMIN=test-token expect environment 0

    # --- Signal handling: both SIGTERM and Ctrl-C must clean up ---
    for signal in TERM INT; do
        markers="$CHECKS/signal-$signal"; mkdir "$markers"
        # Job control prevents Bash from making SIGINT ignored in asynchronous jobs.
        set -m
        bash "$0" _case signal "$markers" >"$CHECKS/signal-$signal.log" 2>&1 &
        ACTIVE_TEST=$!
        set +m
        deadline=$((SECONDS + 5))
        while [[ ! -f "$markers/command" ]]; do
            if ! kill -0 "$ACTIVE_TEST" 2>/dev/null || ((SECONDS >= deadline)); then
                cat "$CHECKS/signal-$signal.log"; echo 'FAIL: command did not start' >&2; exit 1
            fi
            sleep 0.05
        done
        kill -"$signal" "$ACTIVE_TEST"
        status=0; wait "$ACTIVE_TEST" || status=$?; ACTIVE_TEST=0
        [[ "$status" == 130 ]] || { echo "FAIL: $signal returned $status" >&2; exit 1; }
        assert_clean "$markers"
        printf 'PASS SIG%s cleanup\n' "$signal"
    done

    # --- Reject malformed startup timeout arguments ---
    for value in 0 -1 01 1.5 invalid 9999999999999999999999; do
        status=0
        bash .oss-scanner/devnet.sh --timeout "$value" >"$CHECKS/argument.log" 2>&1 || status=$?
        [[ "$status" == 1 && "$(cat "$CHECKS/argument.log")" == *'--timeout must be an integer'* ]] || {
            echo "FAIL: invalid timeout $value was not rejected" >&2; exit 1;
        }
    done
    printf 'PASS invalid startup timeouts\n'

    # --- Package timeout: kill Cargo and its descendants ---
    # Exercise the real package timeout with a Cargo stand-in that leaves a child.
    mkdir "$CHECKS/bin"
    cat >"$CHECKS/bin/cargo" <<'EOF'
#!/usr/bin/env bash
sleep 60 &
echo "$!" >"$LUMINA_TIMEOUT_MARKER"
wait
EOF
    chmod +x "$CHECKS/bin/cargo"
    status=0
    PATH="$CHECKS/bin:$PATH" LUMINA_TEST_TIMEOUT=3 LUMINA_TIMEOUT_MARKER="$CHECKS/cargo-child" \
        bash .oss-scanner/test.sh unit >"$CHECKS/cargo-timeout.log" 2>&1 || status=$?
    [[ "$status" == 124 ]] || { cat "$CHECKS/cargo-timeout.log"; echo 'FAIL: package timeout' >&2; exit 1; }
    [[ "$(cat "$CHECKS/cargo-timeout.log")" == *'Native tests for celestia-proto timed out after 3s'* ]] || {
        echo 'FAIL: package timeout omitted its package and deadline' >&2; exit 1;
    }
    pid=$(cat "$CHECKS/cargo-child")
    deadline=$((SECONDS + 3))
    while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do sleep 0.05; done
    if kill -0 "$pid" 2>/dev/null; then echo 'FAIL: Cargo descendant remains' >&2; exit 1; fi
    printf 'PASS package timeout cleanup\n'

    # --- Rust toolchain selection: repository default and caller override ---
    # The native path selects the repository channel without invoking rustup's wasm
    # target installation, while an explicit caller override remains authoritative.
    cat >"$CHECKS/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$RUSTUP_TOOLCHAIN" >>"$LUMINA_TOOLCHAIN_MARKER"
EOF
    for toolchain in repository caller-override; do
        marker="$CHECKS/toolchain-$toolchain"
        if [[ "$toolchain" == repository ]]; then
            (
                unset RUSTUP_TOOLCHAIN
                PATH="$CHECKS/bin:$PATH" LUMINA_TOOLCHAIN_MARKER="$marker" bash .oss-scanner/test.sh unit
            ) >"$CHECKS/toolchain-$toolchain.log" 2>&1
            actual=$(sed -n '1p' "$marker")
            [[ "$(cat rust-toolchain.toml)" == *"channel = \"$actual\""* && -n "$actual" ]] || {
                echo 'FAIL: native commands did not select the repository Rust channel' >&2; exit 1;
            }
        else
            PATH="$CHECKS/bin:$PATH" RUSTUP_TOOLCHAIN=caller-override LUMINA_TOOLCHAIN_MARKER="$marker" \
                bash .oss-scanner/test.sh unit >"$CHECKS/toolchain-$toolchain.log" 2>&1
            [[ "$(sed -n '1p' "$marker")" == caller-override ]] || {
                echo 'FAIL: native commands ignored the caller Rust channel' >&2; exit 1;
            }
        fi
    done
    printf 'PASS native Rust channel selection and override\n'

    # The check-only path ends here; it never enters the native test runner.
    printf 'PASS all Bash devnet lifecycle checks\n'
    exit
fi

# =============================================================================
# 2. Native package selection and timeout settings
# =============================================================================

# These suites run without a Celestia network (`unit` mode).
core=(
    celestia-proto
    celestia-types
    celestia-grpc-macros
    lumina-utils
    rsema1d
    lumina-cli
)

# These suites need the private Celestia services.
integration=(
    lumina-node
    celestia-rpc
    celestia-grpc
    celestia-client
    celestia-fibre
)

all=("${core[@]}" "${integration[@]}")

# Bound each package's compilation/test run (default: 30 minutes).
test_timeout=${LUMINA_TEST_TIMEOUT:-1800}
if ! [[ "$test_timeout" =~ ^[1-9][0-9]{0,5}$ ]] || ! ((10#$test_timeout <= 86400)); then
    echo 'LUMINA_TEST_TIMEOUT must be an integer from 1 to 86400' >&2; exit 2
fi

# =============================================================================
# 3. Compile or test one package
# =============================================================================

test_package() {
    local package=$1 mode=$2 pid started=$SECONDS status=0
    local flags=(--locked --offline -p "$package")

    # Match each package's native test features.
    case "$package" in
        lumina-utils) flags+=(--features 'executor,time,failover,test-utils') ;;
        celestia-client|celestia-grpc) flags+=(--features tls-ring) ;;
    esac

    # Build-only mode produces test binaries without executing them.
    if [[ "$mode" == build ]]; then cargo test "${flags[@]}" --no-run; return; fi

    # The simplified devnet has one chain; omit the specialized eviction case.
    local cargo_command=(cargo test "${flags[@]}")
    if [[ "$package" == celestia-grpc ]]; then
        echo 'Skipping client::tests::retransmit_evicted: requires a separate eviction-test chain.'
        cargo_command+=(-- --skip client::tests::retransmit_evicted)
    fi

    # Supervise Cargo until it finishes or reaches its deadline.
    # The outer devnet scope owns this worker's entire process group. On timeout,
    # returning 124 makes that scope stop Cargo and any test-spawned descendants.
    "${cargo_command[@]}" &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        if ((SECONDS - started >= test_timeout)); then
            printf 'Native tests for %s timed out after %ss\n' "$package" "$test_timeout" >&2
            kill -TERM "$pid" 2>/dev/null || true
            return 124
        fi
        sleep 0.2
    done
    wait "$pid" || status=$?
    return "$status"
}

# =============================================================================
# 4. Command dispatch: network setup stays in devnet.sh
# =============================================================================

action=${1:-test}
[[ $# == 0 ]] || shift
case "$action" in
    # `_exec` borrows devnet.sh's cleanup scope without starting any services.
    build|unit)
        exec bash .oss-scanner/devnet.sh _exec -- bash .oss-scanner/test.sh "_$action"
        ;;

    # Internal workers, entered inside the supervised cleanup scope above.
    _build)
        cargo build --locked --offline -p lumina-cli
        for package in "${all[@]}"; do test_package "$package" build; done
        ;;

    _unit)
        for package in "${core[@]}"; do test_package "$package" test; done
        ;;

    # Validate package names, then start one fresh devnet for the whole run.
    test)
        packages=("$@")
        [[ ${#packages[@]} != 0 ]] || packages=("${all[@]}")
        for package in "${packages[@]}"; do
            case " ${all[*]} " in
                *" $package "*) ;;
                *) echo "Unknown native package: $package" >&2; exit 2 ;;
            esac
        done
        exec bash .oss-scanner/devnet.sh -- bash .oss-scanner/test.sh _test "${packages[@]}"
        ;;

    # Internal worker: services are ready and authentication tokens are inherited.
    _test)
        for package in "$@"; do test_package "$package" test; done
        ;;

    # Check only service startup/readiness; no Cargo tests.
    smoke) exec bash .oss-scanner/devnet.sh --smoke ;;

    # Run a targeted reproducer or another command with the same fresh devnet.
    run)
        (($#)) || { echo 'run requires a command' >&2; exit 2; }
        exec bash .oss-scanner/devnet.sh -- "$@"
        ;;

    # Show public commands; underscore-prefixed workers are implementation details.
    *)
        echo "Usage: bash .oss-scanner/test.sh {check|build|unit|smoke|test [PACKAGES...]|run COMMAND...}"
        [[ "$action" == help ]] || exit 2
        ;;
esac
